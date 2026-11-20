from collections import Counter
from datetime import datetime, timezone
import logging
import sys

from documentcloud.addon import AddOn
from documentcloud.exceptions import APIError

from utils import (
    BULK_LIMIT,
    REQUIRED_FIELDS,
    build_installation_data,
    first_value,
    load_georisques_data,
    put_batch,
    put_item,
    updated_data,
)

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logging.getLogger("squarelet").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# Log progress every this many read documents.
PROGRESS_EVERY = 10000

# The API's maximum; reading the whole project is what bounds a run
PER_PAGE = 100

# Abort the whole run once this many documents have failed to update
MAX_GIVE_UPS = 5


class GeorisquesMetadataUpdater(AddOn):
    """Reconciles every document's installation metadata with the Géorisques CSV."""

    def check_time_limit(self):
        if self.time_limit != 0:
            limit_in_seconds = self.time_limit * 60
            elapsed = (datetime.now(timezone.utc) - self.start_time).total_seconds()

            if elapsed > limit_in_seconds:
                logger.info(f"Closing due to time limit ({self.time_limit} minutes).")
                self.flush()
                sys.exit(0)

    def doc_label(self, document):
        return f'Document {document.id} "{document.title}"'

    def log_changes(self, document, changes):
        logger.info(f"[dry-run] {self.doc_label(document)}: {len(changes)} field(s) to update:")
        for key, (old, new) in changes.items():
            if new is None:
                logger.info(f"[dry-run]     {key}: {old!r} -> deleted")
            else:
                logger.info(f"[dry-run]     {key}: {old!r} -> {new!r}")

    def update_metadata(self, document):
        """Queue one document's write when its installation_* keys differ from the CSV.

        A document with no AIOT code, or one the CSV no longer lists, is left untouched.
        """
        data = document.data or {}
        code_aiot = first_value(data.get("installation_aiot_code"))
        if not code_aiot:
            self.counts["no installation_aiot_code"] += 1
            return
        target = build_installation_data(code_aiot, self.df_installations, self.rubriques_by_aiot)
        if not target:
            self.counts["installation_aiot_code not in the CSV"] += 1
            return

        new_data, changes = updated_data(data, target)
        if new_data is None:
            self.counts["unchanged"] += 1
            return

        missing = [field for field in REQUIRED_FIELDS if getattr(document, field, None) is None]
        if missing:
            self.counts["skipped"] += 1
            logger.warning(f"{self.doc_label(document)}: skipped, no {', '.join(missing)} to send back.")
            return

        self.counts["changed"] += 1
        for key, (_old, new) in changes.items():
            self.key_counts[f"{key} {'deleted' if new is None else 'set'}"] += 1
        if self.dry_run:
            self.log_changes(document, changes)
            return

        self.batch.append(put_item(document, new_data))
        if len(self.batch) >= BULK_LIMIT:
            self.flush()

    def flush(self):
        batch, self.batch = self.batch, []
        if not batch:
            return
        try:
            put_batch(self.client, batch)
        except APIError as exc:
            if exc.status_code != 400 or len(batch) == 1:
                self.give_up(batch)
                return
            # A bulk update is all-or-nothing: retry alone so only the bad document fails
            for item in batch:
                self.batch = [item]
                self.flush()
        except Exception:  # noqa: BLE001 - keep the run going
            self.give_up(batch)
        else:
            self.saved_count += len(batch)

    def give_up(self, batch):
        logger.exception(f"Failed to update documents {[item['id'] for item in batch]}")
        self.failure_count += len(batch)
        if self.failure_count >= MAX_GIVE_UPS:
            logger.error(
                f"Aborting run: {self.failure_count} documents failed to update "
                f"(max_give_ups={MAX_GIVE_UPS}). DocumentCloud may be rate-limiting "
                "or unavailable; the next run compares every document again."
            )
            sys.exit(1)

    def main(self):
        """The main add-on functionality goes here."""

        self.start_time = datetime.now(timezone.utc)

        # Add a custom user agent here to positively identify the Add-On
        self.client.session.headers.update(
            {"User-Agent": "Disclose Georisques Metadata Updater Add-On"}
        )

        # Variables from config.yaml
        self.project = self.data.get("project", None)
        self.time_limit = self.data["time_limit"]
        self.dry_run = self.data.get("dry_run", False)
        self.max_documents = self.data.get("max_documents", 0)

        self.read_count = 0
        self.saved_count = 0
        self.failure_count = 0
        self.counts = Counter()
        self.key_counts = Counter()
        self.batch = []

        if self.dry_run:
            logger.info("Running in DRY-RUN mode: no document will be modified.")

        # Fetch & load Géorisques data (installations table + rubriques index)
        self.df_installations, self.rubriques_by_aiot = load_georisques_data()
        logger.info(
            f"Loaded {len(self.df_installations)} installations "
            f"({len(self.rubriques_by_aiot)} with rubriques)."
        )

        try:
            # Sorted by id so the cursor is stable while the run writes
            documents = self.client.documents.search(
                f"project:{self.project}", sort="id", per_page=PER_PAGE
            )
            logger.info(f"Comparing the project's {documents.count} documents with the CSV...")
            for document in documents:
                if self.max_documents and self.read_count >= self.max_documents:
                    logger.info(f"Reached max_documents limit ({self.max_documents}); stopping.")
                    break
                self.check_time_limit()
                self.read_count += 1
                self.update_metadata(document)
                if self.read_count % PROGRESS_EVERY == 0:
                    logger.info(
                        f"Read {self.read_count} documents so far, "
                        f"{self.counts['changed']} changed..."
                    )
            self.flush()
        finally:
            # Always report the totals, whatever ended the run: natural completion,
            # the max_documents cap, the time limit (sys.exit), or the give-up abort.
            written = "would be written" if self.dry_run else f"{self.saved_count} written"
            logger.info(
                f"Run finished. Read {self.read_count} documents: {dict(self.counts) or 'none'}; "
                f"changed documents {written}, {self.failure_count} failed."
            )
            if self.key_counts:
                logger.info(f"Changes by key: {dict(sorted(self.key_counts.items()))}")


if __name__ == "__main__":
    GeorisquesMetadataUpdater().main()
