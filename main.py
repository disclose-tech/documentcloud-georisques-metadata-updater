from datetime import datetime, timezone
import logging
import sys

from documentcloud.addon import AddOn

from utils import (
    build_installation_data,
    first_value,
    load_georisques_data,
    normalize,
    save_with_backoff,
    utcnow_iso,
)

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
# The DocumentCloud/squarelet client logs every HTTP request at INFO (very noisy,
# one line per API call incl. the full PUT body); quiet it to WARNING so our own
# progress/diff logs stay readable.
logging.getLogger("squarelet").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# Log a running total every this many processed documents.
PROGRESS_EVERY = 1000

# Abort the whole run once this many documents have failed to update (a "give-up"
# is one document whose processing raised after its retries — usually a save that
# exhausted its 429 backoff). Guards against grinding on while DocumentCloud is
# hard rate-limiting or otherwise broken.
MAX_GIVE_UPS = 10


class GeorisquesMetadataUpdater(AddOn):
    """An Add-On for DocumentCloud."""

    def check_time_limit(self):

        if self.time_limit != 0:

            limit_in_seconds = self.time_limit * 60
            elapsed = (datetime.now(timezone.utc) - self.start_time).total_seconds()

            if elapsed > limit_in_seconds:
                logger.info(f"Closing due to time limit ({self.time_limit} minutes).")
                sys.exit(0)

    def doc_label(self, document):
        return f'Document {document.id} "{document.title}"'

    def log_changes(self, document, changes, prefix=""):
        label = self.doc_label(document)
        if not changes:
            logger.info(f"{prefix}{label}: no metadata changes.")
            return
        logger.info(f"{prefix}{label}: {len(changes)} field(s) to update:")
        for key, (old, new) in sorted(changes.items()):
            logger.info(f"{prefix}    {key}: {old!r} -> {new!r}")

    def stamp_and_save(self, document):
        """Set last_metadata_update to now and persist the document."""
        if document.data is None:
            document.data = {}
        document.data["last_metadata_update"] = utcnow_iso()
        save_with_backoff(document)

    def update_metadata(self, document):
        """Reconcile one document's installation-level metadata against the CSV.

        - No matching installation -> stamp last_metadata_update only (the doc
          leaves the queue), change nothing else.
        - Otherwise -> update the installation_* keys that differ, then stamp.
        In dry-run mode nothing is written; the changes that *would* be made are
        logged instead (and last_metadata_update is left untouched).
        """
        if document.data is None:
            document.data = {}

        code_aiot = first_value(document.data.get("installation_aiot_code"))
        target = (
            build_installation_data(code_aiot, self.df_installations, self.rubriques_by_aiot)
            if code_aiot
            else {}
        )

        if not target:
            reason = (
                "no installation_aiot_code"
                if not code_aiot
                else f"codeAiot '{code_aiot}' not in Géorisques CSV"
            )
            if self.dry_run:
                logger.info(
                    f"[dry-run] {self.doc_label(document)}: {reason} -> would stamp "
                    "last_metadata_update only."
                )
            else:
                logger.warning(
                    f"{self.doc_label(document)}: {reason}; stamping "
                    "last_metadata_update only."
                )
                self.stamp_and_save(document)
            return

        changes = {}
        for key, new_value in target.items():
            current_value = document.data.get(key)
            if normalize(current_value) != normalize(new_value):
                changes[key] = (current_value, new_value)

        if self.dry_run:
            self.log_changes(document, changes, prefix="[dry-run] ")
            return

        # Non-dry run: don't log per-doc changes (only the every-100 progress and
        # the final total are printed); just apply and save.
        for key, (_old, new_value) in changes.items():
            document.data[key] = new_value
        self.stamp_and_save(document)

    def process_documents(self, documents):
        """Run update_metadata over a search result, honouring the time limit
        and the optional max_documents cap (shared across both phases).

        A failure on one document is logged and skipped (the doc is left unstamped
        so it is retried on a later run) rather than aborting the whole batch.
        """
        for document in documents:
            if self.max_documents and self.processed_count >= self.max_documents:
                logger.info(f"Reached max_documents limit ({self.max_documents}); stopping.")
                break
            self.check_time_limit()
            self.processed_count += 1  # count the doc as processed once we commit to it
            try:
                self.update_metadata(document)
            except Exception as exc:  # noqa: BLE001 - keep the batch going
                logger.exception(f"Failed to update {self.doc_label(document)}: {exc}")
                self.failure_count += 1
                if self.failure_count >= MAX_GIVE_UPS:
                    logger.error(
                        f"Aborting run: {self.failure_count} documents failed to update "
                        f"(max_give_ups={MAX_GIVE_UPS}). DocumentCloud may be rate-limiting "
                        "or unavailable; the remaining documents will be retried next run."
                    )
                    sys.exit(1)
            if self.processed_count % PROGRESS_EVERY == 0:
                logger.info(f"Processed {self.processed_count} documents so far...")

    def main(self):
        """The main add-on functionality goes here."""

        self.start_time = datetime.now(timezone.utc)

        # Add a custom user agent here to positively identify yourself
        self.client.session.headers.update(
            {"User-Agent": "Disclose Georisques Metadata Updater Add-On"}
        )

        # Variables from config.yaml
        self.project = self.data.get("project", None)
        self.time_limit = self.data["time_limit"]
        self.dry_run = self.data.get("dry_run", False)
        self.max_documents = self.data.get("max_documents", 0)
        self.processed_count = 0
        self.failure_count = 0

        if self.dry_run:
            logger.info("Running in DRY-RUN mode: no document will be modified.")

        # Fetch & load Géorisques data (installations table + rubriques index)
        self.df_installations, self.rubriques_by_aiot = load_georisques_data()
        logger.info(
            f"Loaded {len(self.df_installations)} installations "
            f"({len(self.rubriques_by_aiot)} with rubriques)."
        )

        try:
            # 1 Documents never updated yet.
            logger.info("Phase 1: documents with no last_metadata_update...")
            non_updated_docs = self.client.documents.search(
                f"project:{self.project} -data_last_metadata_update:*"
            )
            self.process_documents(non_updated_docs)

            # 2 Already-updated documents, least-recently-updated first.
            # Bound to documents stamped BEFORE this run started so a run never
            # re-processes what it just wrote (the docs stamped in phase 1, or its
            # own phase-2 writes). The range value has colons, hence the quoting.
            logger.info("Phase 2: previously-updated documents (least recently first)...")
            start_token = self.start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            already_updated_docs = self.client.documents.search(
                f'project:{self.project} '
                f'data_last_metadata_update:[* TO "{start_token}"] '
                f"sort:data_last_metadata_update"
            )
            self.process_documents(already_updated_docs)
        finally:
            # Always report the total, whatever ended the run: natural completion,
            # the max_documents cap, the time limit (sys.exit), or the give-up abort.
            logger.info(f"Run finished. Processed {self.processed_count} documents.")


if __name__ == "__main__":
    GeorisquesMetadataUpdater().main()
