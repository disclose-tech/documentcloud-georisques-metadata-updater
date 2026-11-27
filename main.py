from collections import Counter
from datetime import datetime, timezone
import logging
import sys

from documentcloud.addon import AddOn
from documentcloud.exceptions import APIError
import ftfy

from utils import (
    BULK_LIMIT,
    DEPARTMENTS_KEY,
    INSEE_KEY,
    MANAGED_KEYS,
    MAX_BLANKED_PERCENT,
    MUNICIPALITY_KEY,
    POSTAL_KEY,
    REMOVED_KEY,
    REQUIRED_FIELDS,
    WITHDRAWN_KEY,
    InseeGeocoder,
    build_installation_data,
    build_title,
    count_blanked,
    department_of,
    first_value,
    keep,
    load_georisques_data,
    matches_lossy,
    put_batch,
    put_item,
    same_place,
    updated_data,
)

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logging.getLogger("squarelet").setLevel(logging.WARNING)
logging.getLogger("documentcloud").addFilter(
    lambda record: not record.getMessage().startswith("Rate limit reached")
)
logger = logging.getLogger(__name__)

# Log progress every this many read documents.
PROGRESS_EVERY = 10000

# The API's maximum; reading the whole project is what bounds a run
PER_PAGE = 100

# Abort the whole run once this many documents have failed to update
MAX_GIVE_UPS = 5


class GeorisquesMetadataUpdater(AddOn):
    """Reconciles every document's installation metadata with Géorisques."""

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
        """Queue one document's write when its installation keys, description or "?"-damaged title
        differ from Géorisques.

        A document with no AIOT code is left untouched, and one Géorisques no longer lists is marked
        removed (see mark_removed). One whose installation Géorisques has blanked goes private (see
        withdrawn_access).
        """
        data = document.data or {}
        code_aiot = first_value(data.get("installation_aiot_code"))
        if not code_aiot:
            self.counts["no installation_aiot_code"] += 1
            return
        if code_aiot not in self.df_installations.index:
            self.mark_removed(document, data, code_aiot)
            return
        target = build_installation_data(code_aiot, self.df_installations, self.rubriques_by_aiot)

        self.complete_place(code_aiot, data, target)
        access = self.withdrawn_access(document, data, target)
        new_data, changes = updated_data(data, target)
        if access:
            changes["access"] = (document.access, access)
        description = None
        name = target.get("installation_name", "")
        # The scraper's description, shown as the folder name: it follows a renamed installation
        stored_description = getattr(document, "description", None)
        if name and stored_description != f"{name} ({code_aiot})":
            description = f"{name} ({code_aiot})"
            changes["description"] = (stored_description, description)
        title = None
        filename = self.repaired_filename(data)
        if filename:
            new_data = dict(new_data or data)
            changes["original_filename"] = (first_value(data.get("original_filename")), filename)
            new_data["original_filename"] = filename
            title = self.repaired_title(document, data, filename)
            if title:
                changes["title"] = (document.title, title)
        if not changes:
            self.counts["unchanged"] += 1
            return
        self.queue(document, new_data or data, changes, description, title, access)

    def mark_removed(self, document, data, code_aiot):
        """A document whose installation Géorisques no longer lists keeps its stored values; the first time,
        it's marked removed with the run's date, logged once per installation at the end of the run."""
        self.counts["installation removed from Géorisques"] += 1
        target = {}
        keep(data, target, MANAGED_KEYS)
        target.setdefault(REMOVED_KEY, self.today)
        new_data, changes = updated_data(data, target)
        if not changes:
            return
        # The PUT carries `access`, which DocumentCloud refuses while a document processes
        if getattr(document, "status", None) in ("pending", "readable"):
            self.counts["marked removed later (pending or readable)"] += 1
            return
        if self.queue(document, new_data, changes):
            self.newly_removed.add((code_aiot, first_value(data.get("installation_name"))))

    def queue(self, document, data, changes, description=None, title=None, access=None):
        """Count a document's changes and queue its write (only logged in a dry run). False when it can't
        be sent."""
        missing = [field for field in REQUIRED_FIELDS if getattr(document, field, None) is None]
        if missing:
            self.counts["skipped"] += 1
            logger.warning(f"{self.doc_label(document)}: skipped, no {', '.join(missing)} to send back.")
            return False

        self.counts["changed"] += 1
        if access:
            self.counts["made private (blanked)" if access == "private" else "made public (name back)"] += 1
        for key, (_old, new) in changes.items():
            self.key_counts[f"{key} {'deleted' if new is None else 'set'}"] += 1
        if self.dry_run:
            self.log_changes(document, changes)
            return True

        self.batch.append(put_item(document, data, description, title, access))
        if len(self.batch) >= BULK_LIMIT:
            self.flush()
        return True

    def withdrawn_access(self, document, data, target):
        """The document's new access when Géorisques blanked its installation or gave the name back, else None.

        Sets the withdrawn marker in ``target`` along with it, and removes it once the document is public
        again. A document made private by hand carries no marker, so it is never made public.
        """
        # The marker stays as stored unless a case below changes it
        keep(data, target, (WITHDRAWN_KEY,))

        # Blanked: a public document goes private and is marked withdrawn; any other is left alone
        if not target.get("installation_name"):
            if document.access != "public":
                return None
            access = "private"
            target[WITHDRAWN_KEY] = "true"
        # Named again after withdrawal
        elif WITHDRAWN_KEY in data:
            if document.access == "private":
                # Back to public, marker kept until the next run: DocumentCloud makes a document public
                # in a background task, and one that stalls would leave it private and unmarked
                access = "public"
            else:
                # Already public (our switch finished, or a hand change): only the marker goes
                target.pop(WITHDRAWN_KEY)
                return None
        # Named, no marker: a normal document, or one made private by hand, which stays private
        else:
            return None

        # The change waits for a later run, marker as stored, when the read looks broken or the document
        # isn't success
        if self.access_paused:
            reason = "paused (too many blanked)"
        elif getattr(document, "status", None) != "success":
            reason = "pending (not success)"
        else:
            return access
        keep(data, target, (WITHDRAWN_KEY,))
        self.counts[f"{access} {reason}"] += 1
        return None

    def repaired_filename(self, data):
        """Géorisques's name for the document's file when the stored one lost characters to "?", else None."""
        stored = first_value(data.get("original_filename"))
        if not stored or "?" not in stored:
            return None
        raw = self.doc_names.get(first_value(data.get("file_id")))
        if raw is None:
            return None
        # Cleaned as the scraper's CleanTextPipeline cleans a name before storing it
        clean = ftfy.fix_text(raw, unescape_html=True).strip()
        if clean == stored:  # Géorisques's own name has the "?"
            return None
        
        if not (matches_lossy(stored, clean) or matches_lossy(stored, raw.strip())):
            self.counts["original_filename kept (not a lost-character match)"] += 1
            return None
        return clean

    def repaired_title(self, document, data, filename):
        """The title rebuilt from the repaired file name, or None when someone set the title by hand."""
        doc_type = first_value(data.get("doc_type"))
        if document.title != build_title(doc_type, first_value(data.get("original_filename"))):
            self.counts["title kept (not the built form)"] += 1
            return None
        return build_title(doc_type, filename)

    def complete_place(self, code_aiot, data, target):
        """Add to ``target`` what the scraper derives from Géorisques: a geocoded INSEE code and the département."""
        if INSEE_KEY not in target:
            self.resolve_insee_code(code_aiot, data, target)
        code_insee = first_value(target.get(INSEE_KEY))
        if code_insee:
            target[DEPARTMENTS_KEY] = department_of(code_insee)
        else:
            keep(data, target, (DEPARTMENTS_KEY,))

    def resolve_insee_code(self, code_aiot, data, target):
        """Geocode Géorisques's place when the document has no code, or its place changed.

        The commune's name is fril the Géorisques API (the geocoder's for a "?" name), never the stored one,
        which may hold the CSV export's "?".
        """
        stored_code = first_value(data.get(INSEE_KEY))
        if stored_code and same_place(data, target):
            keep(data, target, (INSEE_KEY,))
            return

        postal, commune = target.get(POSTAL_KEY), target.get(MUNICIPALITY_KEY)
        if postal and commune:
            result, reason = self.geocoder.resolve(postal, commune)
        else:
            result, reason = None, "no postal code or commune"
        if result:
            target[INSEE_KEY] = result["code_insee"]
            if result["commune"]:
                target[MUNICIPALITY_KEY] = result["commune"]
            return

        if stored_code:
            # The stored code stays with the postal code it came from, so the next run tries again
            keep(data, target, (INSEE_KEY, POSTAL_KEY))
        if reason != "geocoding stopped":
            kept = "stored place kept" if stored_code else "Géorisques place used"
            self.unresolved[(code_aiot, postal, commune, reason, kept)] += 1

    def flush(self):
        """Write the queued documents to DocumentCloud in one bulk request, then empty the queue."""
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
        self.today = self.start_time.date().isoformat()

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
        self.geocoder = InseeGeocoder()
        self.unresolved = Counter()
        self.newly_removed = set()

        if self.dry_run:
            logger.info("Running in DRY-RUN mode: no document will be modified.")

        # Fetch & load Géorisques data (installations table, rubriques index, file names)
        self.df_installations, self.rubriques_by_aiot, self.doc_names = load_georisques_data()
        logger.info(
            f"Loaded {len(self.df_installations)} installations "
            f"({len(self.rubriques_by_aiot)} with rubriques)."
        )
        blanked = count_blanked(self.df_installations)
        installations = self.df_installations.index.nunique()
        self.access_paused = blanked * 100 > MAX_BLANKED_PERCENT * installations
        logger.info(f"Géorisques shows {blanked} of {installations} installations with no name (blanked).")
        if self.access_paused:
            logger.error(
                f"Over {MAX_BLANKED_PERCENT} % of installations have no name, more than any real blanking: "
                "a broken read? No document's access changes this run, and the run ends failed."
            )

        try:
            # Sorted by id so the cursor is stable while the run writes
            documents = self.client.documents.search(
                f"project:{self.project}", sort="id", per_page=PER_PAGE
            )
            logger.info(f"Comparing the project's {documents.count} documents with Géorisques...")
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
            if self.newly_removed:
                logger.info(f"Newly removed from Géorisques: {len(self.newly_removed)} installation(s).")
            for code_aiot, name in sorted(self.newly_removed, key=str):
                logger.info(f"AIOT {code_aiot}: {name or '(no stored name)'}")
            logger.info(f"Geocoder: {dict(self.geocoder.stats) or 'no lookup'}.")
            if self.unresolved:
                logger.info(f"Unresolved INSEE codes: {sum(self.unresolved.values())} document(s).")
            for (code_aiot, postal, commune, reason, kept), count in sorted(self.unresolved.items(), key=str):
                logger.warning(
                    f"AIOT {code_aiot}: no INSEE code for {postal} {commune} ({reason}): "
                    f"{count} document(s), {kept}."
                )
        if self.access_paused:
            sys.exit(1)


if __name__ == "__main__":
    GeorisquesMetadataUpdater().main()
