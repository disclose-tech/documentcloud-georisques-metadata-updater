# Géorisques Metadata Updater — DocumentCloud Add-On

[Please see the Add-On documentation](https://github.com/MuckRock/documentcloud-hello-world-addon/wiki/)

This Add-On keeps the **installation-level metadata** of already-uploaded Géorisques
documents in sync with the latest Géorisques source data. It is the maintenance
counterpart to the Géorisques scraper (`documentcloud-georisques-scraper`, which does the
initial upload): the scraper stamps each document with an `installation_aiot_code`, and this
Add-On revisits those documents and refreshes their `installation_*` metadata from the
current Géorisques CSV export.

## What it does

On each run it:

1. Downloads the full Géorisques *installations classées* CSV export
   (`InstallationClassee.csv` + `rubriqueIC.csv`, paginated at 6000 rows/page) and builds
   an installations table indexed by `codeAiot` plus a per-installation index of unique
   ICPE rubrique numbers.
2. Processes the target project's documents as a **resumable queue**, stamping a
   `last_metadata_update` timestamp on every document it touches:
   - **Phase 1** — documents with no `last_metadata_update` yet.
   - **Phase 2** — previously-updated documents, least-recently-updated first, bounded to
     documents stamped *before this run started* so a run never re-processes its own writes.
   Because progress is persisted per-document, a crash or a time-limit exit simply resumes
   on the next run.
3. For each document, looks up its `installation_aiot_code` in the CSV and updates the
   `installation_*` metadata keys that have changed (list-valued keys such as
   `installation_nomenclature_sections` and `installation_topics` are compared as sets, so
   re-ordering is not a change). Documents whose `installation_aiot_code` is missing or not
   found in the CSV are stamped and left otherwise untouched, so they leave the queue.

**Scope:** only installation-level metadata (`installation_*`) is reconciled. Per-document
file-level fields (date, file id, source URLs, doc type, …) and geocoder-derived
`departments` are left untouched.