from collections import Counter
import logging
import math
import re
import time

from documentcloud.exceptions import APIError
import ftfy
import pandas as pd
import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_fixed,
)

logger = logging.getLogger(__name__)

# The JSON API, not the CSV export: the export is Latin-1 and writes "?" for every
# character Latin-1 lacks (’ œ –), which no decoding can bring back
API_ENDPOINT = "https://georisques.gouv.fr/api/v1/installations_classees"
# Géorisques resets or cuts large responses: 200-row pages, a failing one fetched again as 2 × 100
PAGE_SIZE = 200
SPLIT_SIZE = 100
HEADERS = {"User-Agent": "Disclose Georisques Metadata Updater Add-On - contact tech@disclose.ngo"}
# Seconds between two API pages
PAGE_DELAY = 1
# Seconds before the pages that failed during the crawl are tried once more
RETRY_ROUND_DELAY = 300
# Log progress every this many pages
PAGE_PROGRESS_EVERY = 100


# Géorisques source parsing/mapping
# Installation column (the CSV export's names, kept by installation_row) -> DocumentCloud data key.
INSTALLATION_FIELD_MAP = {
    "codeNaf": "installation_naf_code",
    "numeroSiret": "installation_siret",
    "statutSeveso": "installation_seveso_status",
    "ied": "installation_ied",
    "prioriteNationale": "installation_national_priority",
    "etatActivite": "installation_activity_status",
    "regimeVigueur": "installation_regime",
    "codePostal": "installation_postal_code",
    "codeInsee": "installation_municipality_insee_code",
    "commune": "installation_municipality",
    "raisonSociale": "installation_name",
}

INSEE_KEY = INSTALLATION_FIELD_MAP["codeInsee"]
MUNICIPALITY_KEY = INSTALLATION_FIELD_MAP["commune"]
POSTAL_KEY = INSTALLATION_FIELD_MAP["codePostal"]
DEPARTMENTS_KEY = "departments"

# Boolean theme columns aggregated into the installation_topics list.
THEME_COLUMNS = ["bovins", "porcs", "volailles", "carriere", "eolienne", "industrie"]

# Géorisques's own data holds some UTF-8 read as Latin-1 ("CoopÃ‰rative"), repaired with
# ftfy like the scraper's CleanTextPipeline; clean text comes out unchanged
FTFY_KEYS = {"installation_name", "installation_address", "installation_municipality"}

INSTALLATION_PAGE_URL = "https://www.georisques.gouv.fr/risques/installations/donnees/details/{code_aiot}"

# JSON key of each installation column the code reads
INSTALLATION_COLUMNS = {
    **{column: column for column in INSTALLATION_FIELD_MAP},
    "numeroSiret": "siret",
    "regimeVigueur": "regime",
    **{column: column for column in THEME_COLUMNS},
    **{f"adresse{i}": f"adresse{i}" for i in (1, 2, 3)},
}

# Start of synchronized block

# Kept identical across scraper & metadata updater:
# any difference makes the updater rewrite every new upload the day after.

# Whole address parts that only stand in for a missing value, as Géorisques's data has them
ADDRESS_PLACEHOLDER = re.compile(r"(?i)x+|0+|n[ée]ant|inconnue?|adresse inconnue|sans adresse|nd|na")
# Separators left at either end once parts are joined or dropped ("3 rue X -", "- DE OLIVIERA")
ADDRESS_EDGES = re.compile(r"^[\s\-–—‑/,;:]+|[\s\-–—‑/,;:]+$")


def column_value(value):
    """A JSON value as the CSV export wrote it: "" for null, "true"/"false" for booleans."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def installation_row(record):
    """One JSON installation as the row the CSV export gave, under its column names."""
    row = {column: column_value(record.get(key)) for column, key in INSTALLATION_COLUMNS.items()}
    row["codeAiot"] = record["codeAIOT"]
    row["url"] = INSTALLATION_PAGE_URL.format(code_aiot=record["codeAIOT"])
    return row


def tidy_whitespace(text):
    """Strip, and turn any run of 2+ whitespace characters into one space (a lone NBSP stays)."""
    return re.sub(r"\s{2,}", " ", text.strip())


def build_address(row):
    """Join adresse1/2/3 into one address.

    Drops a part with no letter or digit (" -- ", "/"), a placeholder, or a repeat of an earlier part.
    """
    parts = []
    for i in (1, 2, 3):
        part = tidy_whitespace(str(row.get(f"adresse{i}", "")))
        if not re.search(r"[^\W_]", part) or ADDRESS_PLACEHOLDER.fullmatch(part):
            continue
        # "Lieu--dit" -> "Lieu-dit"; an arrow ("BORDEAUX --> TOULOUSE") stays
        part = re.sub(r"-{2,}(?!>)", "-", part)
        if part.lower() not in (kept.lower() for kept in parts):
            parts.append(part)
    return ADDRESS_EDGES.sub("", " ".join(parts))

# End of synchronized block


def build_installation_data(code_aiot, df_installations, rubriques_by_aiot):
    """What Géorisques says about one installation, as the data keys the scraper uploads.

    ``{}`` when Géorisques doesn't list the codeAiot. Empty values are left out, as the scraper does;
    the geocoded INSEE code and the département are added afterwards (main.complete_place).
    """
    if code_aiot not in df_installations.index:
        return {}

    row = df_installations.loc[code_aiot]
    if isinstance(row, pd.DataFrame):  # duplicate codeAiot: keep the first row
        row = row.iloc[0]

    data = {}
    for column, dc_key in INSTALLATION_FIELD_MAP.items():
        # Tidy whitespaces like the scraper does.
        value = tidy_whitespace(row.get(column, ""))
        if value:
            data[dc_key] = value

    themes = [column for column in THEME_COLUMNS if row.get(column, "") == "true"]
    if themes:
        data["installation_topics"] = themes

    rubriques = rubriques_by_aiot.get(code_aiot)
    if rubriques:
        data["installation_nomenclature_sections"] = rubriques

    address = build_address(row)
    if address:
        data["installation_address"] = address

    for key in FTFY_KEYS:
        if key in data:
            data[key] = ftfy.fix_text(data[key], unescape_html=True).strip()
            if not data[key]:  # an entity-only value, e.g. "&nbsp;"
                del data[key]

    return data


def _get_page(page_size, page):
    """One page of the installations API, its JSON body as a dict."""
    params = {"page_size": page_size, "page": page}
    response = requests.get(API_ENDPOINT, params=params, headers=HEADERS, timeout=(60, 300))  # (connect, read)
    response.raise_for_status()
    # A cut body raises ChunkedEncodingError in get() or JSONDecodeError here: both are RequestExceptions
    return response.json()


_RETRY_LOG = before_sleep_log(logger, logging.WARNING)
# 2 tries at PAGE_SIZE; then fetch_page reads the same rows as SPLIT_SIZE pages, which Géorisques serves reliably
_get_full_page = retry(
    retry=retry_if_exception_type(requests.RequestException),
    wait=wait_fixed(5),
    stop=stop_after_attempt(2),
    before_sleep=_RETRY_LOG,
    reraise=True,
)(_get_page)
_get_split_page = retry(
    retry=retry_if_exception_type(requests.RequestException),
    # 15, 30, 60, 120, 240 then 300 s between attempts: rides out a ~13-minute outage
    wait=wait_exponential(multiplier=15, max=300),
    stop=stop_after_attempt(7),
    before_sleep=_RETRY_LOG,
    reraise=True,
)(_get_page)


def fetch_page(page):
    """``(results, records, split)`` for one PAGE_SIZE page: Géorisques's installation count,
    the page's installation records, and whether the page had to be fetched as SPLIT_SIZE pages.

    Raises RequestException when a split page still fails.
    """
    try:
        body = _get_full_page(PAGE_SIZE, page)
        return body["results"], body["data"], False
    except requests.RequestException as exc:
        logger.warning(f"Page {page} failed twice ({exc!r}): fetching it as pages of {SPLIT_SIZE}")

    records = []
    per_page = PAGE_SIZE // SPLIT_SIZE
    for split_page in range((page - 1) * per_page + 1, page * per_page + 1):
        body = _get_split_page(SPLIT_SIZE, split_page)
        records += body["data"]
        time.sleep(PAGE_DELAY)
    return body["results"], records, True


def document_names(record):
    """{file id: Géorisques's name for the file} for one installation's documents that have a file."""
    names = {}
    for inspection in record.get("inspections") or []:
        file = inspection.get("fichierInspection") or {}
        if file.get("urlFichier") and file.get("identifiantFichier"):
            names[file["identifiantFichier"]] = column_value(file.get("nomFichier"))
    for file in record.get("documentsHorsInspection") or []:
        if file.get("urlFichier") and file.get("identifiantFichier"):
            names[file["identifiantFichier"]] = column_value(file.get("nomFichier"))
    return names


def load_georisques_data():
    """Read every installation from the Géorisques API.

    A page that still fails is tried once more after the others, RETRY_ROUND_DELAY later; if it
    fails again, or page 1 fails (no installation count to page by), RequestException is raised:
    the run must not go on with part of the data.

    Returns the installations table (indexed by codeAiot, columns as the CSV export named them),
    the rubriques index ``{codeAiot: sorted unique numeroRubrique}`` and ``{file id: file name}``.
    """
    rows = []
    rubriques_sets = {}
    doc_names = {}
    page, last_page, split_pages, results = 1, None, 0, 0
    failed_pages = []

    def add(records):
        for record in records:
            rows.append(installation_row(record))
            numbers = {column_value(r.get("numeroRubrique")).strip() for r in record.get("rubriques") or []}
            rubriques_sets.setdefault(record["codeAIOT"], set()).update(numbers - {""})
            doc_names.update(document_names(record))

    while last_page is None or page <= last_page:
        try:
            results, records, split = fetch_page(page)
        except requests.RequestException as exc:
            if last_page is None:
                raise
            logger.warning(f"Page {page} still fails ({exc!r}): tried again after the other pages.")
            failed_pages.append(page)
        else:
            # A split page reports its own page count, so count from the total
            last_page = last_page or math.ceil(results / PAGE_SIZE)
            split_pages += split
            add(records)

        if page % PAGE_PROGRESS_EVERY == 0:
            logger.info(f"Read installations page {page}/{last_page}...")
        page += 1
        time.sleep(PAGE_DELAY)

    if failed_pages:
        logger.warning(f"Trying pages {failed_pages} again in {RETRY_ROUND_DELAY} s...")
        time.sleep(RETRY_ROUND_DELAY)
        for failed_page in failed_pages:
            _results, records, split = fetch_page(failed_page)
            split_pages += split
            add(records)

    df_installations = pd.DataFrame(rows, columns=["codeAiot", *INSTALLATION_COLUMNS], dtype=str)
    df_installations.set_index("codeAiot", inplace=True)
    read = df_installations.index.nunique()
    logger.info(
        f"Read {read} installations in {page - 1} pages of {PAGE_SIZE} "
        f"({split_pages} fetched as pages of {SPLIT_SIZE}, {len(failed_pages)} after a retry round); "
        f"Géorisques counts {results}."
    )
    if read < results:
        # Offset paging while Géorisques writes can skip a row; that installation's documents wait a run
        logger.warning(f"{results - read} installation(s) missed by the paging.")

    rubriques_by_aiot = {aiot: sorted(nums) for aiot, nums in rubriques_sets.items() if nums}
    return df_installations, rubriques_by_aiot, doc_names


# INSEE code geocoding, as the scraper's InseeCodePipeline does when Géorisques has no code
GEOCODER_URL = "https://data.geopf.fr/geocodage/search/"
# This many failed lookups in a row means the geocoder is down: no more lookups this run
GEOCODER_MAX_FAILURES = 5


@retry(
    retry=retry_if_exception_type(requests.RequestException),
    stop=stop_after_attempt(2),
    reraise=True,
)
def _geocode(code_postal, commune):
    """Properties of the geocoder's best match for "<postal code> <commune>", or None when nothing matches.

    Raises RequestException on a network error, a 5xx or 429, or a reply of the wrong shape.
    """
    params = {"q": f"{code_postal} {commune}", "limit": 1}
    response = requests.get(GEOCODER_URL, params=params, headers=HEADERS, timeout=10)
    # A rejected query (4xx, bar 429) fails the same way every time: no match, not an outage
    if 400 <= response.status_code < 500 and response.status_code != 429:
        return None
    response.raise_for_status()
    body = response.json()
    features = body.get("features") if isinstance(body, dict) else None
    if not features:
        return None
    if not isinstance(features, list) or not isinstance(features[0], dict) or not isinstance(
        features[0].get("properties"), dict
    ):
        raise requests.RequestException(f"Unexpected geocoder response: {body!r:.200}")
    return features[0]["properties"]


class InseeGeocoder:
    """Finds the INSEE code of a (postal code, commune) with the scraper's rules.

    Each place is looked up once per run: matches, misses and failures are all cached.
    """

    def __init__(self):
        self.cache = {}
        self.failures_in_a_row = 0
        self.stats = Counter()

    @property
    def stopped(self):
        return self.failures_in_a_row >= GEOCODER_MAX_FAILURES

    def resolve(self, code_postal, commune):
        """``(result, None)`` when the geocoder finds the place, else ``(None, reason)``.

        ``result`` is ``{"code_insee", "commune"}``. ``commune`` is the geocoder's correct spelling when the
        name has a "?" in place of a letter (Crèvec?ur for Crèvecœur), else None.
        """
        key = (code_postal, commune)
        if key in self.cache:
            return self.cache[key]
        if self.stopped:
            return None, "geocoding stopped"

        # A "?" standing for a lost letter (a few are in Géorisques's own data) defeats the geocoder
        corrupted = "?" in commune
        self.stats["lookups"] += 1
        try:
            props = _geocode(code_postal, commune.replace("?", "").strip()) or {}
        except requests.RequestException as exc:
            # Cached too, so one failing place counts once towards the breaker; the next run retries
            self.stats["failed"] += 1
            self.failures_in_a_row += 1
            if self.stopped:
                logger.warning(
                    f"Geocoding stopped for this run after {GEOCODER_MAX_FAILURES} failed lookups in a row."
                )
            self.cache[key] = None, f"lookup failed: {exc!r}"
            return self.cache[key]
        self.failures_in_a_row = 0

        if not props.get("citycode"):
            outcome = None, "no match"
        else:
            corrected = None
            # The geocoder's name replaces a "?" name only when its postcode is Géorisques's
            if corrupted and props.get("postcode") == code_postal:
                corrected = props.get("city") or props.get("name")
            outcome = {"code_insee": str(props["citycode"]), "commune": corrected}, None
        self.stats["matched" if outcome[0] else "missed"] += 1
        self.cache[key] = outcome
        return outcome


def department_of(code_insee):
    """The département of an INSEE commune code, as the scraper's TagDepartmentsPipeline derives it."""
    # Overseas départements and collectivités take three digits
    return code_insee[:3] if code_insee[:2] in ("97", "98") else code_insee[:2]


# ---------------------------------------------------------------------------
# DocumentCloud data helpers
# ---------------------------------------------------------------------------

def first_value(value):
    if isinstance(value, list):
        return value[0] if value else None
    return value


def normalize_for_comparison(value):
    if value is None:
        return []
    if isinstance(value, list):
        return sorted(str(item) for item in value)
    return [str(value)]


def matches_lossy(lossy, clean):
    """Whether ``clean`` is ``lossy`` with each "?" (a character the CSV export lost) restored.

    >>> matches_lossy("Sch?lcher", "Schœlcher"), matches_lossy("Sch?lcher", "Schoelcher")
    (True, False)
    """
    pattern = ".".join(re.escape(part) for part in lossy.split("?"))
    return re.fullmatch(pattern, clean, re.DOTALL) is not None


def same_place(data, target):
    """Whether the document's postal code and commune are still Géorisques's."""
    if first_value(data.get(POSTAL_KEY)) != target.get(POSTAL_KEY):
        return False
    stored, current = first_value(data.get(MUNICIPALITY_KEY)), target.get(MUNICIPALITY_KEY)
    if stored is None or current is None:
        return stored == current
    # A "?" stands for a lost letter on either side: a document uploaded from the CSV export may
    # hold "Sch?lcher", and a "?" in Géorisques's own name matches the geocoder's spelling stored
    return stored == current or matches_lossy(stored, current) or matches_lossy(current, stored)


def keep(data, target, keys):
    """Copy the document's current values for ``keys`` into ``target`` (or remove a key the document
    lacks), so the update leaves those keys as they are.
    """
    for key in keys:
        if key in data:
            target[key] = data[key]
        else:
            target.pop(key, None)


# Every key the updater writes: one missing from the target is deleted
MANAGED_KEYS = set(INSTALLATION_FIELD_MAP.values()) | {
    "installation_topics",
    "installation_nomenclature_sections",
    "installation_address",
    DEPARTMENTS_KEY,
}


def updated_data(data, target):
    """Apply Géorisques's values in ``target`` to a document's ``data``.

    Returns the complete new ``data`` (a bulk update replaces it, it does not merge)
    and the changes as ``{key: (old, new)}``, ``new`` being None for a deleted key.
    Returns ``(None, {})`` when nothing differs.
    """
    changes = {}
    for key in sorted(MANAGED_KEYS):
        old, new = data.get(key), target.get(key)
        if normalize_for_comparison(old) != normalize_for_comparison(new):
            changes[key] = (old, new)
    if not changes:
        return None, {}

    new_data = dict(data)
    for key, (_old, new) in changes.items():
        if new is None:
            del new_data[key]
        else:
            new_data[key] = new
    return new_data, changes


# Bulk writes
# DocumentCloud's REST_BULK_LIMIT
BULK_LIMIT = 25

# A PUT is a full update: these fall back to private / eng / pdf when left out
REQUIRED_FIELDS = ["access", "language", "original_extension"]

# What `Document.save()` sends, plus `original_extension`
PUT_FIELDS = [
    "access",
    "data",
    "description",
    "language",
    "original_extension",
    "publish_at",
    "published_url",
    "related_article",
    "source",
    "title",
]


def put_item(document, data, description=None, title=None):
    """One bulk PUT entry: the document's writable fields with ``data`` (and ``description``, ``title``) replaced."""
    item = {"id": document.id}
    item |= {field: getattr(document, field) for field in PUT_FIELDS if hasattr(document, field)}
    item["data"] = data
    if description is not None:
        item["description"] = description
    if title is not None:
        item["title"] = title
    return item


# Document titles, built exactly as the scraper's TitlePipeline builds them
# File extensions to drop from the end of a title (the scraper's TITLE_EXTENSIONS)
TITLE_EXTENSIONS = ["pdf", "pdfa", "doc", "docx", "odt", "rtf", "txt"]
TITLE_EXTENSIONS_RE = re.compile(r"(?:\s*\.(?:" + "|".join(TITLE_EXTENSIONS) + r"))+$", re.IGNORECASE)


def beautify_title(title):
    """The scraper's beautify_title: no underscores, single spaces, no trailing file extension."""
    beautified = re.sub(r"\s+", " ", title.replace("_", " ")).strip()
    beautified = TITLE_EXTENSIONS_RE.sub("", beautified).strip()
    # DocumentCloud rejects an empty title
    return beautified or title


def build_title(doc_type, original):
    """The scraper's build_title: `<doc_type> - <beautified filename>`, the filename alone for `Autre`."""
    if not doc_type or doc_type == "Autre":
        return beautify_title(original)
    return f"{doc_type} - {beautify_title(original)}"


def is_transient(exc):
    """A 429, a 5xx or a network failure; a 400 would fail the same way again."""
    if isinstance(exc, APIError):
        return exc.status_code == 429 or (exc.status_code or 0) >= 500
    return isinstance(exc, requests.RequestException)


@retry(
    retry=retry_if_exception(is_transient),
    wait=wait_exponential(multiplier=1, max=30),
    stop=stop_after_attempt(3),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def put_batch(client, items):
    # The client throttles `PUT documents/` itself (12 calls/min, 25 documents each)
    client.put("documents/", json=items)
