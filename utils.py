from collections import Counter
import logging
import os
import re
import time
import zipfile

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
)

logger = logging.getLogger(__name__)

# Géorisques cuts large CSVs mid-download
PAGE_SIZE = 2000
CSV_ENDPOINT = "https://georisques.gouv.fr/api/v1/csv/installations_classees?page_size={page_size}&page={page}"
HEADERS = {"User-Agent": "Disclose Georisques Metadata Updater Add-On - contact tech@disclose.ngo"}
# Seconds between two CSV pages
PAGE_DELAY = 5
DOWNLOAD_FOLDER = "downloaded_files"


# Géorisques source parsing/mapping
# InstallationClassee.csv column -> DocumentCloud installation-level data key.
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

# Text the CSV can hold garbled (UTF-8 read as Latin-1) or with HTML entities, repaired
# with ftfy like the scraper's CleanTextPipeline; clean text comes out unchanged
FTFY_KEYS = {"installation_name", "installation_address", "installation_municipality"}


def read_installations_csv(csv_path):
    """Read InstallationClassee.csv exactly as the scraper does (indexed by codeAiot)."""
    df = pd.read_csv(csv_path, sep=";", encoding="ISO-8859-1", dtype=str)
    df.fillna("", inplace=True)
    df.set_index("codeAiot", inplace=True)
    return df


def build_rubriques_by_aiot(csv_path):
    """{codeAiot: its nomenclature sections (numeroRubrique), unique and sorted}, as the scraper builds them.

    rubriqueIC.csv has one row per alinéa, so a section repeats for one installation.
    """
    df = pd.read_csv(csv_path, sep=";", encoding="ISO-8859-1", dtype=str)
    df.fillna("", inplace=True)

    sets_by_aiot = {}
    for aiot, num in zip(df["codeAiot"], df["numeroRubrique"]):
        if num:
            sets_by_aiot.setdefault(aiot, set()).add(num)
    return {aiot: sorted(nums) for aiot, nums in sets_by_aiot.items()}


def build_installation_address(row):
    """Join adresse1/2/3 into a single address string (scraper add_installation_adress)."""
    parts = [str(row.get(f"adresse{i}", "")) for i in (1, 2, 3) if row.get(f"adresse{i}", "")]
    return " ".join(parts).strip()


def build_installation_data(code_aiot, df_installations, rubriques_by_aiot):
    """What the CSV says about one installation, as the data keys the scraper uploads.

    ``{}`` when the CSV doesn't list the codeAiot. Empty values are left out, as the scraper does;
    the geocoded INSEE code and the département are added afterwards (main.complete_place).
    """
    if code_aiot not in df_installations.index:
        return {}

    row = df_installations.loc[code_aiot]
    if isinstance(row, pd.DataFrame):  # duplicate codeAiot: keep the first row
        row = row.iloc[0]

    data = {}
    for csv_col, dc_key in INSTALLATION_FIELD_MAP.items():
        value = row.get(csv_col, "")
        if value:
            data[dc_key] = value

    themes = [column for column in THEME_COLUMNS if row.get(column, "") == "true"]
    if themes:
        data["installation_topics"] = themes

    rubriques = rubriques_by_aiot.get(code_aiot)
    if rubriques:
        data["installation_nomenclature_sections"] = rubriques

    address = build_installation_address(row)
    if address:
        data["installation_address"] = address

    for key in FTFY_KEYS:
        if key in data:
            data[key] = ftfy.fix_text(data[key], unescape_html=True)

    return data


@retry(
    retry=retry_if_exception_type(requests.RequestException),
    # 15, 30, 60 then 120 s between attempts
    wait=wait_exponential(multiplier=15, max=120),
    stop=stop_after_attempt(5),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _download_csv_page(url):
    """One Géorisques CSV page (a zip), retried on network and HTTP errors."""
    response = requests.get(url, headers=HEADERS, timeout=(60, 300))  # (connect, read)
    response.raise_for_status()
    return response.content


def load_georisques_data():
    """Download every Géorisques CSV page and build the installations table +
    the rubriques index.
    """
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

    installations_frames = []
    rubriques_sets = {}
    page = 1

    while True:
        url = CSV_ENDPOINT.format(page_size=PAGE_SIZE, page=page)
        logger.info(f"Downloading installations CSV page {page}...")

        zip_path = os.path.join(DOWNLOAD_FOLDER, f"georisques_csv_page_{page}.zip")
        with open(zip_path, "wb") as f:
            f.write(_download_csv_page(url))

        extracted_path = os.path.join(DOWNLOAD_FOLDER, f"georisques_csv_page_{page}")
        with zipfile.ZipFile(zip_path) as archive:
            archive.extractall(extracted_path)

        df_page = read_installations_csv(os.path.join(extracted_path, "InstallationClassee.csv"))
        installations_frames.append(df_page)

        page_rubriques = build_rubriques_by_aiot(os.path.join(extracted_path, "rubriqueIC.csv"))
        for aiot, nums in page_rubriques.items():
            rubriques_sets.setdefault(aiot, set()).update(nums)

        # A page with fewer than PAGE_SIZE installations is the last one.
        if len(df_page) < PAGE_SIZE:
            break

        page += 1
        time.sleep(PAGE_DELAY)

    df_installations = pd.concat(installations_frames)
    rubriques_by_aiot = {aiot: sorted(nums) for aiot, nums in rubriques_sets.items()}
    return df_installations, rubriques_by_aiot


# INSEE code geocoding, as the scraper's InseeCodePipeline does when the CSV has no code
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
        CSV name has a "?" in place of a letter (Crèvec?ur for Crèvecœur), else None.
        """
        key = (code_postal, commune)
        if key in self.cache:
            return self.cache[key]
        if self.stopped:
            return None, "geocoding stopped"

        # The CSV holds characters Latin-1 lacks (œ) as "?", which the geocoder can't match
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
            # The geocoder's name replaces a "?" name only when its postcode is the CSV's
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


def same_place(data, target):
    """Whether the document's postal code and commune are still the ones in the CSV."""
    if first_value(data.get(POSTAL_KEY)) != target.get(POSTAL_KEY):
        return False
    stored, current = first_value(data.get(MUNICIPALITY_KEY)), target.get(MUNICIPALITY_KEY)
    if stored is None or current is None:
        return stored == current
    # The scraper stores the geocoder's name for a "?" name: each "?" stands for one character
    pattern = ".".join(re.escape(part) for part in current.split("?"))
    return re.fullmatch(pattern, stored) is not None


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
    """Apply the CSV values in ``target`` to a document's ``data``.

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


def put_item(document, data, description=None):
    """One bulk PUT entry: the document's writable fields with ``data`` (and ``description``) replaced."""
    item = {"id": document.id}
    item |= {field: getattr(document, field) for field in PUT_FIELDS if hasattr(document, field)}
    item["data"] = data
    if description is not None:
        item["description"] = description
    return item


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
