import logging
import os
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

# Géorisques always cuts a CSV page of ~1.4 MB mid-download (6000 rows, 2026-11-20)
PAGE_SIZE = 2000
CSV_ENDPOINT = "https://georisques.gouv.fr/api/v1/csv/installations_classees?page_size={page_size}&page={page}"
CSV_HEADERS = {"User-Agent": "Disclose Georisques Metadata Updater Add-On - contact tech@disclose.ngo"}
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

# Boolean theme columns aggregated into the installation_topics list.
THEME_COLUMNS = ["bovins", "porcs", "volailles", "carriere", "eolienne", "industrie"]

# Human-readable fields repaired for mixed Latin-1/UTF-8 mojibake + HTML entities
# (the scraper's CleanTextPipeline). ftfy.fix_text is a no-op on clean text.
FTFY_KEYS = {"installation_name", "installation_address", "installation_municipality"}


def read_installations_csv(csv_path):
    """Read InstallationClassee.csv exactly as the scraper does (indexed by codeAiot)."""
    df = pd.read_csv(csv_path, sep=";", encoding="ISO-8859-1", dtype=str)
    df.fillna("", inplace=True)
    df.set_index("codeAiot", inplace=True)
    return df


def build_rubriques_by_aiot(csv_path):
    """Aggregate rubriqueIC.csv into {codeAiot: sorted unique numeroRubrique}.

    rubriqueIC.csv has several rows per codeAiot (one per alinéa); the scraper
    keeps the set of unique numeroRubrique, sorted.
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
    """Installation-level DocumentCloud ``data`` subset for one codeAiot.

    Returns ``{}`` when the codeAiot is absent from the installations table.
    Mirrors the scraper (add_installation_metadata/add_installation_adress +
    CleanTextPipeline ftfy repair + UploadPipeline item->data-key mapping). Only
    truthy values are emitted, matching the scraper (which never sets empty keys).
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
    """GET one Géorisques CSV page; bounded timeout + retry on transient errors."""
    response = requests.get(url, headers=CSV_HEADERS, timeout=(60, 300))  # (connect, read)
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


# Every key build_installation_data() can emit: one the CSV no longer provides is deleted
MANAGED_KEYS = set(INSTALLATION_FIELD_MAP.values()) | {
    "installation_topics",
    "installation_nomenclature_sections",
    "installation_address",
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


def put_item(document, data):
    """One bulk PUT entry: the document's writable fields with ``data`` replaced."""
    item = {"id": document.id}
    item |= {field: getattr(document, field) for field in PUT_FIELDS if hasattr(document, field)}
    item["data"] = data
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
