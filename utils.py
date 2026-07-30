"""Utilities for the Géorisques Metadata Updater add-on.

Holds the framework-agnostic helpers used by ``main.py``:
- the Géorisques CSV parsing/mapping (kept in sync with the scraper),
- small DocumentCloud ``data`` helpers, and
- the 429-aware save-with-backoff wrapper.
"""

from datetime import datetime, timezone
import logging
import os
import re
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

PAGE_SIZE = 6000
CSV_ENDPOINT = "https://georisques.gouv.fr/api/v1/csv/installations_classees?page_size={page_size}&page={page}"
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
    wait=wait_exponential(multiplier=1, max=30),
    stop=stop_after_attempt(3),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _download_csv_page(url):
    """GET one Géorisques CSV page; bounded timeout + retry on transient errors."""
    response = requests.get(url, timeout=(10, 120))  # (connect, read)
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

    df_installations = pd.concat(installations_frames)
    rubriques_by_aiot = {aiot: sorted(nums) for aiot, nums in rubriques_sets.items()}
    return df_installations, rubriques_by_aiot


# ---------------------------------------------------------------------------
# DocumentCloud data helpers
# ---------------------------------------------------------------------------

def first_value(value):
    """DocumentCloud data values come back as lists; returns the first."""
    if isinstance(value, list):
        return value[0] if value else None
    return value


def normalize(value):
    """Order-insensitive comparison form: sorted list of stringified values.

    Handles both single and list-valued data keys.
    """
    if value is None:
        return []
    if isinstance(value, list):
        return sorted(str(item) for item in value)
    return [str(value)]


def utcnow_iso():
    """Fixed-width ISO-8601 UTC so lexical order == chronological order."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# Title beautifying
# File extensions to drop from the end of a title. Whitelisted deliberately
# to avoid removing legitimate title fragments
TITLE_EXTENSIONS = ["pdf", "pdfa", "doc", "docx", "odt", "rtf", "txt"]

# Remove one or more of those extensions at the very end of the title
TITLE_EXTENSIONS_RE = re.compile(
    r"(?:\s*\.(?:" + "|".join(TITLE_EXTENSIONS) + r"))+$", re.IGNORECASE
)


def beautify_title(title):
    """Turn a source filename into a human-readable document title.
    """
    beautified = title.replace("_", " ")

    beautified = re.sub(r"\s+", " ", beautified).strip()

    beautified = TITLE_EXTENSIONS_RE.sub("", beautified).strip()

    return beautified or title


# Rate-limit-aware saving
def is_rate_limited(exc):
    """True for a DocumentCloud 429 (Too Many Requests) response."""
    return isinstance(exc, APIError) and getattr(exc, "status_code", None) == 429


@retry(
    retry=retry_if_exception(is_rate_limited),
    # 3 attempts, exponential backoff ~1s then ~12s (capped at 15s).
    wait=wait_exponential(multiplier=1, exp_base=12, min=1, max=15),
    stop=stop_after_attempt(3),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def save_with_backoff(document):
    """Persist a document, retrying with exponential backoff on 429s."""
    document.save()
