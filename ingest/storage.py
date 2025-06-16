"""
Save raw bytes, then create bronze & silver layers, update manifest (idempotent).
"""

from pathlib import Path
import hashlib, json, datetime, tempfile, shutil, os

from ingest.bronze import raw_to_bronze
from ingest.silver.extract import extract_to_silver   # silver step

RAW        = Path("data/raw")
BRONZE_DIR = Path("data/bronze")
MANIFEST   = Path("data/manifest")

for p in (RAW, BRONZE_DIR, MANIFEST):
    p.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------------- #
def _append_manifest(source_id: str, row: dict) -> None:
    """Atomically add one row to data/manifest/<source>.json."""
    mf = MANIFEST / f"{source_id}.json"
    rows = json.loads(mf.read_text()) if mf.exists() else []
    rows.append(row)

    with tempfile.NamedTemporaryFile(
        "w", delete=False, suffix=".json", dir=mf.parent
    ) as tmp:
        json.dump(rows, tmp, indent=2)
    shutil.move(tmp.name, mf)

# --------------------------------------------------------------------------- #
def save_raw(content: bytes, content_type: str,
             source_id: str, url: str) -> str:
    """
    Store immutable raw file, create bronze & silver derivatives, patch manifest.
    Returns absolute path to the raw file.
    """
    sha       = hashlib.sha256(content).hexdigest()
    raw_path  = RAW / content_type / f"{sha}.{content_type}"
    raw_path.parent.mkdir(parents=True, exist_ok=True)

    # ---------------- RAW --------------------------------------------------
    if not raw_path.exists():                         # idempotent
        raw_path.write_bytes(content)

    # ---------------- BRONZE ------------------------------------------------
    bronze_path = raw_to_bronze(raw_path)

    # ---------------- SILVER ------------------------------------------------
    meta = {
        "source":       source_id,
        "doc_type":     f"{source_id.split('_')[0].upper()}_{content_type.upper()}",
        "published_at": datetime.datetime.utcnow().date().isoformat(),
        "raw_path":     str(raw_path),
    }
    silver_path = extract_to_silver(bronze_path, meta)

    # ---------------- MANIFEST ---------------------------------------------
    _append_manifest(source_id, {
        "sha256":       sha,
        "url":          url,
        "timestamp":    datetime.datetime.utcnow().isoformat(),
        "ctype":        content_type,
        "bronze_path":  str(bronze_path),
        "silver_path":  str(silver_path),
    })
    return str(raw_path)
