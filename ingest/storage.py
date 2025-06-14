# ingest/storage.py
from pathlib import Path
import hashlib, json, datetime, os, tempfile, shutil                # ⬅ add tempfile & shutil

from ingest.bronze import raw_to_bronze

RAW        = Path("data/raw")
BRONZE_DIR = Path("data/bronze")
MANIFEST   = Path("data/manifest")

for p in (RAW, BRONZE_DIR, MANIFEST):
    p.mkdir(parents=True, exist_ok=True)

# ✨  ADD THIS HELPER —–––––––––––––––––––––––––––––––––––––––––
def _append_manifest(source_id: str, row: dict) -> None:
    """
    Atomically add a lineage row to data/manifest/<source>.json.
    Idempotent: called every time save_raw runs, but content is
    SHA-deduped so repeats are harmless.
    """
    mf = MANIFEST / f"{source_id}.json"
    rows = json.loads(mf.read_text()) if mf.exists() else []
    rows.append(row)

    with tempfile.NamedTemporaryFile("w", delete=False,
                                     suffix=".json",
                                     dir=mf.parent) as tmp:
        json.dump(rows, tmp, indent=2)
    shutil.move(tmp.name, mf)
# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

def save_raw(content: bytes, content_type: str, source_id: str, url: str) -> str:
    digest = hashlib.sha256(content).hexdigest()
    raw_path = RAW / content_type / f"{digest}.{content_type}"
    raw_path.parent.mkdir(parents=True, exist_ok=True)

    if not raw_path.exists():           # idempotent
        raw_path.write_bytes(content)

    bronze_path = raw_to_bronze(raw_path)

    _append_manifest(source_id, {
        "sha256":   digest,
        "url":      url,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "ctype":    content_type,
        "bronze_path": str(bronze_path)
    })
    return str(raw_path)

