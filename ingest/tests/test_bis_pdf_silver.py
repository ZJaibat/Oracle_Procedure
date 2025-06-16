from pathlib import Path
import json
from ingest.silver.extract import extract_to_silver

RAW_SHA = "04c7817a69cf65b69fa7d9f13faab6443f916ae8e28b215252ec773b9acebaaf"                     # use real sha
BRONZE = Path(f"data/bronze/pdf/{RAW_SHA}.txt")
META = {
    "source": "bis_fr_api",
    "doc_type": "BIS_PDF",                 # ← match patterns.yml
    "published_at": "1996-05-09",
    "raw_path": "",
}


def test_bis_restrict_action():
    silver = extract_to_silver(BRONZE, META)
    data = json.loads(Path(silver).read_text())
    assert data["action"] == "restrict"
    assert "BIS" in data["actor_ids"]

