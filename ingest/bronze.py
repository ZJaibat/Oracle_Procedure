# ingest/bronze.py
from pathlib import Path
from ingest.parsers import pdf, html, xml

EXT2FUNC = {
    ".pdf": pdf.parse_pdf,        # pass Path
    ".html": html.parse_html,     # pass Path
    ".htm":  html.parse_html,
    ".xml":  xml.parse_xml,
}

BRONZE_ROOT = Path("data/bronze")

def raw_to_bronze(raw_path: Path) -> Path:
    func = EXT2FUNC.get(raw_path.suffix.lower())
    if not func:
        raise ValueError(f"No parser for {raw_path.suffix}")

    bronze_dir = BRONZE_ROOT / raw_path.parent.name
    bronze_dir.mkdir(parents=True, exist_ok=True)

    bronze_path = bronze_dir / (raw_path.stem + ".txt")
    if bronze_path.exists():          # idempotent
        return bronze_path

    text = func(raw_path)             # <- Path goes in
    bronze_path.write_text(text, encoding="utf-8")
    return bronze_path

