# ingest/parsers/pdf.py
from pathlib import Path
import logging
logging.getLogger("pdfminer").setLevel(logging.WARNING)

import pdfplumber

def parse_pdf(path: Path) -> str:
    with pdfplumber.open(path) as pdf:
        pages = [page.extract_text() or "" for page in pdf.pages]
    return "\n".join(pages).strip()

