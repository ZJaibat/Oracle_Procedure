# ingest/parsers/xml.py
from pathlib import Path
from bs4 import BeautifulSoup, NavigableString
import re

_WHITESPACE = re.compile(r"\s+")

def _parse_xml_bytes(raw: bytes) -> str:
    """Existing logic: bytes → cleaned text."""
    doc = BeautifulSoup(raw, "lxml-xml")
    lines = [
        t.strip()
        for t in doc.find_all(string=True)
        if isinstance(t, NavigableString) and t.strip()
    ]
    # collapse runs of whitespace so tests like
    # “Regulation FD Disclosure” don’t break
    return _WHITESPACE.sub(" ", "\n".join(lines)).strip()

def parse_xml(path: Path) -> str:
    """Path → cleaned text (public API, aligns with pdf/html parsers)."""
    return _parse_xml_bytes(path.read_bytes())

