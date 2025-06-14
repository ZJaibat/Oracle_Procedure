# ingest/tests/test_asml_html_parse.py
from pathlib import Path
from ingest.parsers.html import parse_html

GOLD = Path("data/raw/html/823dcfa5fc2c285f6786ca89955070cd9fb3033033e9a736c6a31fb6834eebbe.html")
def test_asml_html_contains_brand():
    txt = parse_html(GOLD.read_bytes())
    assert "ASML" in txt

