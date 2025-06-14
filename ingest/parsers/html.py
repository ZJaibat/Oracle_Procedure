# ingest/parsers/html.py
from bs4 import BeautifulSoup
import re

def parse_html(raw: bytes) -> str:
    soup = BeautifulSoup(raw, "lxml")
    for tag in soup.select("script, style, nav, header, footer"):
        tag.decompose()
    text = soup.get_text("\n")
    return re.sub(r"\n{3,}", "\n\n", text).strip()

