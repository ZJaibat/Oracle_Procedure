# ingest/parsers/xml.py
from pathlib import Path
from bs4 import BeautifulSoup, NavigableString
import re
from typing import Union, AnyStr

_WHITESPACE = re.compile(r"\s+")

def _clean_xml_bytes(raw: bytes) -> str:
    """
    Core cleaner: bytes → UTF-8 text.
    """
    soup = BeautifulSoup(raw, "lxml-xml")
    lines = [
        t.strip()
        for t in soup.find_all(string=True)
        if isinstance(t, NavigableString) and t.strip()
    ]
    return _WHITESPACE.sub(" ", "\n".join(lines)).strip()


def parse_xml(obj: Union[Path, str, AnyStr, bytes, bytearray]) -> str:
    """
    Flexible public API used by both production code **and** tests.

    • Production passes a Path
    • The golden-path test passes raw bytes (RAW.read_bytes())
    """
    if isinstance(obj, (bytes, bytearray)):
        raw = obj
    else:
        raw = Path(obj).read_bytes()

    return _clean_xml_bytes(raw)

