# ingest/tests/test_bis_pdf_parse.py
import json, pathlib
from ingest.parsers.pdf import parse_pdf

mf = json.load(open("data/manifest/bis_fr_api.json"))
RAW = pathlib.Path("data/raw/pdf") / (mf[0]["sha256"] + ".pdf")



def test_bis_pdf_contains_keyword():
    txt = parse_pdf(RAW)
    assert "Federal Register" in txt        # ← changed keyword


