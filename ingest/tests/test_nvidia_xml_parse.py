# ingest/tests/test_nvidia_xml_parse.py
import json, pathlib
from ingest.parsers.xml import parse_xml

mf = json.load(open("data/manifest/nvidia_8k.json"))
RAW = pathlib.Path("data/raw/xml") / (mf[0]["sha256"] + ".xml")

def test_nvidia_xml_has_form_header():
    txt = parse_xml(RAW.read_bytes())
    assert "FORM 8-K" in txt  # appears in every primary doc

