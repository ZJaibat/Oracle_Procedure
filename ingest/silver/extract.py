# ingest/silver/extract.py
"""
Convert a Bronze .txt into a structured Silver JSON.
Lightweight: works with a blank spaCy pipeline (no model download).
"""

from __future__ import annotations
from pathlib import Path
import json, re, hashlib
import spacy
import pandas as pd, yaml

# ---------- paths ------------------------------------------------------
REGISTRY_CSV   = Path("data/registry/actors.csv")
PATTERN_YAML   = Path("config/patterns.yml")
SILVER_DIR     = Path("data/silver")
SILVER_VERSION = "1.0"

# ---------- 1. blank pipeline + sentencizer ----------------------------
def _build_nlp() -> spacy.language.Language:
    nlp = spacy.blank("en")        # no large model download
    nlp.add_pipe("sentencizer")    # sentence boundaries
    ruler = nlp.add_pipe("entity_ruler")

    df = pd.read_csv(REGISTRY_CSV)     # id,alias,type
    patterns = [{"label": r["id"], "pattern": r["alias"]} for _, r in df.iterrows()]
    ruler.add_patterns(patterns)
    return nlp

_NLP = _build_nlp()

# ---------- 2. load regex rules ----------------------------------------
with open(PATTERN_YAML) as f:
    _PATTERNS = yaml.safe_load(f) or {}

def _match_event(doc_type: str, text: str) -> dict:
    for rule in _PATTERNS.get(doc_type, []):
        if re.search(rule["object_re"], text, re.I):
            return {
                "action":        rule["action"],
                "object":        rule["object_re"],
                "target_region": (
                    re.search(rule.get("target_re", ""), text, re.I).group(0)
                    if rule.get("target_re") else None
                ),
            }
    return {}    # no rule matched

# ---------- 3. main ----------------------------------------------------
def extract_to_silver(bronze_path: Path, meta: dict) -> Path:
    """
    bronze_path : Path to Bronze .txt
    meta        : dict with keys source, doc_type, published_at, raw_path
    """
    sha        = bronze_path.stem
    silver_out = SILVER_DIR / f"{sha}.json"
    silver_out.parent.mkdir(parents=True, exist_ok=True)
    if silver_out.exists():
        return silver_out                         # idempotent

    text = bronze_path.read_text()
    doc  = _NLP(text)

    # -------- actor IDs & unknown spans ---------------------------------
    actor_ids, unknown_ents = [], []
    for ent in doc.ents:
        if ent.label_:                      # matched via EntityRuler
            actor_ids.append(ent.label_)
        else:                               # unseen – preserve it
            tmp = "UNSEEN_" + hashlib.sha1(ent.text.encode()).hexdigest()[:6]
            actor_ids.append(tmp)
            unknown_ents.append({
                "id":    tmp,
                "span":  ent.text,
                "start": ent.start_char,
                "end":   ent.end_char,
                "type":  "unknown",
            })
    actor_ids = sorted(set(actor_ids))

    # -------- event pattern match ---------------------------------------
    doc_type = meta.get("doc_type")
    event    = _match_event(doc_type, text)

    # representative sentence offsets
    sent_span = None
    if event.get("object"):
        for sent in doc.sents:
            if re.search(event["object"], sent.text, re.I):
                sent_span = [sent.start_char, sent.end_char]
                break

    # -------- assemble Silver record ------------------------------------
    silver = {
        "sha256":         sha,
        "silver_version": SILVER_VERSION,
        "published_at":   meta.get("published_at"),
        "source":         meta.get("source"),
        "doc_type":       doc_type,
        "actor_ids":      actor_ids,
        **event,
        "sentence_span":  sent_span,
        "entities":       unknown_ents,
        "bronze_path":    str(bronze_path),
        "raw_path":       meta.get("raw_path"),
    }
    silver_out.write_text(json.dumps(silver, indent=2))
    return silver_out
