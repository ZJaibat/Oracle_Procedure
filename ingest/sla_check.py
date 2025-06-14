"""
Quick-n-dirty health monitor.

It reads `registry.yaml`, opens the corresponding manifest JSON for each
source, grabs the newest timestamp, and fails (exit 1) if the lag exceeds
`sla_hours`.

Run it manually, from cron, or via a Dagster sensor.
"""

import json, pathlib, datetime, sys, yaml

REGISTRY = yaml.safe_load(open("registry.yaml"))["sources"]
MANIFEST_DIR = pathlib.Path("data/manifest")

utc_now = datetime.datetime.utcnow()

for src in REGISTRY:
    manifest_path = MANIFEST_DIR / f"{src['id']}.json"
    if not manifest_path.exists():
        sys.exit(f"{src['id']} has never ingested — SLA breach")

    rows = json.loads(manifest_path.read_text())
    last_ts = max(rows, key=lambda r: r["timestamp"])["timestamp"]
    lag = utc_now - datetime.datetime.fromisoformat(last_ts)

    if lag > datetime.timedelta(hours=src["sla_hours"]):
        sys.exit(f"{src['id']} lag {lag} > {src['sla_hours']} h — SLA breach")

print("All sources healthy ✔︎")

