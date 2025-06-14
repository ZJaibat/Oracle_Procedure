ls -lh data/bronze/pdf | head
ls -lh data/bronze/xml | head
ls -lh data/bronze/html | head
python ingest/sla_check.py     # expect “All sources healthy ✔︎”

