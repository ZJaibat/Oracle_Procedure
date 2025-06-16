python - <<'PY'
import sys, pathlib
print("cwd:", pathlib.Path().resolve())
try:
    import ingest
    print("✅  import ingest OK at runtime")
except ModuleNotFoundError as e:
    print("❌  import ingest failed:", e)
PY

