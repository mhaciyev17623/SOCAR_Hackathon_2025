#!/usr/bin/env bash
set -euo pipefail

DATA_DIR=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-dir) DATA_DIR="$2"; shift 2;;
    *) echo "Unknown arg: $1"; exit 2;;
  esac
done

if [[ -z "$DATA_DIR" ]]; then
  echo "Usage: $0 --data-dir <path>"
  exit 2
fi

OUT_DIR="processed_data"
mkdir -p "$OUT_DIR"

# python3 - <<'PY' "$DATA_DIR" "$OUT_DIR"

python - <<'PY' "$DATA_DIR" "$OUT_DIR"

import sys
from pathlib import Path

# Make our local library importable
sys.path.insert(0, str(Path.cwd() / "src"))

from cpetro_sgx.reader import write_parquet_from_sgx

data_dir = Path(sys.argv[1])
out_dir = Path(sys.argv[2])

sgx_files = sorted(data_dir.rglob("*.sgx"))
if not sgx_files:
    print(f"No .sgx files found under: {data_dir}")
    raise SystemExit(0)

report_lines = []
for f in sgx_files:
    out_path = out_dir / (f.stem + ".parquet")
    try:
        info = write_parquet_from_sgx(f, out_path)
        line = f"[OK] {info['file']} -> {out_path.name} | survey_type_id={info['survey_type_id']} | rows={info['rows_written']} (header_trace_count={info['trace_count_header']})"
        print(line)
        report_lines.append(line)
    except Exception as e:
        line = f"[FAIL] {f.name}: {e}"
        print(line)
        report_lines.append(line)

(out_dir / "sgx_load_report.txt").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
print(f"\nSaved report to: {out_dir/'sgx_load_report.txt'}")
PY
