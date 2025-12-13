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
REPORT="$OUT_DIR/corrupted_parquet_report.txt"
: > "$REPORT"

python3 - <<'PY' "$DATA_DIR" "$OUT_DIR" "$REPORT"
import sys, io
from pathlib import Path

data_dir = Path(sys.argv[1])
out_dir = Path(sys.argv[2])
report = Path(sys.argv[3])

def try_read_pyarrow(path: Path):
    import pyarrow.parquet as pq
    return pq.read_table(path)

def write_clean_parquet(table, out_path: Path):
    import pyarrow.parquet as pq
    pq.write_table(table, out_path)

def repair_truncate_to_last_par1(raw: bytes):
    # Parquet magic is b'PAR1' at start and end.
    # Many corruptions are: extra bytes appended or missing end.
    if raw[:4] != b"PAR1":
        return None, "missing header magic PAR1 (not parquet?)"

    # Find last occurrence of PAR1 (likely real end marker)
    last = raw.rfind(b"PAR1")
    if last == -1:
        return None, "no PAR1 found at all"
    if last < 8:
        return None, "PAR1 found too early"

    repaired = raw[: last + 4]  # keep up to end marker
    # Ensure there are at least 8 bytes before end for [footer_len][PAR1]
    if len(repaired) < 12:
        return None, "too small after truncation"
    return repaired, f"truncated to last PAR1 at offset {last}"

parquets = sorted(list(data_dir.rglob("*.parquet")))

lines = []
for p in parquets:
    out_path = out_dir / p.name
    try:
        t = try_read_pyarrow(p)
        write_clean_parquet(t, out_path)
        msg = f"[OK] {p.name} readable -> wrote clean copy to {out_path}"
        print(msg)
        lines.append(msg)
        continue
    except Exception as e1:
        msg = f"[FAIL] {p.name} pyarrow read failed: {e1}"
        print(msg)
        lines.append(msg)

    # Try byte-level repair
    raw = p.read_bytes()
    repaired, why = repair_truncate_to_last_par1(raw)
    if repaired is None:
        msg = f"[SKIP] {p.name} cannot repair by truncation: {why}"
        print(msg)
        lines.append(msg)
        continue

    # Try reading repaired bytes
    tmp = out_dir / (p.stem + ".__repaired_tmp.parquet")
    tmp.write_bytes(repaired)

    try:
        t = try_read_pyarrow(tmp)
        write_clean_parquet(t, out_path)
        tmp.unlink(missing_ok=True)
        msg = f"[REPAIRED] {p.name} -> {out_path} ({why})"
        print(msg)
        lines.append(msg)
    except Exception as e2:
        msg = f"[STILL_FAIL] {p.name} after repair ({why}): {e2}"
        print(msg)
        lines.append(msg)
        tmp.unlink(missing_ok=True)

report.write_text("\n".join(lines) + "\n", encoding="utf-8")
print(f"\nReport saved to: {report}")
PY
