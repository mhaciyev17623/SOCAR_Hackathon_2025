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
OUT_FILE="$OUT_DIR/flag_parquet_results.txt"
: > "$OUT_FILE"

python3 - <<'PY' "$DATA_DIR" "$OUT_FILE"
import os, re, sys
from pathlib import Path

data_dir = Path(sys.argv[1])
out_file = Path(sys.argv[2])

# common CTF-like flag patterns; we also capture anything like WORD{...}
patterns = [
    re.compile(r"(FLAG|SOCAR|CTF|HACK|KEY)\{[^}\n\r]{4,200}\}", re.IGNORECASE),
    re.compile(r"[A-Z0-9_]{2,20}\{[^}\n\r]{4,200}\}"),
]

def printable_strings(b: bytes, min_len=6):
    # extract ascii-ish strings from bytes
    out = []
    cur = []
    for x in b:
        if 32 <= x <= 126:
            cur.append(x)
        else:
            if len(cur) >= min_len:
                out.append(bytes(cur).decode("utf-8", errors="ignore"))
            cur = []
    if len(cur) >= min_len:
        out.append(bytes(cur).decode("utf-8", errors="ignore"))
    return out

def scan_file(p: Path):
    b = p.read_bytes()
    n = len(b)

    # Read last chunk (footer region). If footers are “padded”, scanning last 32MB is usually enough.
    tail_len = min(32 * 1024 * 1024, n)
    tail = b[-tail_len:]

    strings = printable_strings(tail, min_len=6)

    hits = []
    for s in strings:
        for pat in patterns:
            for m in pat.finditer(s):
                hits.append(m.group(0))

    # As fallback: regex search directly on tail bytes (sometimes flags are broken by separators)
    tail_text = tail.decode("utf-8", errors="ignore")
    for pat in patterns:
        hits.extend([m.group(0) for m in pat.finditer(tail_text)])

    # Deduplicate while preserving order
    seen = set()
    dedup = []
    for h in hits:
        if h not in seen:
            seen.add(h)
            dedup.append(h)
    return dedup

parquets = sorted(list(data_dir.rglob("*.parquet")))
with out_file.open("a", encoding="utf-8") as f:
    for p in parquets:
        try:
            hits = scan_file(p)
            if hits:
                f.write(f"\n=== {p} ===\n")
                for h in hits:
                    f.write(h + "\n")
                print(f"[FOUND] {p.name}: " + " | ".join(hits))
            else:
                print(f"[OK] {p.name}: no obvious flag pattern")
        except Exception as e:
            print(f"[ERR] {p}: {e}")

print(f"\nSaved results to: {out_file}")
PY
