from __future__ import annotations

import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Tuple, Dict, Any, Optional, List

import pyarrow as pa
import pyarrow.parquet as pq


MAGIC = b"CPETRO01"
HEADER_FMT = "<8sII"     # magic(8), survey_type_id(uint32), trace_count(uint32)  little-endian
RECORD_FMT = "<IffB"     # well_id(uint32), depth(float32), amplitude(float32), quality_flag(uint8)

HEADER_SIZE = struct.calcsize(HEADER_FMT)   # 16
RECORD_SIZE = struct.calcsize(RECORD_FMT)   # 13


@dataclass(frozen=True)
class SGXHeader:
    magic: bytes
    survey_type_id: int
    trace_count: int


def parse_header(b: bytes) -> SGXHeader:
    if len(b) < HEADER_SIZE:
        raise ValueError(f"File too small for SGX header: {len(b)} bytes")
    magic, survey_type_id, trace_count = struct.unpack(HEADER_FMT, b[:HEADER_SIZE])
    return SGXHeader(magic=magic, survey_type_id=survey_type_id, trace_count=trace_count)


def iter_sgx_records(path: str | Path) -> Iterator[Tuple[int, float, float, int]]:
    """
    Yields (well_id, depth_ft, amplitude, quality_flag)
    """
    path = Path(path)
    data = path.read_bytes()
    hdr = parse_header(data)

    if hdr.magic != MAGIC:
        # Still allow inspection, but this likely means wrong file or corrupted header.
        raise ValueError(f"Bad SGX magic: expected {MAGIC!r}, got {hdr.magic!r} in {path.name}")

    offset = HEADER_SIZE
    expected_bytes = HEADER_SIZE + hdr.trace_count * RECORD_SIZE
    if len(data) < expected_bytes:
        # partial file: read only complete records available
        max_records = (len(data) - HEADER_SIZE) // RECORD_SIZE
    else:
        max_records = hdr.trace_count

    for _ in range(max_records):
        chunk = data[offset: offset + RECORD_SIZE]
        if len(chunk) < RECORD_SIZE:
            break
        well_id, depth_ft, amplitude, qflag = struct.unpack(RECORD_FMT, chunk)
        yield well_id, float(depth_ft), float(amplitude), int(qflag)
        offset += RECORD_SIZE


def read_sgx(path: str | Path) -> Tuple[SGXHeader, pa.Table]:
    """
    Reads SGX into (header, pyarrow.Table)
    """
    path = Path(path)
    data = path.read_bytes()
    hdr = parse_header(data)

    rows = []
    for well_id, depth_ft, amplitude, qflag in iter_sgx_records(path):
        rows.append((well_id, depth_ft, amplitude, qflag))

    # Arrow table
    table = pa.table(
        {
            "well_id": [r[0] for r in rows],
            "depth_ft": [r[1] for r in rows],
            "amplitude": [r[2] for r in rows],
            "quality_flag": [r[3] for r in rows],
            "survey_type_id": [hdr.survey_type_id] * len(rows),
            "source_file": [path.name] * len(rows),
        }
    )
    return hdr, table


def write_parquet_from_sgx(path: str | Path, out_parquet: str | Path) -> Dict[str, Any]:
    """
    Writes SGX to Parquet. Returns summary dict.
    """
    hdr, table = read_sgx(path)
    out_parquet = Path(out_parquet)
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out_parquet)

    return {
        "file": Path(path).name,
        "survey_type_id": hdr.survey_type_id,
        "trace_count_header": hdr.trace_count,
        "rows_written": table.num_rows,
        "out": str(out_parquet),
    }
