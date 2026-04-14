"""
Join val-19zl.csv (column B) with all.csv by 1-based row index from the pano
filename (e.g. 0001227.jpg -> row 1227 of all.csv).

Paths are resolved from this repo layout:
  crossview/
    script/val.py   (this file)
    data/CVPR_subset/CVPR_subset/...

Output: crossview/results/val_8884.csv with id (filename only), then lat, lon,
cvusa_lat, cvusa_lon, heading from all.csv.

Open Cursor/VS Code on C:\\Users\\2715439W\\Projects to work across libraries/ and crossview/.
"""
from __future__ import annotations

import csv
import re
from pathlib import Path

# crossview/script -> crossview/
_CROSSVIEW_ROOT = Path(__file__).resolve().parent.parent
_DATA_ROOT = _CROSSVIEW_ROOT / "data" / "CVPR_subset" / "CVPR_subset"

VAL_PATH = _DATA_ROOT / "splits" / "splits" / "val-19zl.csv"
ALL_PATH = _DATA_ROOT / "all.csv"
OUT_PATH = _CROSSVIEW_ROOT / "results" / "val_8884.csv"

OUT_HEADER = ("id", "query_lat", "query_lon", "cvusa_lat", "cvusa_lon", "heading")

PANO_STEM = re.compile(r"(\d+)\.jpg$", re.IGNORECASE)


def pano_filename_and_row_index(cell: str) -> tuple[str, int]:
    """Return basename like 0001227.jpg and 1-based row index for all.csv."""
    name = Path(cell.strip().replace("\\", "/")).name
    m = PANO_STEM.search(name)
    if not m:
        raise ValueError(f"No pano id in column B: {cell!r}")
    return name, int(m.group(1), 10)

def main() -> None:
    with ALL_PATH.open(newline="", encoding="utf-8") as f:
        all_rows = list(csv.reader(f))
    n_all = len(all_rows)
    if n_all == 0:
        raise SystemExit("all.csv is empty")

    missing: list[tuple[int, int, str]] = []
    out_rows: list[list[str]] = []

    with VAL_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader, start=1):
            if len(row) < 2:
                raise ValueError(f"val-19zl line {i}: need at least 2 columns, got {row!r}")
            col_b = row[1]
            pano_id_file, idx = pano_filename_and_row_index(col_b)
            if idx < 1 or idx > n_all:
                missing.append((i, idx, col_b))
                continue
            all_line = all_rows[idx - 1]
            if len(all_line) != 5:
                raise ValueError(
                    f"all.csv line {idx}: expected 5 columns, got {len(all_line)}: {all_line!r}"
                )
            out_rows.append([pano_id_file, *all_line])

    if missing:
        print("WARNING: row index out of range for all.csv:")
        for line_no, idx, col_b in missing[:20]:
            print(f"  val line {line_no}: index {idx} from {col_b!r} (all.csv has {n_all} lines)")
        if len(missing) > 20:
            print(f"  ... and {len(missing) - 20} more")
        raise SystemExit(1)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(OUT_HEADER)
        w.writerows(out_rows)

    print(f"Wrote {len(out_rows)} rows to {OUT_PATH}")
    print(f"all.csv lines: {n_all}")


if __name__ == "__main__":
    main()
