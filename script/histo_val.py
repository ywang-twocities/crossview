"""
Read val_8884.csv, call search_panoramas(query_lat, query_lon) per row, and write
crossview/results/histo_val.csv with dated panoramas only (skip date=None).

Requires Python 3.10+ (same as libraries/streetview).

Default: all rows (--limit N for first N only). Use --limit 0 for all rows (same as default).

After each val row (each query coordinate), sleeps a random duration to pace requests.

Progress is printed per row. Each val row is flushed to the CSV immediately.

Resume: distinct ``id`` values already present in crossview/results/histo_val.csv are
treated as completed; re-run skips them and appends new rows. Use --fresh to delete
the output CSV and start over.
"""
from __future__ import annotations

import argparse
import csv
import importlib.util
import random
import sys
import time
from pathlib import Path

# Seconds; random uniform delay after each val_8884 row is processed.
_ROW_SLEEP_SEC_MIN = 1.0
_ROW_SLEEP_SEC_MAX = 3.0

if sys.version_info < (3, 10):
    raise SystemExit(
        "histo_val.py requires Python 3.10+ (streetview uses PEP 604 union types). "
        f"Current interpreter: {sys.version.split()[0]}"
    )

_CROSSVIEW_ROOT = Path(__file__).resolve().parent.parent
_PROJECTS_ROOT = _CROSSVIEW_ROOT.parent
_STREETVIEW_SEARCH = (
    _PROJECTS_ROOT / "libraries" / "streetview" / "src" / "streetview" / "search.py"
)


def _load_search_panoramas():
    """Load search_panoramas without importing streetview package __init__ (avoids httpx/PIL)."""
    if not _STREETVIEW_SEARCH.is_file():
        raise SystemExit(f"streetview search not found: {_STREETVIEW_SEARCH}")
    spec = importlib.util.spec_from_file_location("streetview_search", _STREETVIEW_SEARCH)
    if spec is None or spec.loader is None:
        raise SystemExit(f"Could not load module spec from {_STREETVIEW_SEARCH}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["streetview_search"] = mod
    spec.loader.exec_module(mod)
    return mod.search_panoramas


VAL_CSV = _CROSSVIEW_ROOT / "results" / "val_8884.csv"
OUT_CSV = _CROSSVIEW_ROOT / "results" / "histo_val.csv"
# Legacy sidecar from older script versions; removed if --fresh.
_LEGACY_DONE_IDS = _CROSSVIEW_ROOT / "results" / "histo_val.done_ids.txt"

OUT_HEADER = (
    "id",
    "pano_id",
    "lat",
    "lon",
    "heading",
    "pitch",
    "roll",
    "date",
    "query_lat",
    "query_lon",
)


def _csv_has_data_rows() -> bool:
    if not OUT_CSV.is_file() or OUT_CSV.stat().st_size == 0:
        return False
    with OUT_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return False
        return any(True for _ in reader)


def load_completed_ids_from_output_csv() -> set[str]:
    """Source image ids already present in histo_val.csv (any row counts as done for that id)."""
    ids: set[str] = set()
    if not OUT_CSV.is_file() or OUT_CSV.stat().st_size == 0:
        return ids
    with OUT_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "id" not in reader.fieldnames:
            return ids
        for row in reader:
            ids.add(row["id"].strip())
    return ids


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max rows from val_8884 to consider from the start (default 0 = all).",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Delete histo_val.csv (and legacy histo_val.done_ids.txt if present) and start from scratch.",
    )
    args = parser.parse_args()

    search_panoramas = _load_search_panoramas()

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    if args.fresh:
        for p in (OUT_CSV, _LEGACY_DONE_IDS):
            if p.is_file():
                p.unlink()
        print("Fresh start: removed previous histo_val.csv (and legacy sidecar if any).")

    done_ids = load_completed_ids_from_output_csv()

    with VAL_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        if not fieldnames or "id" not in fieldnames:
            raise SystemExit(f"{VAL_CSV}: expected CSV with 'id' column, got {fieldnames!r}")
        if "query_lat" not in fieldnames or "query_lon" not in fieldnames:
            raise SystemExit(f"{VAL_CSV}: need query_lat, query_lon columns")
        rows = list(reader)

    if not rows:
        raise SystemExit(f"No data rows in {VAL_CSV}")

    limit = args.limit
    slice_rows = rows if limit == 0 else rows[:limit]
    total_in_slice = len(slice_rows)

    pending = [r for r in slice_rows if r["id"].strip() not in done_ids]
    n_skipped = total_in_slice - len(pending)
    n_pending = len(pending)

    if n_skipped:
        print(f"Resume: skipping {n_skipped} already-completed id(s); {n_pending} row(s) left to run.")

    # Append only if there is at least one data row (header-only must reopen as "w" to avoid duplicates).
    append_mode = _csv_has_data_rows()
    mode = "a" if append_mode else "w"

    total_pano_rows_session = 0
    with OUT_CSV.open(mode, newline="", encoding="utf-8") as f_out:
        w = csv.writer(f_out, lineterminator="\n")
        if mode == "w":
            w.writerow(OUT_HEADER)
            f_out.flush()

        for j, row in enumerate(pending):
            pano_id_file = row["id"].strip()
            q_lat = row["query_lat"].strip()
            q_lon = row["query_lon"].strip()
            lat = float(q_lat)
            lon = float(q_lon)

            panos = search_panoramas(lat=lat, lon=lon)
            batch: list[list[str]] = []
            for p in panos:
                if p.date is None:
                    continue
                batch.append(
                    [
                        pano_id_file,
                        p.pano_id,
                        str(p.lat),
                        str(p.lon),
                        str(p.heading),
                        "" if p.pitch is None else str(p.pitch),
                        "" if p.roll is None else str(p.roll),
                        p.date,
                        q_lat,
                        q_lon,
                    ]
                )

            w.writerows(batch)
            f_out.flush()

            total_pano_rows_session += len(batch)
            overall_done = n_skipped + j + 1
            pct = 100.0 * overall_done / total_in_slice if total_in_slice else 0.0
            print(
                f"[{j + 1}/{n_pending}] overall {overall_done}/{total_in_slice} ({pct:.1f}%) "
                f"id={pano_id_file} dated_panos={len(batch)} (session total lines={total_pano_rows_session})",
                flush=True,
            )

            if j < n_pending - 1:
                time.sleep(random.uniform(_ROW_SLEEP_SEC_MIN, _ROW_SLEEP_SEC_MAX))

    print(
        f"Done. This run wrote {total_pano_rows_session} dated panorama row(s) across {n_pending} val row(s). "
        f"Output: {OUT_CSV}"
    )


if __name__ == "__main__":
    main()
