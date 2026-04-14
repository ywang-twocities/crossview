import os
import csv
import time
import random
from pathlib import Path

from streetview import (
    get_panorama,
    crop_bottom_and_right_black_border,
    crop_horizontal_overlap,
    to_cvusa_format,
    normalize_panorama_heading_to_north,
)

CSV_PATH = r"C:\Users\2715439W\Projects\crossview\results\histo_val.csv"
BASE_OUTPUT_DIR = r"C:\Users\2715439W\Projects\crossview\data"

START_ROW = 1
END_ROW = None # None is for no limit, set to a small number for testing

ZOOM = 1
SLEEP_MIN = 2
SLEEP_MAX = 3

FAILED_LOG_PATH = BASE_OUTPUT_DIR / "failed_log.txt"


def ensure_dirs(base_dir: Path) -> dict:
    out_dirs = {
        "raw": base_dir / "val_raw",
        "clean": base_dir / "val_clean",
        "north": base_dir / "val_north",
        "north_cvusa": base_dir / "val_north_cvusa",
    }
    for d in out_dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return out_dirs


def safe_str(value: str) -> str:
    if value is None:
        return "unknown"
    return str(value).strip().replace("/", "-").replace("\\", "-").replace(":", "-")


def build_output_paths(row: dict, out_dirs: dict) -> dict:
    image_id = row["id"].strip()
    date_str = safe_str(row["date"].strip())
    stem = Path(image_id).stem

    return {
        "raw": out_dirs["raw"] / f"{stem}_raw_{date_str}.jpg",
        "clean": out_dirs["clean"] / f"{stem}_clean_{date_str}.jpg",
        "north": out_dirs["north"] / f"{stem}_north_{date_str}.jpg",
        "north_cvusa": out_dirs["north_cvusa"] / f"{stem}_north_cvusa_{date_str}.jpg",
    }


def is_fully_done(paths: dict) -> bool:
    return all(path.exists() for path in paths.values())


def remove_partial_outputs(paths: dict) -> None:
    for path in paths.values():
        if path.exists():
            path.unlink()


def log_failure(row_index: int, row: dict, error: Exception) -> None:
    FAILED_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with FAILED_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(
            f"{row_index}\t"
            f"{row.get('id', '')}\t"
            f"{row.get('pano_id', '')}\t"
            f"{row.get('date', '')}\t"
            f"{repr(error)}\n"
        )


def get_panorama_with_retry(pano_id: str, zoom: int = ZOOM, retries: int = 3):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return get_panorama(pano_id, zoom=zoom)
        except Exception as e:
            last_error = e
            wait_time = random.uniform(2, 5) * attempt
            print(f"get_panorama failed for {pano_id}, attempt {attempt}/{retries}, error={repr(e)}")
            print(f"Waiting {wait_time:.2f}s before retry...")
            time.sleep(wait_time)
    raise last_error


def process_one_row(row: dict, out_dirs: dict, row_index: int) -> str:
    image_id = row["id"].strip()
    pano_id = row["pano_id"].strip()
    heading = float(row["heading"].strip())
    date_str = safe_str(row["date"].strip())

    paths = build_output_paths(row, out_dirs)

    if is_fully_done(paths):
        print(f"[{row_index}] Skip fully done: {image_id} | pano_id={pano_id} | date={date_str}")
        return "skipped"

    existing_count = sum(path.exists() for path in paths.values())
    if existing_count > 0:
        print(f"[{row_index}] Partial outputs found ({existing_count}/4), regenerating: {image_id}")
        remove_partial_outputs(paths)

    print(f"[{row_index}] Processing: {image_id} | pano_id={pano_id} | date={date_str}")

    img_raw = get_panorama_with_retry(pano_id, zoom=ZOOM, retries=3)
    img_raw.save(paths["raw"], "JPEG")

    img = crop_bottom_and_right_black_border(img_raw)
    result = crop_horizontal_overlap(img, side="both")
    img_clean = result["image"]
    img_clean.save(paths["clean"], "JPEG")

    img_north = normalize_panorama_heading_to_north(img_clean, heading)
    img_north.save(paths["north"], "JPEG")

    img_north_cvusa = to_cvusa_format(img_north)
    img_north_cvusa.save(paths["north_cvusa"], "JPEG")

    print(f"[{row_index}] Saved all 4 outputs for {image_id}")
    return "processed"


def main() -> None:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV file not found: {CSV_PATH}")

    out_dirs = ensure_dirs(BASE_OUTPUT_DIR)

    processed = 0
    skipped = 0
    failed = 0

    with CSV_PATH.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        for row_index, row in enumerate(reader, start=1):
            if row_index < START_ROW:
                continue
            if END_ROW is not None and row_index > END_ROW:
                break

            try:
                status = process_one_row(row, out_dirs, row_index)
                if status == "processed":
                    processed += 1
                elif status == "skipped":
                    skipped += 1
            except Exception as e:
                failed += 1
                print(f"[{row_index}] Failed: {row.get('id', 'unknown')} | error={e}")
                log_failure(row_index, row, e)

            sleep_seconds = random.uniform(SLEEP_MIN, SLEEP_MAX)
            print(f"[{row_index}] Sleeping for {sleep_seconds:.2f} seconds...")
            time.sleep(sleep_seconds)

    print("\nDone.")
    print(f"Processed: {processed}")
    print(f"Skipped:   {skipped}")
    print(f"Failed:    {failed}")
    print(f"Failure log: {FAILED_LOG_PATH}")


if __name__ == "__main__":
    main()