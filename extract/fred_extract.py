"""Extract FRED macro series to raw JSON: one file per series, the API response unchanged.

Usage: python extract/fred_extract.py [--series DGS10 UNRATE] [--run-date YYYY-MM-DD]
"""

import argparse
import json
import os
import shutil
import sys
import time
from datetime import date
from pathlib import Path

import requests
from dotenv import load_dotenv

OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"
SERIES = ["DGS10", "DFF", "VIXCLS", "CPIAUCSL", "UNRATE"]
MAX_RETRIES = 5


def fetch_series(session: requests.Session, series_id: str, api_key: str) -> dict:
    """GET all observations for one FRED series. Retries on 429/5xx."""
    params = {"series_id": series_id, "api_key": api_key, "file_type": "json"}

    for attempt in range(MAX_RETRIES):
        response = session.get(OBSERVATIONS_URL, params=params, timeout=30)

        if response.status_code == 429 or response.status_code >= 500:
            wait = 2**attempt
            print(
                f"WARNING: {series_id} got {response.status_code}, retrying in {wait}s "
                f"(attempt {attempt + 1}/{MAX_RETRIES})"
            )
            time.sleep(wait)
            continue

        # Not raise_for_status(): its message contains the full URL, including api_key.
        if not response.ok:
            raise RuntimeError(
                f"{series_id}: HTTP {response.status_code}: {response.text[:200]}"
            )
        data = response.json()

        # pagination guard: did we get every row?
        if data["count"] > len(data["observations"]):
            raise RuntimeError(
                f"{series_id}: got {len(data['observations'])} of {data['count']} rows, need pagination"
            )
        return data

    raise RuntimeError(f"Gave up after {MAX_RETRIES} attempts: {series_id}")


def extract_run(
    session: requests.Session, series_ids: list[str], api_key: str, run_dir: Path
) -> None:
    """Fetch each series and write it to <run_dir>/<SERIES>.json.

    Writes into <run_dir>.tmp/ first, then renames it to <run_dir> (no partial runs visible).
    """
    tmp_dir = run_dir.with_name(run_dir.name + ".tmp")
    shutil.rmtree(tmp_dir, ignore_errors=True)  # leftover from a crashed run
    tmp_dir.mkdir(parents=True)

    for series_id in series_ids:
        data = fetch_series(session, series_id, api_key)
        with open(tmp_dir / f"{series_id}.json", "w", encoding="utf-8") as f:
            json.dump(data, f)

        observations = data["observations"]
        first = observations[0]["date"] if observations else "-"
        last = observations[-1]["date"] if observations else "-"
        print(f"{series_id}: {len(observations)} rows, {first} .. {last}")

    shutil.rmtree(run_dir, ignore_errors=True)  # old copy of this run date
    tmp_dir.rename(run_dir)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract FRED macro series to raw JSON."
    )
    parser.add_argument(
        "--series", nargs="+", default=SERIES, help="FRED series ids (default: all)"
    )
    parser.add_argument(
        "--run-date", default=date.today().isoformat(), help="folder name, YYYY-MM-DD"
    )
    parser.add_argument("--out", default="data/raw/fred")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_dotenv()
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        sys.exit("FRED_API_KEY is not set. Copy .env.example to .env and fill it in.")

    run_dir = Path(args.out) / args.run_date
    with requests.Session() as session:
        extract_run(session, args.series, api_key, run_dir)

    print(f"Done: {len(args.series)} series -> {run_dir}")


if __name__ == "__main__":
    main()
