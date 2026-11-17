import argparse
import gzip
import json
import os
import shutil
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
REQUEST_PAUSE = 0.12  # seconds between requests -> ~8 req/s, under SEC's 10/s
MAX_RETRIES = 5


def load_ciks(path) -> list[str]:
    """Return the sorted, unique 10-digit CIKs from the S&P 500 ticker CSV."""
    df = pd.read_csv(path, dtype=str)
    return sorted(set(df["CIK"]))


def make_session(user_agent: str) -> requests.Session:
    """Session that sends the SEC-required User-Agent on every request."""

    session = requests.Session()
    session.headers["User-Agent"] = user_agent
    return session


def fetch_json(session: requests.Session, url: str) -> dict | None:
    """GET a URL politely. Returns the parsed JSON, or None if SEC says 404."""
    for attempt in range(MAX_RETRIES):
        time.sleep(REQUEST_PAUSE)  # throttle: we limit ourselves
        response = session.get(url, timeout=30)

        # if status is 404 -> return None (company has no data; not an error)
        if response.status_code == 404:
            return None

        # if status is 429 or >= 500 -> print a warning, sleep 2 ** attempt, `continue`
        if response.status_code == 429 or response.status_code >= 500:
            wait = 2**attempt
            print(
                f"WARNING: got {response.status_code}, retrying in {wait}s "
                f"(attempt {attempt + 1}/{MAX_RETRIES})"
            )
            time.sleep(wait)
            continue

        # response.raise_for_status(), then return response.json()
        response.raise_for_status()
        return response.json()

    raise RuntimeError(f"Gave up after {MAX_RETRIES} attempts: {url}")


def extract_run(session: requests.Session, ciks: list[str], run_dir: Path) -> list[str]:
    """Fetch submissions + companyfacts for each CIK and stream them to JSONL files.

    Writes into <run_dir>.tmp/ first, then renames it to <run_dir> (no partial runs visible).
    Returns the list of CIKs that SEC answered with 404 (no data).
    """
    tmp_dir = run_dir.with_name(run_dir.name + ".tmp")
    shutil.rmtree(tmp_dir, ignore_errors=True)  # leftover from a crashed run
    tmp_dir.mkdir(parents=True)

    missing = []
    with (
        gzip.open(
            tmp_dir / "submissions.jsonl.gz", "wt", encoding="utf-8"
        ) as subs_file,
        gzip.open(
            tmp_dir / "companyfacts.jsonl.gz", "wt", encoding="utf-8"
        ) as facts_file,
    ):
        for i, cik in enumerate(ciks, start=1):
            # fetch the submissions JSON for this cik (use SUBMISSIONS_URL.format(cik=cik))
            submissions = fetch_json(session, SUBMISSIONS_URL.format(cik=cik))

            # if it's not None -> write json.dumps(it) + "\n" to subs_file
            # else -> append f"submissions:{cik}" to missing
            if submissions is not None:
                subs_file.write(json.dumps(submissions) + "\n")
            else:
                missing.append(f"submissions:{cik}")

            # same two steps for COMPANYFACTS_URL -> facts_file
            companyfacts = fetch_json(session, COMPANYFACTS_URL.format(cik=cik))

            # if it's not None -> write json.dumps(it) + "\n" to subs_file
            # else -> append f"submissions:{cik}" to missing
            if companyfacts is not None:
                facts_file.write(json.dumps(companyfacts) + "\n")
            else:
                # (missing entry: f"companyfacts:{cik}")
                missing.append(f"companyfacts:{cik}")

            if i % 50 == 0:
                print(f"{i}/{len(ciks)} companies done")

    shutil.rmtree(run_dir, ignore_errors=True)  # old copy of this run date
    tmp_dir.rename(run_dir)
    return missing


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract SEC EDGAR company info and facts to raw JSONL."
    )
    parser.add_argument(
        "--ciks", nargs="+", help="10-digit CIKs to fetch (default: all S&P 500)"
    )
    parser.add_argument(
        "--run-date", default=date.today().isoformat(), help="folder name, YYYY-MM-DD"
    )
    parser.add_argument("--tickers-csv", default="data/reference/sp500_tickers.csv")
    parser.add_argument("--out", default="data/raw/sec_edgar")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_dotenv()
    user_agent = os.environ.get("SEC_USER_AGENT")
    if not user_agent:
        sys.exit(
            'SEC_USER_AGENT is not set. Add this line to .env: SEC_USER_AGENT="stock-market-elt-pipeline your@email.com"'
        )
    session = make_session(user_agent)

    ciks = args.ciks if args.ciks else load_ciks(args.tickers_csv)
    run_dir = Path(args.out) / args.run_date
    missing = extract_run(session, ciks, run_dir)

    print(f"Done: {len(ciks)} CIKs -> {run_dir}")
    print(f"Missing ({len(missing)}): {missing}")


if __name__ == "__main__":
    main()
