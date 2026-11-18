"""Upload one raw extract run to the Databricks Volume, mirroring the local layout.

data/raw/<source>/<run_date>/<file>  ->  /Volumes/workspace/stock_market/raw/<source>/<run_date>/<file>

Usage: python extract/upload_raw.py --source fred [--run-date YYYY-MM-DD]
Needs DATABRICKS_HOST and DATABRICKS_TOKEN in .env (see .env.example).
"""

import argparse
from datetime import date
from pathlib import Path

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

LOCAL_ROOT = Path("data/raw")
VOLUME_ROOT = "/Volumes/workspace/stock_market/raw"
SOURCES = ["yfinance", "sec_edgar", "fred"]


def volume_path(local_file: Path, local_root: Path, volume_root: str) -> str:
    """Map a local raw file to its Volume path.

    data/raw/fred/2026-11-18/DGS10.json -> /Volumes/workspace/stock_market/raw/fred/2026-11-18/DGS10.json
    """
    relative = local_file.relative_to(local_root).as_posix()  # always "/", never "\"
    return f"{volume_root}/{relative}"


def upload_run(
    client: WorkspaceClient,
    source: str,
    run_date: str,
    local_root: Path = LOCAL_ROOT,
    volume_root: str = VOLUME_ROOT,
) -> list[str]:
    """Upload every file in <local_root>/<source>/<run_date>/ to the Volume.

    overwrite=True: re-uploading the same run replaces the files (idempotent).
    Returns the Volume paths that were written.
    """
    run_dir = local_root / source / run_date
    if not run_dir.is_dir():
        raise FileNotFoundError(
            f"No run folder: {run_dir}. Run the {source} extract first."
        )

    files = sorted(p for p in run_dir.iterdir() if p.is_file())
    if not files:
        raise FileNotFoundError(f"Run folder is empty: {run_dir}")

    targets = []
    for local_file in files:
        target = volume_path(local_file, local_root, volume_root)
        with open(local_file, "rb") as f:
            client.files.upload(target, f, overwrite=True)
        size_kb = local_file.stat().st_size / 1024
        print(f"{local_file.name} ({size_kb:,.0f} KB) -> {target}")
        targets.append(target)
    return targets


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload one raw extract run to the Databricks Volume."
    )
    parser.add_argument("--source", required=True, choices=SOURCES)
    parser.add_argument(
        "--run-date", default=date.today().isoformat(), help="folder name, YYYY-MM-DD"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_dotenv()
    client = WorkspaceClient()  # reads DATABRICKS_HOST and DATABRICKS_TOKEN

    targets = upload_run(client, args.source, args.run_date)
    print(f"Done: {len(targets)} files -> {VOLUME_ROOT}/{args.source}/{args.run_date}/")


if __name__ == "__main__":
    main()
