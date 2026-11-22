"""Download one raw extract run from the Databricks Volume, mirroring upload_raw.py.

/Volumes/workspace/stock_market/raw/<source>/<run_date>/<file>  ->  <dest_root>/<source>/<run_date>/<file>

Called by the bronze load, which downloads into a temporary folder that is deleted
after loading, so no raw data stays on the PC.
Needs DATABRICKS_HOST and DATABRICKS_TOKEN in .env (see .env.example).
"""

from pathlib import Path
import shutil

from databricks.sdk import WorkspaceClient

from extract.upload_raw import VOLUME_ROOT


def download_run(
    client: WorkspaceClient,
    source: str,
    run_date: str,
    dest_root: Path,
    volume_root: str = VOLUME_ROOT,
) -> list[Path]:
    """Download every file in <volume_root>/<source>/<run_date>/ to <dest_root>/<source>/<run_date>/.

    dest_root has no default on purpose: the caller (the bronze load) passes a temporary
    folder, so raw data is never written to data/raw/ on the PC by accident.
    Returns the local paths of the downloaded files.
    """
    run_dir = dest_root/source/run_date

    volume_folder_path = f"{volume_root}/{source}/{run_date}"
    entries = client.files.list_directory_contents(volume_folder_path)
    files = sorted(
        [entry for entry in entries if not entry.is_directory], key=lambda e: e.name)
    if not files:
        raise FileNotFoundError(f"Run folder is empty: {volume_folder_path}")

    run_dir.mkdir(parents=True, exist_ok=True)

    local_files = []

    for entry in files:
        local_target = run_dir/entry.name
        response = client.files.download(entry.path)
        with open(local_target, "wb") as file:
            shutil.copyfileobj(response.contents, file)
        size_kb = local_target.stat().st_size / 1024
        print(f"{entry.path} ({size_kb:,.0f} KB) -> {local_target}")
        local_files.append(local_target)

    return local_files
