"""Unit tests for extract/upload_raw.py. No network calls: a FakeClient records
what would have been uploaded instead of talking to Databricks."""

from pathlib import Path

import pytest

from extract import upload_raw as up

VOLUME = "/Volumes/workspace/stock_market/raw"


class FakeFiles:
    """Stands in for WorkspaceClient().files. Records (path, bytes, overwrite)."""

    def __init__(self):
        self.uploads: list[tuple[str, bytes, bool | None]] = []

    def upload(self, file_path, contents, *, overwrite=None):
        self.uploads.append((file_path, contents.read(), overwrite))


class FakeClient:
    def __init__(self):
        self.files = FakeFiles()


# --- volume_path ---------------------------------------------------------------


def test_volume_path_mirrors_local_layout():
    local = Path("data/raw/fred/2026-11-18/DGS10.json")
    assert (
        up.volume_path(local, Path("data/raw"), VOLUME)
        == "/Volumes/workspace/stock_market/raw/fred/2026-11-18/DGS10.json"
    )


def test_volume_path_works_with_absolute_root(tmp_path):
    local = tmp_path / "sec_edgar" / "2026-11-18" / "submissions.jsonl.gz"
    assert (
        up.volume_path(local, tmp_path, VOLUME)
        == f"{VOLUME}/sec_edgar/2026-11-18/submissions.jsonl.gz"
    )


# --- upload_run ----------------------------------------------------------------


def test_upload_run_uploads_every_file_with_overwrite(tmp_path):
    run_dir = tmp_path / "fred" / "2026-11-18"
    run_dir.mkdir(parents=True)
    (run_dir / "DGS10.json").write_bytes(b'{"a": 1}')
    (run_dir / "UNRATE.json").write_bytes(b'{"b": 2}')
    client = FakeClient()

    targets = up.upload_run(client, "fred", "2026-11-18", tmp_path, VOLUME)

    assert targets == [
        f"{VOLUME}/fred/2026-11-18/DGS10.json",
        f"{VOLUME}/fred/2026-11-18/UNRATE.json",
    ]
    assert client.files.uploads == [
        (f"{VOLUME}/fred/2026-11-18/DGS10.json", b'{"a": 1}', True),
        (f"{VOLUME}/fred/2026-11-18/UNRATE.json", b'{"b": 2}', True),
    ]


def test_upload_run_skips_subfolders(tmp_path):
    run_dir = tmp_path / "fred" / "2026-11-18"
    (run_dir / "nested").mkdir(parents=True)
    (run_dir / "DGS10.json").write_bytes(b"{}")
    client = FakeClient()

    up.upload_run(client, "fred", "2026-11-18", tmp_path, VOLUME)

    assert [path for path, _, _ in client.files.uploads] == [
        f"{VOLUME}/fred/2026-11-18/DGS10.json"
    ]


def test_upload_run_missing_folder_raises(tmp_path):
    with pytest.raises(FileNotFoundError, match="No run folder"):
        up.upload_run(FakeClient(), "fred", "2026-11-18", tmp_path, VOLUME)


def test_upload_run_empty_folder_raises(tmp_path):
    (tmp_path / "fred" / "2026-11-18").mkdir(parents=True)
    client = FakeClient()

    with pytest.raises(FileNotFoundError, match="empty"):
        up.upload_run(client, "fred", "2026-11-18", tmp_path, VOLUME)
    assert client.files.uploads == []
