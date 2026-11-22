"""Unit tests for extract/download_raw.py. No network calls: a FakeClient serves
files from a dict instead of talking to Databricks."""

import io
from dataclasses import dataclass

import pytest

from extract import download_raw as dl

VOLUME = "/Volumes/workspace/stock_market/raw"


@dataclass
class FakeEntry:
    """Stands in for databricks.sdk's DirectoryEntry (only the fields we use)."""

    name: str
    path: str
    is_directory: bool = False


@dataclass
class FakeDownload:
    """Stands in for DownloadResponse: .contents is a readable stream."""

    contents: io.BytesIO


class FakeFiles:
    """Stands in for WorkspaceClient().files, backed by {volume_path: bytes}."""

    def __init__(self, files: dict[str, bytes], subfolders: list[str] = ()):
        self.files = files
        self.subfolders = list(subfolders)
        self.listed: list[str] = []

    def list_directory_contents(self, directory_path):
        self.listed.append(directory_path)
        prefix = directory_path + "/"
        # Unsorted on purpose, to check that download_run sorts by name.
        for path in reversed(list(self.files)):
            if path.startswith(prefix):
                yield FakeEntry(name=path.removeprefix(prefix), path=path)
        for path in self.subfolders:
            yield FakeEntry(name=path.rsplit("/", 1)[-1], path=path, is_directory=True)

    def download(self, file_path):
        return FakeDownload(contents=io.BytesIO(self.files[file_path]))


class FakeClient:
    def __init__(self, files: dict[str, bytes], subfolders: list[str] = ()):
        self.files = FakeFiles(files, subfolders)


RUN = f"{VOLUME}/yfinance/2026-11-16"


def test_download_run_writes_every_file_sorted(tmp_path):
    client = FakeClient({f"{RUN}/prices.parquet": b"PRICES", f"{RUN}/splits.parquet": b"SPLITS"})

    paths = dl.download_run(client, "yfinance", "2026-11-16", tmp_path, VOLUME)

    run_dir = tmp_path / "yfinance" / "2026-11-16"
    assert paths == [run_dir / "prices.parquet", run_dir / "splits.parquet"]
    assert (run_dir / "prices.parquet").read_bytes() == b"PRICES"
    assert (run_dir / "splits.parquet").read_bytes() == b"SPLITS"
    assert client.files.listed == [RUN]


def test_download_run_skips_subfolders(tmp_path):
    client = FakeClient({f"{RUN}/prices.parquet": b"P"}, subfolders=[f"{RUN}/nested"])

    paths = dl.download_run(client, "yfinance", "2026-11-16", tmp_path, VOLUME)

    assert [p.name for p in paths] == ["prices.parquet"]


def test_download_run_twice_overwrites(tmp_path):
    dl.download_run(FakeClient({f"{RUN}/a.parquet": b"OLD"}), "yfinance", "2026-11-16", tmp_path, VOLUME)
    (path,) = dl.download_run(
        FakeClient({f"{RUN}/a.parquet": b"NEW"}), "yfinance", "2026-11-16", tmp_path, VOLUME
    )

    assert path.read_bytes() == b"NEW"


def test_download_run_empty_folder_raises_and_creates_nothing(tmp_path):
    with pytest.raises(FileNotFoundError, match="empty"):
        dl.download_run(FakeClient({}), "yfinance", "2026-11-16", tmp_path, VOLUME)
    assert not (tmp_path / "yfinance").exists()
