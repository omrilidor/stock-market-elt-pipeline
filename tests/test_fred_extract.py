"""Unit tests for extract/fred_extract.py. No network calls: a FakeSession
returns hand-made responses instead of calling FRED, and time.sleep is patched
out so the retry backoff doesn't slow the tests down."""

import json

import pytest
import requests

from extract import fred_extract as fred

FAKE_KEY = "k" * 32


def make_response(status: int, body: dict | None = None) -> requests.Response:
    """A real requests.Response, so .ok and .json() behave as in production."""
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(body or {}).encode()
    # Like the real URL, it contains the key, so the leak test can catch a regression.
    response.url = f"{fred.OBSERVATIONS_URL}?series_id=X&api_key={FAKE_KEY}"
    return response


def fred_body(*dates: str) -> dict:
    """A minimal FRED observations response with one row per date."""
    observations = [
        {
            "realtime_start": "2026-11-17",
            "realtime_end": "2026-11-17",
            "date": d,
            "value": "1.0",
        }
        for d in dates
    ]
    return {"count": len(observations), "observations": observations}


class FakeSession:
    """Stands in for requests.Session. `routes` maps a series_id to a list of
    responses, returned one per call (so we can script "503 first, then 200")."""

    def __init__(self, routes: dict[str, list[requests.Response]]):
        self.routes = routes
        self.calls: list[dict] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append(params)
        responses = self.routes[params["series_id"]]
        return responses.pop(0) if len(responses) > 1 else responses[0]


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    # autouse: applies to every test in this file.
    monkeypatch.setattr(fred.time, "sleep", lambda seconds: None)


# --- fetch_series --------------------------------------------------------------


def test_fetch_series_sends_params_and_returns_body_unchanged():
    body = fred_body("2026-01-01", "2026-01-02")
    session = FakeSession({"DGS10": [make_response(200, body)]})

    assert fred.fetch_series(session, "DGS10", FAKE_KEY) == body
    assert session.calls == [
        {"series_id": "DGS10", "api_key": FAKE_KEY, "file_type": "json"}
    ]


def test_fetch_series_retries_temporary_errors_then_succeeds():
    session = FakeSession(
        {
            "DGS10": [
                make_response(503),
                make_response(429),
                make_response(200, fred_body("2026-01-01")),
            ]
        }
    )
    assert len(fred.fetch_series(session, "DGS10", FAKE_KEY)["observations"]) == 1
    assert len(session.calls) == 3


def test_fetch_series_gives_up_after_max_retries():
    session = FakeSession({"DGS10": [make_response(503)]})  # always 503
    with pytest.raises(RuntimeError, match="Gave up"):
        fred.fetch_series(session, "DGS10", FAKE_KEY)
    assert len(session.calls) == fred.MAX_RETRIES


def test_fetch_series_permanent_error_fails_fast_without_leaking_key():
    body = {"error_code": 400, "error_message": "Bad Request."}
    session = FakeSession({"DGS10": [make_response(400, body)]})

    with pytest.raises(RuntimeError) as error:
        fred.fetch_series(session, "DGS10", FAKE_KEY)

    assert "HTTP 400" in str(error.value)
    assert FAKE_KEY not in str(error.value)  # secrets must not end up in logs
    assert len(session.calls) == 1  # 400 won't fix itself, so no retry


def test_fetch_series_raises_when_response_is_paginated():
    body = fred_body("2026-01-01")
    body["count"] = 200_000  # FRED says there are more rows than it sent
    session = FakeSession({"DGS10": [make_response(200, body)]})

    with pytest.raises(RuntimeError, match="pagination"):
        fred.fetch_series(session, "DGS10", FAKE_KEY)


# --- extract_run ---------------------------------------------------------------


def fake_fred() -> FakeSession:
    return FakeSession(
        {
            "DGS10": [make_response(200, fred_body("2026-01-01", "2026-01-02"))],
            "UNRATE": [make_response(200, fred_body("2026-01-01"))],
        }
    )


def test_extract_run_writes_one_file_per_series(tmp_path):
    run_dir = tmp_path / "2026-11-18"
    fred.extract_run(fake_fred(), ["DGS10", "UNRATE"], FAKE_KEY, run_dir)

    assert sorted(p.name for p in run_dir.iterdir()) == ["DGS10.json", "UNRATE.json"]
    saved = json.loads((run_dir / "DGS10.json").read_text())
    assert saved == fred_body("2026-01-01", "2026-01-02")  # raw = unchanged


def test_extract_run_rerun_replaces_run_and_leaves_no_tmp(tmp_path):
    run_dir = tmp_path / "2026-11-18"
    fred.extract_run(fake_fred(), ["DGS10", "UNRATE"], FAKE_KEY, run_dir)
    fred.extract_run(fake_fred(), ["DGS10"], FAKE_KEY, run_dir)  # same run date again

    assert [p.name for p in run_dir.iterdir()] == ["DGS10.json"]  # decision 5
    assert not (tmp_path / "2026-11-18.tmp").exists()


def test_extract_run_failure_keeps_previous_run_intact(tmp_path):
    run_dir = tmp_path / "2026-11-18"
    fred.extract_run(fake_fred(), ["DGS10", "UNRATE"], FAKE_KEY, run_dir)

    broken = FakeSession(
        {
            "DGS10": [make_response(200, fred_body("2026-01-01"))],
            "UNRATE": [make_response(400)],  # second series fails
        }
    )
    with pytest.raises(RuntimeError):
        fred.extract_run(broken, ["DGS10", "UNRATE"], FAKE_KEY, run_dir)

    # The old, complete run is still there: never half a run.
    assert sorted(p.name for p in run_dir.iterdir()) == ["DGS10.json", "UNRATE.json"]
    saved = json.loads((run_dir / "DGS10.json").read_text())
    assert len(saved["observations"]) == 2  # old content, not the new partial one


# --- main ----------------------------------------------------------------------


def test_main_exits_when_api_key_missing(monkeypatch):
    monkeypatch.setattr(fred, "load_dotenv", lambda: None)  # don't read the real .env
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    monkeypatch.setattr("sys.argv", ["fred_extract.py"])

    with pytest.raises(SystemExit, match="FRED_API_KEY is not set"):
        fred.main()
