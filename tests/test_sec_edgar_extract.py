"""Unit tests for extract/sec_edgar_extract.py. No network calls: a FakeSession
returns hand-made responses instead of calling SEC, and time.sleep is patched
out so the throttle and retry backoff don't slow the tests down."""

import gzip
import json

import pytest
import requests

from extract import sec_edgar_extract as sec


def make_response(status: int, body: dict | None = None) -> requests.Response:
    """A real requests.Response, so raise_for_status() and .json() behave as in production."""
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(body or {}).encode()
    response.url = "https://example.test"
    return response


class FakeSession:
    """Stands in for requests.Session. `routes` maps a URL to a list of responses,
    returned one per call (so we can script "503 first, then 200")."""

    def __init__(self, routes: dict[str, list[requests.Response]]):
        self.routes = routes
        self.calls: list[str] = []

    def get(self, url, timeout=None):
        self.calls.append(url)
        responses = self.routes.get(url)
        if not responses:
            return make_response(404)
        return responses.pop(0) if len(responses) > 1 else responses[0]


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    # autouse: applies to every test in this file.
    monkeypatch.setattr(sec.time, "sleep", lambda seconds: None)


def read_jsonl_gz(path) -> list[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return [json.loads(line) for line in f]


# --- load_ciks -----------------------------------------------------------------


def test_load_ciks_dedupes_sorts_and_keeps_leading_zeros(tmp_path):
    csv = tmp_path / "tickers.csv"
    csv.write_text(
        "Symbol,CIK\n"
        "MSFT,0000789019\n"
        "GOOGL,0001652044\n"
        "GOOG,0001652044\n"  # same company, second share class
        "AAPL,0000320193\n"
    )
    assert sec.load_ciks(csv) == ["0000320193", "0000789019", "0001652044"]


# --- make_session --------------------------------------------------------------


def test_make_session_sets_user_agent():
    session = sec.make_session("test-agent admin@example.com")
    assert session.headers["User-Agent"] == "test-agent admin@example.com"


# --- fetch_json ----------------------------------------------------------------


def test_fetch_json_returns_parsed_body():
    session = FakeSession({"u": [make_response(200, {"name": "Apple Inc."})]})
    assert sec.fetch_json(session, "u") == {"name": "Apple Inc."}


def test_fetch_json_returns_none_on_404():
    assert sec.fetch_json(FakeSession({}), "u") is None


def test_fetch_json_retries_temporary_errors_then_succeeds():
    session = FakeSession(
        {"u": [make_response(503), make_response(429), make_response(200, {"ok": 1})]}
    )
    assert sec.fetch_json(session, "u") == {"ok": 1}
    assert len(session.calls) == 3


def test_fetch_json_gives_up_after_max_retries():
    session = FakeSession({"u": [make_response(503)]})  # always 503
    with pytest.raises(RuntimeError, match="Gave up"):
        sec.fetch_json(session, "u")
    assert len(session.calls) == sec.MAX_RETRIES


def test_fetch_json_does_not_retry_permanent_errors():
    session = FakeSession({"u": [make_response(403)]})
    with pytest.raises(requests.HTTPError):
        sec.fetch_json(session, "u")
    assert len(session.calls) == 1  # 403 won't fix itself, so no retry


# --- extract_run ---------------------------------------------------------------


def fake_sec() -> FakeSession:
    """Two companies; the second has no companyfacts (404)."""
    a, b = "0000000001", "0000000002"
    return FakeSession(
        {
            sec.SUBMISSIONS_URL.format(cik=a): [
                make_response(200, {"cik": a, "name": "A"})
            ],
            sec.COMPANYFACTS_URL.format(cik=a): [
                make_response(200, {"cik": 1, "facts": {}})
            ],
            sec.SUBMISSIONS_URL.format(cik=b): [
                make_response(200, {"cik": b, "name": "B"})
            ],
        }
    )


def test_extract_run_writes_one_line_per_company_and_reports_missing(tmp_path):
    run_dir = tmp_path / "2026-11-17"
    missing = sec.extract_run(fake_sec(), ["0000000001", "0000000002"], run_dir)

    assert missing == ["companyfacts:0000000002"]
    assert sorted(p.name for p in run_dir.iterdir()) == [
        "companyfacts.jsonl.gz",
        "submissions.jsonl.gz",
    ]
    assert [r["name"] for r in read_jsonl_gz(run_dir / "submissions.jsonl.gz")] == [
        "A",
        "B",
    ]
    assert len(read_jsonl_gz(run_dir / "companyfacts.jsonl.gz")) == 1


def test_extract_run_rerun_overwrites_instead_of_duplicating(tmp_path):
    run_dir = tmp_path / "2026-11-17"
    ciks = ["0000000001", "0000000002"]
    sec.extract_run(fake_sec(), ciks, run_dir)
    sec.extract_run(fake_sec(), ciks, run_dir)  # same run date again

    assert len(read_jsonl_gz(run_dir / "submissions.jsonl.gz")) == 2  # not 4
    assert not (tmp_path / "2026-11-17.tmp").exists()  # no leftover temp folder
