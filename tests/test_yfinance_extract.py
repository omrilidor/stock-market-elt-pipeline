"""Unit tests for extract/yfinance_extract.py. No network calls: we build small
fake inputs by hand, shaped like what yfinance returns."""

import pandas as pd

from extract.yfinance_extract import (
    combine,
    flatten_prices,
    load_tickers,
    series_to_frame,
    to_yahoo_symbol,
    write_run,
)


def test_to_yahoo_symbol_replaces_dot_with_dash():
    assert to_yahoo_symbol("BRK.B") == "BRK-B"
    assert to_yahoo_symbol("BF.B") == "BF-B"


def test_to_yahoo_symbol_leaves_plain_symbols_alone():
    assert to_yahoo_symbol("AAPL") == "AAPL"


def test_load_tickers_reads_symbol_column_and_maps(tmp_path):
    # tmp_path is a pytest fixture: a fresh empty folder for this test only.
    csv = tmp_path / "tickers.csv"
    csv.write_text("Symbol,Security\nAAPL,Apple\nBRK.B,Berkshire Hathaway\n")
    assert load_tickers(csv) == ["AAPL", "BRK-B"]


def make_wide_prices() -> pd.DataFrame:
    """A tiny copy of yf.download's output: 2 dates x 2 tickers, with
    (Price, Ticker) column levels. MSFT has no data on the 2nd day."""
    columns = pd.MultiIndex.from_product(
        [["Adj Close", "Close", "Volume"], ["AAPL", "MSFT"]],
        names=["Price", "Ticker"],
    )
    index = pd.DatetimeIndex(["2026-11-10", "2026-11-11"], name="Date")
    data = [
        [1.0, 2.0, 10.0, 20.0, 100, 200],
        [1.5, None, 15.0, None, 150, None],
    ]
    return pd.DataFrame(data, index=index, columns=columns)


def test_flatten_prices_returns_one_row_per_ticker_per_date():
    long = flatten_prices(make_wide_prices())

    assert list(long.columns) == ["date", "ticker", "adj_close", "close", "volume"]
    # 2 dates x 2 tickers = 4, minus the all-empty MSFT row on the 11th = 3
    assert len(long) == 3
    assert not long.duplicated(["date", "ticker"]).any()

    aapl_day1 = long[(long["ticker"] == "AAPL") & (long["date"] == pd.Timestamp("2026-11-10").date())]
    assert aapl_day1["close"].item() == 10.0


def test_series_to_frame_adds_ticker_column():
    index = pd.DatetimeIndex(["2020-08-31 09:30"], tz="America/New_York", name="Date")
    splits = pd.Series([4.0], index=index, name="Stock Splits")

    df = series_to_frame(splits, "AAPL", "split_ratio")

    assert list(df.columns) == ["date", "ticker", "split_ratio"]
    assert df.iloc[0]["ticker"] == "AAPL"
    assert df.iloc[0]["split_ratio"] == 4.0
    assert str(df["date"].dt.tz) == "America/New_York"  # timezone kept for dbt


def test_combine_with_no_frames_returns_empty_table_with_columns():
    df = combine([], ["date", "ticker", "dividend"])
    assert df.empty
    assert list(df.columns) == ["date", "ticker", "dividend"]


def test_write_run_twice_is_idempotent(tmp_path):
    run_dir = tmp_path / "2026-11-16"
    table = pd.DataFrame({"ticker": ["AAPL"], "close": [1.0]})

    write_run({"prices": table}, run_dir)
    write_run({"prices": table}, run_dir)  # same run date again

    files = sorted(p.name for p in tmp_path.rglob("*") if p.is_file())
    assert files == ["prices.parquet"]  # one file, no leftovers or .tmp folder
    assert len(pd.read_parquet(run_dir / "prices.parquet")) == 1
