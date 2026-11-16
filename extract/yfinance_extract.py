"""Extract daily prices, splits and dividends from yfinance into raw Parquet files.

Output (one folder per run date, overwritten on re-run):
    data/raw/yfinance/<run_date>/prices.parquet
    data/raw/yfinance/<run_date>/splits.parquet
    data/raw/yfinance/<run_date>/dividends.parquet

Examples:
    # quick test with a few tickers
    python extract/yfinance_extract.py --tickers AAPL MSFT BRK.B
    # one-off 5-year backfill for all S&P 500 tickers
    python extract/yfinance_extract.py --start 2021-11-16
"""

import argparse
import shutil
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

TICKERS_CSV = Path("data/reference/sp500_tickers.csv")
RAW_ROOT = Path("data/raw/yfinance")
LOOKBACK_DAYS = 10  # daily runs re-pull this many days to catch late corrections


def to_yahoo_symbol(symbol: str) -> str:
    """'BRK.B' -> 'BRK-B'. Yahoo uses '-' for share classes."""
    return symbol.replace('.', '-')


def load_tickers(path) -> list[str]:
    """Read sp500_tickers.csv and return Yahoo-style symbols."""

    df = pd.read_csv(path)

    return [to_yahoo_symbol(word) for word in df['Symbol']]


def flatten_prices(wide: pd.DataFrame) -> pd.DataFrame:
    """Turn yfinance's wide (Price, Ticker) table into one row per ticker per date.

    Kept separate from the download so it can be unit tested without the network.
    """
    # Wide -> long. Move the "Ticker" column level down into the rows,
    # so each row becomes one (date, ticker). The columns are then the
    # same few price fields no matter how many tickers we download.
    long = wide.stack(level="Ticker")

    # Drop rows where EVERY value is empty. They're leftovers from the
    # wide layout (the ticker didn't trade that day), not real data.
    # Partly-empty rows are kept on purpose: dbt tests should catch them.
    long = long.dropna(how="all")

    # Date and Ticker are the row index (labels) after stack; make them
    # normal columns so they're written to Parquet like any other column.
    long = long.reset_index()

    # Clean up the header: remove the leftover "Price" label, then rename
    # to snake_case ("Adj Close" -> "adj_close") for Spark and dbt.
    long.columns.name = None
    long.columns = [col.lower().replace(" ", "_") for col in long.columns]

    # Store the trading day as a real DATE (no time part), not a timestamp.
    long["date"] = long["date"].dt.date

    return long


def download_prices(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """Download daily prices and return one row per ticker per date."""
    # yfinance returns a WIDE table: one row per date, and
    # 2-level columns (Price, Ticker), e.g. (Close, AAPL), (Close, MSFT).
    wide = yf.download(
        tickers,
        start=start,
        end=end,  # exclusive: end="2026-11-13" -> last day is the 12th
        auto_adjust=False,  # keep raw Close; dbt does the dividend adjustment
        progress=False,  # no progress bar, keeps logs clean
        threads=True,  # download tickers in parallel
    )
    return flatten_prices(wide)


def series_to_frame(series: pd.Series, symbol: str, value_name: str) -> pd.DataFrame:
    """Turn one ticker's splits or dividends Series into rows of (date, ticker, value)."""
    return pd.DataFrame(
        {
            "date": series.index,  # original tz-aware timestamp; dbt casts to date
            "ticker": symbol,  # one value -> repeated on every row
            value_name: series.values,
        }
    )


def fetch_splits_and_dividends(
    tickers: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    """Return (splits, dividends, failed_tickers), one row per ticker per event.

    Yahoo has no bulk call for these, so we ask one ticker at a time.
    They always come back with the full history, whatever dates we use.
    """
    split_frames = []
    dividend_frames = []
    failed = []

    for symbol in tickers:
        try:
            ticker = yf.Ticker(symbol)
            splits = ticker.splits  # both read the same cached history call
            dividends = ticker.dividends
        # yfinance can raise many error types; one bad ticker must not stop the run.
        except Exception as error:  # noqa: BLE001
            print(f"  ! {symbol}: {error}")
            failed.append(symbol)
            continue

        # Most tickers have no splits: skip empty results instead of adding
        # empty frames (they'd break the column types when we concat).
        if not splits.empty:
            split_frames.append(series_to_frame(splits, symbol, "split_ratio"))
        if not dividends.empty:
            dividend_frames.append(series_to_frame(dividends, symbol, "dividend"))

    # Combine once at the end; concat inside the loop would re-copy everything.
    splits_df = combine(split_frames, ["date", "ticker", "split_ratio"])
    dividends_df = combine(dividend_frames, ["date", "ticker", "dividend"])
    return splits_df, dividends_df, failed


def combine(frames: list[pd.DataFrame], columns: list[str]) -> pd.DataFrame:
    """pd.concat, but return an empty table with the right columns if there's nothing."""
    if not frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True)


def write_run(tables: dict[str, pd.DataFrame], run_dir: Path) -> None:
    """Write each table to <run_dir>/<name>.parquet, replacing the folder if it exists.

    Idempotent: running the same run date twice leaves exactly one copy of each file.
    We write into a temporary folder first and swap it in at the end, so a crash
    halfway never leaves a half-written run folder behind.
    """
    tmp_dir = run_dir.with_name(run_dir.name + ".tmp")
    shutil.rmtree(tmp_dir, ignore_errors=True)  # leftover from a crashed run
    tmp_dir.mkdir(parents=True)

    for name, df in tables.items():
        df.to_parquet(
            tmp_dir / f"{name}.parquet",
            index=False,
            # Spark 3.5 can't read nanosecond timestamps; store microseconds.
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )

    shutil.rmtree(run_dir, ignore_errors=True)  # old copy of this run date
    tmp_dir.rename(run_dir)


def parse_args() -> argparse.Namespace:
    today = date.today()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--tickers",
        nargs="+",
        help="symbols to pull (default: all S&P 500 from the reference CSV)",
    )
    parser.add_argument(
        "--start",
        default=str(today - timedelta(days=LOOKBACK_DAYS)),
        help=f"first date, YYYY-MM-DD (default: {LOOKBACK_DAYS} days ago)",
    )
    parser.add_argument(
        "--end",
        default=str(today + timedelta(days=1)),
        help="end date, exclusive (default: tomorrow, so today is included)",
    )
    parser.add_argument(
        "--run-date",
        default=str(today),
        help="name of the output folder (default: today)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.tickers:
        tickers = [to_yahoo_symbol(t) for t in args.tickers]
    else:
        tickers = load_tickers(TICKERS_CSV)
    started = time.time()
    print(f"yfinance extract: {len(tickers)} tickers, {args.start} -> {args.end}")

    prices = download_prices(tickers, args.start, args.end)
    missing = sorted(set(tickers) - set(prices["ticker"]))
    print(f"  prices:    {len(prices):>7,} rows, missing tickers: {missing or 'none'}")

    splits, dividends, failed = fetch_splits_and_dividends(tickers)
    print(f"  splits:    {len(splits):>7,} rows")
    print(f"  dividends: {len(dividends):>7,} rows, failed tickers: {failed or 'none'}")

    run_dir = RAW_ROOT / args.run_date
    write_run({"prices": prices, "splits": splits, "dividends": dividends}, run_dir)
    print(f"  wrote {run_dir}/ in {time.time() - started:.0f}s")


if __name__ == "__main__":
    main()
