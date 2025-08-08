import logging
import random
import time
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests
import yfinance as yf
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.common.logger_config import setup_logging


setup_logging()
logger = logging.getLogger(__name__)


def read_tickers_from_file(file_name: str = "tickers.txt") -> List[str]:
    current_dir = Path(__file__).resolve().parent
    tickers_file_path = current_dir / file_name
    tickers: List[str] = []
    try:
        with open(tickers_file_path, "r") as f:
            for line in f:
                ticker = line.strip()
                if ticker:
                    tickers.append(ticker)
    except FileNotFoundError:
        logger.error(f"Ticker file not found: {tickers_file_path}")
        return []
    return tickers


class DataExtractor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                )
            }
        )
        retry_strategy = Retry(
            total=5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            backoff_factor=1.5,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def fetch_market_data(
        self,
        symbol: str,
        period: Optional[str] = None,
        interval: str = "1d",
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        max_retries = 3
        retries = 0
        backoff_factor = 2

        def _download_from_stooq(
            symbol_in: str, start_s: Optional[str], end_s: Optional[str]
        ) -> Optional[pd.DataFrame]:
            candidates = [symbol_in.lower()]
            if not symbol_in.lower().endswith((".us", ".de", ".pl", ".jp")):
                candidates.insert(0, f"{symbol_in.lower()}.us")
            for sym in candidates:
                url = f"https://stooq.com/q/d/l/?s={sym}&i=d"
                try:
                    resp = self.session.get(url, timeout=15)
                    if (
                        (resp.status_code != 200)
                        or (not resp.text)
                        or (resp.text.strip() == "")
                    ):
                        continue
                    df_csv = pd.read_csv(StringIO(resp.text))
                    if df_csv.empty:
                        continue
                    df_csv.rename(
                        columns={
                            "Date": "timestamp",
                            "Open": "open",
                            "High": "high",
                            "Low": "low",
                            "Close": "close",
                            "Volume": "volume",
                        },
                        inplace=True,
                    )
                    df_csv["timestamp"] = pd.to_datetime(df_csv["timestamp"])
                    if start_s:
                        df_csv = df_csv[df_csv["timestamp"] >= pd.to_datetime(start_s)]
                    if end_s:
                        df_csv = df_csv[df_csv["timestamp"] <= pd.to_datetime(end_s)]
                    if df_csv.empty:
                        continue
                    df_csv["symbol"] = symbol_in
                    return df_csv[
                        [
                            "symbol",
                            "timestamp",
                            "open",
                            "high",
                            "low",
                            "close",
                            "volume",
                        ]
                    ]
                except Exception:
                    continue
            return None

        def _download_with_fallback(
            symbol_in: str,
            start_s: Optional[str],
            end_s: Optional[str],
            period_s: Optional[str],
        ) -> Optional[pd.DataFrame]:
            data_local = _download_from_stooq(symbol_in, start_s, end_s)
            if data_local is not None and not data_local.empty:
                return data_local
            if start_s and end_s:
                data_local = yf.download(
                    tickers=symbol_in,
                    start=start_s,
                    end=end_s,
                    interval=interval,
                    progress=False,
                    auto_adjust=False,
                    session=self.session,
                    threads=False,
                )
            else:
                data_local = yf.download(
                    tickers=symbol_in,
                    period=period_s,
                    interval=interval,
                    progress=False,
                    auto_adjust=False,
                    session=self.session,
                    threads=False,
                )
            if not data_local.empty:
                return data_local
            ticker = yf.Ticker(symbol_in, session=self.session)
            if start_s and end_s:
                data_local = ticker.history(
                    start=start_s, end=end_s, interval=interval, auto_adjust=False
                )
            else:
                data_local = ticker.history(
                    period=period_s or "max", interval=interval, auto_adjust=False
                )
            if data_local.empty:
                return None
            return data_local

        def _normalize_dataframe(df_in: pd.DataFrame) -> pd.DataFrame:
            df_out = df_in.rename(
                columns={
                    "Open": "open",
                    "High": "high",
                    "Low": "low",
                    "Close": "close",
                    "Volume": "volume",
                }
            ).copy()
            df_out.reset_index(inplace=True)
            df_out.rename(
                columns={"Datetime": "timestamp", "Date": "timestamp"}, inplace=True
            )
            df_out["symbol"] = symbol
            df_out = df_out[
                ["symbol", "timestamp", "open", "high", "low", "close", "volume"]
            ]
            return df_out

        def _fetch_chunked(start_s: str, end_s: str) -> Optional[pd.DataFrame]:
            start_dt = datetime.fromisoformat(start_s)
            end_dt = datetime.fromisoformat(end_s)
            stooq_df = _download_from_stooq(symbol, start_s, end_s)
            if stooq_df is not None and not stooq_df.empty:
                stooq_df.drop_duplicates(subset=["symbol", "timestamp"], inplace=True)
                stooq_df.sort_values(by="timestamp", inplace=True)
                return stooq_df
            chunk_size_days = 365 * 5
            frames: list[pd.DataFrame] = []
            current_start = start_dt
            while current_start <= end_dt:
                current_end = min(
                    current_start + timedelta(days=chunk_size_days), end_dt
                )
                local_retries = 0
                while local_retries <= max_retries:
                    try:
                        data_local = _download_with_fallback(
                            symbol,
                            current_start.strftime("%Y-%m-%d"),
                            current_end.strftime("%Y-%m-%d"),
                            None,
                        )
                        if data_local is None or data_local.empty:
                            raise ValueError(
                                "Empty dataframe from Yahoo Finance (chunk)"
                            )
                        frames.append(_normalize_dataframe(data_local))
                        break
                    except Exception as ex:
                        local_retries += 1
                        if local_retries > max_retries:
                            logger.error(
                                f"Chunk {current_start:%Y-%m-%d}..{current_end:%Y-%m-%d} for {symbol} failed: {ex}"
                            )
                            return None
                        wait_time = backoff_factor**local_retries + random.uniform(
                            0.5, 1.5
                        )
                        logger.info(
                            f"Retry chunk for {symbol} in {wait_time:.1f}s after error: {ex}"
                        )
                        time.sleep(wait_time)
                time.sleep(random.uniform(1.0, 2.0))
                current_start = current_end + timedelta(days=1)
            if not frames:
                return None
            combined = pd.concat(frames, ignore_index=True)
            combined.drop_duplicates(subset=["symbol", "timestamp"], inplace=True)
            combined.sort_values(by="timestamp", inplace=True)
            return combined

        while retries <= max_retries:
            log_msg = f"Fetching data for {symbol}"
            if start and end:
                log_msg += f" from {start} to {end}"
            elif period:
                log_msg += f" for period {period}"
            log_msg += (
                f" interval {interval}... (attempt {retries + 1}/{max_retries + 1})"
            )
            logger.info(log_msg)
            try:
                if start and end:
                    start_dt = datetime.fromisoformat(start)
                    end_dt = datetime.fromisoformat(end)
                    if (end_dt - start_dt).days > 365 * 5:
                        data = _fetch_chunked(start, end)
                        if data is None or data.empty:
                            raise ValueError("Empty dataframe after chunked fetch")
                    else:
                        data = _download_with_fallback(symbol, start, end, None)
                        if data is None or data.empty:
                            raise ValueError("Empty dataframe from Yahoo Finance")
                        data = _normalize_dataframe(data)
                else:
                    data = _download_with_fallback(symbol, None, None, period)
                    if data is None or data.empty:
                        raise ValueError("Empty dataframe from Yahoo Finance")
                    data = _normalize_dataframe(data)
                logger.info(f"Fetched and normalized {len(data)} rows for {symbol}.")
                return data
            except Exception as e:
                logger.error(
                    f"Error fetching {symbol} (attempt {retries + 1}/{max_retries + 1}): {e}"
                )
                retries += 1
                if retries <= max_retries:
                    wait_time = backoff_factor**retries + random.uniform(0.5, 1.5)
                    logger.info(f"Waiting {wait_time:.1f}s before next attempt...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"All {max_retries + 1} attempts for {symbol} failed.")
                    return None
