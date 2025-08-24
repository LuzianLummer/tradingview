import logging
from pathlib import Path
from typing import List

from tradingview.common.logger_config import setup_logging

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


