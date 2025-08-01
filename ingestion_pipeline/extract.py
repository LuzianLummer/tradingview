import logging
import pandas as pd
import yfinance as yf
from typing import Optional, List
from pathlib import Path
import time  # Import time module for sleep


from ingestion_pipeline.database_manager import DatabaseManager
from ingestion_pipeline.models import MarketData
from ingestion_pipeline.logger_config import setup_logging


setup_logging()  # Zentrales Logging konfigurieren

logger = logging.getLogger(__name__)


def read_tickers_from_file(file_name: str = "tickers.txt") -> List[str]:
    """
    Liest Ticker-Symbole aus einer Textdatei, die sich im selben Verzeichnis wie diese Datei befindet.
    """
    # Pfad zur aktuellen Datei (extract.py)
    current_file_path = Path(__file__).resolve()
    # Pfad zum Verzeichnis, in dem sich extract.py befindet
    current_dir = current_file_path.parent
    # Vollständiger Pfad zur Ticker-Datei
    tickers_file_path = current_dir / file_name

    tickers = []
    try:
        with open(tickers_file_path, "r") as f:
            for line in f:
                ticker = line.strip()
                if ticker:
                    tickers.append(ticker)
    except FileNotFoundError:
        logger.error(f"Die Ticker-Datei wurde nicht gefunden: {tickers_file_path}")
        return []
    return tickers


class DataExtractor:
    def __init__(self):
        pass

    def fetch_market_data(
        self,
        symbol: str,
        period: Optional[str] = None,
        interval: str = "1d",
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Holt Marktdaten für ein bestimmtes Symbol mit yfinance.
        Implementiert einen Wiederholungsmechanismus bei Fehlern.
        Priorisiert die Verwendung von start/end-Parametern gegenüber period, falls vorhanden.
        """
        max_retries = 3
        retries = 0
        backoff_factor = 2  # Multiplikator für die Wartezeit

        while retries <= max_retries:
            log_msg = f"Hole Daten für {symbol}"
            if start and end:
                log_msg += f" von {start} bis {end}"
            elif period:
                log_msg += f" für den Zeitraum {period}"
            log_msg += f" und Intervall {interval}... (Versuch {retries + 1}/{max_retries + 1})"
            logger.info(log_msg)

            try:
                if start and end:
                    data = yf.download(
                        tickers=symbol,
                        start=start,
                        end=end,
                        interval=interval,
                        progress=False,
                    )
                else:
                    data = yf.download(
                        tickers=symbol,
                        period=period,
                        interval=interval,
                        progress=False,
                        auto_adjust=True,
                    )
                if data.empty:
                    logger.warning(
                        f"Keine Daten für das Symbol {symbol} gefunden nach {retries + 1} Versuchen."
                    )
                    return None

                data.rename(
                    columns={
                        "Open": "open",
                        "High": "high",
                        "Low": "low",
                        "Close": "close",
                        "Volume": "volume",
                    },
                    inplace=True,
                )
                data["symbol"] = symbol
                data.reset_index(inplace=True)
                data.rename(
                    columns={"Datetime": "timestamp", "Date": "timestamp"}, inplace=True
                )

                required_columns = [
                    "symbol",
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                ]
                data = data[required_columns]

                # Ensure all columns are scalar values, not Series (can happen for single-row results)
                for col in data.columns:
                    if isinstance(data[col].iloc[0], pd.Series):
                        data[col] = data[col].apply(
                            lambda x: x.item() if isinstance(x, pd.Series) else x
                        )

                logger.info(
                    f"Erfolgreich {len(data)} Datensätze für {symbol} geholt und formatiert."
                )
                return data
            except Exception as e:
                logger.error(
                    f"Fehler beim Holen der Daten für {symbol} (Versuch {retries + 1}/{max_retries + 1}): {e}"
                )
                retries += 1
                if retries <= max_retries:
                    wait_time = backoff_factor**retries
                    logger.info(
                        f"Warte {wait_time} Sekunden vor dem nächsten Versuch..."
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(
                        f"Alle {max_retries + 1} Versuche für {symbol} fehlgeschlagen. Abbruch."
                    )
                    return None
