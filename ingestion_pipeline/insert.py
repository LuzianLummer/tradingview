import pandas as pd
from sqlalchemy.exc import IntegrityError
import logging
from sqlalchemy import func
from datetime import datetime
from typing import Optional

from ingestion_pipeline.database_manager import DatabaseManager
from ingestion_pipeline.models import MarketData, RawMarketData
from ingestion_pipeline.logger_config import setup_logging

setup_logging()

logger = logging.getLogger(__name__)


class DataIngestor:
    def __init__(self):
        pass

    def get_latest_timestamp_from_db(self, symbol: str) -> Optional[datetime]:
        """
        Ruft den neuesten Zeitstempel für ein bestimmtes Symbol aus der raw_market_data Tabelle ab.
        """
        with DatabaseManager.get_session() as session:
            latest_timestamp = (
                session.query(func.max(RawMarketData.timestamp))
                .filter(RawMarketData.symbol == symbol)
                .scalar()
            )
            return latest_timestamp

    def save_market_data(self, df: pd.DataFrame):
        """
        Speichert ein Pandas DataFrame mit Marktdaten in die Datenbank.
        Handhabt potenzielle Duplikatsfehler.
        """
        if df.empty:
            logger.warning("Leeres DataFrame erhalten, keine Daten zum Speichern.")
            return

        # Konvertiere DataFrame in eine Liste von MarketData-Objekten
        # Dies ist effizienter, als Zeile für Zeile zu adden
        market_data_objects = [
            MarketData(
                symbol=row["symbol"].item(),
                timestamp=row["timestamp"].item(),
                open=row["open"].item(),
                high=row["high"].item(),
                low=row["low"].item(),
                close=row["close"].item(),
                volume=row["volume"].item(),
            )
            for index, row in df.iterrows()
        ]

        logger.info(
            f"Versuche, {len(market_data_objects)} Datensätze in die Datenbank zu speichern..."
        )
        with DatabaseManager.get_session() as session:
            try:
                # Fügen Sie alle Objekte der Session hinzu
                session.bulk_save_objects(market_data_objects)
                logger.info(
                    f"Erfolgreich {len(market_data_objects)} Datensätze in die Datenbank geladen."
                )
            except IntegrityError:
                logger.warning(
                    "Einige Datensätze existieren bereits (Duplikat). Überspringe."
                )
                # Hier könnten Sie eine Strategie implementieren, z.B. Update statt Insert,
                # wenn die Daten sich ändern können (upsert).
                # Für dieses Beispiel lassen wir es einfach und ignorieren Duplikate.
            except Exception as e:
                logger.error(f"Fehler beim Speichern der Marktdaten: {e}")
                raise  # Die Exception wird vom Kontextmanager behandelt (Rollback und Re-raise)

    def save_raw_market_data(self, df: pd.DataFrame):
        """
        Speichert ein Pandas DataFrame mit Roh-Marktdaten in die Datenbank.
        Handhabt potenzielle Duplikatsfehler.
        """
        if df.empty:
            logger.warning("Leeres DataFrame erhalten, keine Rohdaten zum Speichern.")
            return

        with DatabaseManager.get_session() as session:
            try:
                existing_records = (
                    session.query(RawMarketData.symbol, RawMarketData.timestamp)
                    .filter(RawMarketData.symbol.in_(df["symbol"].unique().tolist()))
                    .filter(RawMarketData.timestamp.in_(df["timestamp"].unique().tolist()))
                    .all()
                )
                existing_keys = set((r.symbol, r.timestamp) for r in existing_records)

                new_data_rows = []
                for index, row in df.iterrows():
                    row_key = (row["symbol"].item(), row["timestamp"].item())
                    if row_key not in existing_keys:
                        new_data_rows.append(
                            RawMarketData(
                                symbol=row["symbol"].item(),
                                timestamp=row["timestamp"].item(),
                                open=row["open"].item(),
                                high=row["high"].item(),
                                low=row["low"].item(),
                                close=row["close"].item(),
                                volume=row["volume"].item(),
                            )
                        )
                    else:
                        logger.info(
                            f'Skipping duplicate record for {row["symbol"].item()} at {row["timestamp"].item()}'
                        )
                if not new_data_rows:
                    logger.info("No new unique raw data records to save.")
                    return

                logger.info(
                    f"Versuche, {len(new_data_rows)} neue Rohdatensätze in die Datenbank zu speichern..."
                )
                session.bulk_save_objects(new_data_rows)
                logger.info(
                    f"Erfolgreich {len(new_data_rows)} neue Rohdatensätze in die Datenbank geladen."
                )
            except Exception as e:
                logger.error(f"Fehler beim Speichern der Roh-Marktdaten: {e}")
                raise
