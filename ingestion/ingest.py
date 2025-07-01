# Importiere benötigte Standardbibliotheken
import os
import time
from datetime import datetime
import logging
from typing import Dict, Any, Optional

# Importiere externe Bibliotheken für Datenverarbeitung und Datenbankzugriff
import pandas as pd
import psycopg2
import numpy as np
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import yfinance as yf
import schedule

# Konfiguriere das Logging-System
# Dies ermöglicht uns, wichtige Informationen und Fehler während der Ausführung zu protokollieren
logging.basicConfig(
    level=logging.INFO,  # Setze Log-Level auf INFO (zeigt alle wichtigen Meldungen)
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'  # Definiere das Format der Log-Nachrichten
)
logger = logging.getLogger(__name__)  # Erstelle einen Logger für dieses Modul

# Lade Umgebungsvariablen aus .env Datei (falls vorhanden)
# Diese werden für die Datenbankverbindung verwendet
load_dotenv()

class DataIngestion:
    """
    Hauptklasse für die Datenaufnahme.
    Verwaltet die Verbindung zur Datenbank und führt Datenbankoperationen aus.
    """
    def __init__(self):
        # Konfiguriere Datenbankverbindungsparameter
        # os.getenv() versucht die Werte aus Umgebungsvariablen zu lesen
        # Falls nicht gefunden, werden die Standardwerte verwendet
        self.db_params = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'database': os.getenv('POSTGRES_DB', 'tradingview'),
            'user': os.getenv('POSTGRES_USER', 'tradingview'),
            'password': os.getenv('POSTGRES_PASSWORD', 'tradingview')
        }
        self.conn = None  # Initialisiere Verbindungsobjekt als None
        self.connect()    # Stelle die Datenbankverbindung her

    def connect(self):
        """
        Stellt die Verbindung zur PostgreSQL-Datenbank her.
        Bei Fehlern wird eine Exception geworfen und geloggt.
        """
        try:
            # Versuche Verbindung mit den konfigurierten Parametern herzustellen
            self.conn = psycopg2.connect(**self.db_params)
            logger.info("Successfully connected to the database")
        except Exception as e:
            # Bei Fehlern: Logge den Fehler und werfe ihn weiter
            logger.error(f"Error connecting to the database: {e}")
            raise

    def insert_market_data(self, data: pd.DataFrame):
        """
        Fügt Marktdaten in die Datenbank ein.
        
        Args:
            data (pd.DataFrame): DataFrame mit Marktdaten (OHLCV + Symbol und Timestamp)
        """
        # Stelle sicher, dass eine Verbindung existiert
        if self.conn is None or self.conn.closed:
            logger.warning("Database connection was closed. Reconnecting...")
            self.connect()

        try:
            with self.conn.cursor() as cur:
                # Konvertiere DataFrame in Liste von Tupeln für effizientes Einfügen
                values = [tuple(x) for x in data.to_numpy()]
                
                # SQL-Query für das Einfügen der Daten
                # %s ist ein Platzhalter für die execute_values Funktion
                insert_query = """
                    INSERT INTO market_data 
                    (symbol, timestamp, open, high, low, close, volume)
                    VALUES %s
                    ON CONFLICT (symbol, timestamp) DO NOTHING;
                """
                
                # Führe das Einfügen aus
                # execute_values ist effizienter als einzelne INSERTs
                execute_values(cur, insert_query, values)
                self.conn.commit()  # Bestätige die Transaktion
                logger.info(f"Successfully inserted/updated {len(values)} records for {data['symbol'].iloc[0]}")
                
        except Exception as e:
            # Bei Fehlern: Mache die Transaktion rückgängig
            self.conn.rollback()
            logger.error(f"Error inserting market data: {e}")
            raise

    def insert_trading_signal(self, signal_data: Dict[str, Any]):
        """
        Fügt ein Handelssignal in die Datenbank ein.
        
        Args:
            signal_data (Dict): Dictionary mit Signal-Daten (Symbol, Timestamp, Signal-Typ, Preis, Konfidenz)
        """
        if self.conn is None or self.conn.closed:
            self.connect()

        try:
            with self.conn.cursor() as cur:
                # SQL-Query für das Einfügen eines Signals
                insert_query = """
                    INSERT INTO trading_signals 
                    (symbol, timestamp, signal_type, price, confidence)
                    VALUES (%s, %s, %s, %s, %s)
                """
                # Führe das Einfügen mit den Signal-Daten aus
                cur.execute(insert_query, (
                    signal_data['symbol'],
                    signal_data['timestamp'],
                    signal_data['signal_type'],
                    signal_data['price'],
                    signal_data['confidence']
                ))
                self.conn.commit()
                logger.info(f"Successfully inserted trading signal for {signal_data['symbol']}")
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error inserting trading signal: {e}")
            raise

    def close(self):
        """
        Schließt die Datenbankverbindung.
        Sollte immer aufgerufen werden, wenn die Verbindung nicht mehr benötigt wird.
        """
        if self.conn is not None and not self.conn.closed:
            self.conn.close()
            logger.info("Database connection closed")

def generate_mock_market_data(symbol: str, periods: int = 10, interval_minutes: int = 15) -> pd.DataFrame:
    """
    Erzeugt simulierte Marktdaten für Testzwecke.
    """
    now = pd.Timestamp.now().floor('min')
    timestamps = [now - pd.Timedelta(minutes=interval_minutes * i) for i in range(periods)][::-1]
    data = {
        'symbol': [symbol] * periods,
        'timestamp': timestamps,
        'open': np.random.uniform(100, 200, periods),
        'high': np.random.uniform(200, 300, periods),
        'low': np.random.uniform(50, 100, periods),
        'close': np.random.uniform(100, 200, periods),
        'volume': np.random.randint(1000, 10000, periods)
    }
    df = pd.DataFrame(data)
    return df

def fetch_market_data(symbol: str, period: str = "1d", interval: str = "15m") -> Optional[pd.DataFrame]:
    """
    Holt Marktdaten für ein bestimmtes Symbol mit yfinance oder simuliert sie.
    """
    use_mock = os.getenv("USE_MOCK_DATA", "false").lower() == "true"
    if use_mock:
        logger.info(f"USE_MOCK_DATA: {os.getenv('USE_MOCK_DATA')}")
        logger.info(f"Simuliere Marktdaten für {symbol}")
        mock_df=generate_mock_market_data(symbol)
        print(mock_df)
        return mock_df
    logger.info(f"Hole Daten für {symbol} für den Zeitraum {period} und Intervall {interval}...")
    try:
        data = yf.download(tickers=symbol, period=period, interval=interval, progress=False)
        if data.empty:
            logger.warning(f"Keine Daten für das Symbol {symbol} gefunden.")
            return None

        data.rename(columns={
            'Open': 'open', 'High': 'high', 'Low': 'low',
            'Close': 'close', 'Volume': 'volume'
        }, inplace=True)
        data['symbol'] = symbol
        data.reset_index(inplace=True)
        data.rename(columns={'Datetime': 'timestamp', 'Date': 'timestamp'}, inplace=True)

        required_columns = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
        data = data[required_columns]

        logger.info(f"Erfolgreich {len(data)} Datensätze für {symbol} geholt und formatiert.")
        return data
    except Exception as e:
        logger.error(f"Fehler beim Holen der Daten für {symbol}: {e}")
        return None

def run_ingestion_job():
    """
    Der Haupt-Job, der vom Scheduler ausgeführt wird.
    Holt Daten für vordefinierte Symbole und fügt sie in die DB ein.
    """
    logger.info("--- Starte Ingestion-Job ---")
    symbols_to_fetch = ['BTC-USD', 'ETH-USD', 'SOL-USD']
    ingestion = None
    try:
        ingestion = DataIngestion()
        for symbol in symbols_to_fetch:
            market_data = fetch_market_data(symbol, period="2d", interval="15m")
            if market_data is not None and not market_data.empty:
                ingestion.insert_market_data(market_data)
            else:
                logger.warning(f"Keine Daten zum Einfügen für {symbol}.")
            time.sleep(2)
    except Exception as e:
        logger.error(f"Ein Fehler ist im Ingestion-Job aufgetreten: {e}")
    finally:
        if ingestion:
            ingestion.close()
        logger.info("--- Ingestion-Job beendet ---")

def main():
    """
    Hauptfunktion, die den Scheduler für die Datenaufnahme einrichtet und startet.
    """
    use_mock = os.getenv("USE_MOCK_DATA", "false").lower() == "true"
    if use_mock:
        logger.info("USE_MOCK_DATA: true")
        run_ingestion_job()
    else:
        logger.info("USE_MOCK_DATA: false")
        logger.info("Scheduler wird eingerichtet...")
    
        # Führe den Job alle 15 Minuten aus
        schedule.every(15).minutes.do(run_ingestion_job)
    
         # Führe den Job einmal sofort beim Start aus
        logger.info("Führe initialen Ingestion-Job aus...")
        run_ingestion_job()

        logger.info("Scheduler gestartet. Warte auf den nächsten Durchlauf...")
        while True:
           schedule.run_pending()
           time.sleep(1)

# Führe main() nur aus, wenn das Skript direkt ausgeführt wird
# (nicht wenn es als Modul importiert wird)
if __name__ == "__main__":
    main() 