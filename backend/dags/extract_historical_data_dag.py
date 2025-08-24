from airflow.decorators import dag, task
from datetime import datetime, timedelta

import logging
from tradingview.ingestion.extract import DataExtractor
from tradingview.common.utilfunctions import read_tickers_from_file
from tradingview.ingestion.ingest import DataIngestor
from tradingview.common.logger_config import setup_logging
import time
import random


@dag(
    dag_id="extract_historical_market_data",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["market_data", "extraction", "historical"],
)
def historical_market_insert():
    @task()
    def extract(data_interval_end: datetime):
        setup_logging()
        logger = logging.getLogger(__name__)
        data_extractor = DataExtractor()
        data_ingestor = DataIngestor()
        symbols = read_tickers_from_file()
        if not symbols:
            logger.warning(
                "No ticker symbols found in the file, skipping data extraction."
            )
            return None

        end_date = data_interval_end.strftime("%Y-%m-%d")
        for symbol in symbols:
            latest_timestamp = data_ingestor.get_latest_timestamp_from_db(symbol)

            start_date = None
            if latest_timestamp:
                start_dt = latest_timestamp + timedelta(days=1)
                if start_dt.tzinfo is None and data_interval_end.tzinfo is not None:
                    start_dt = start_dt.replace(tzinfo=data_interval_end.tzinfo)
                if start_dt > data_interval_end:
                    logger.info(
                        f"Data for {symbol} already up to date for {end_date}. Skipping."
                    )
                    continue
                start_date = start_dt.strftime("%Y-%m-%d")
            else:
                start_date = "1980-01-01"

            raw_data = data_extractor.fetch_market_data(
                symbol=symbol, start=start_date, end=end_date, interval="1d"
            )
            if raw_data is not None:
                data_ingestor.save_both_tables(raw_data)
            time.sleep(random.uniform(0.8, 1.6))
        return symbols

    extract_task = extract()


historical_market_insert_dag = historical_market_insert()


