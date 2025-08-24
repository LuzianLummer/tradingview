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
    schedule_interval="50 23 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["market_data", "extraction", "daily"],
)
def daily_market_insert():
    """This DAG extracts daily market data and saves it as raw data into the database."""

    @task()
    def extract(data_interval_end: datetime):
        setup_logging()
        logger = logging.getLogger(__name__)
        data_extractor = DataExtractor()
        data_ingestor = DataIngestor()

        symbols = read_tickers_from_file()
        if not symbols:
            logger.warning("No symbols found in tickers.txt, skipping data extraction.")
            return None

        for symbol in symbols:
            latest_timestamp = data_ingestor.get_latest_timestamp_from_db(symbol)

            end_date_dt = data_interval_end
            end_date = end_date_dt.strftime("%Y-%m-%d")

            start_date = None
            if latest_timestamp:
                start_date_dt = latest_timestamp + timedelta(days=1)
                if start_date_dt.tzinfo is None and end_date_dt.tzinfo is not None:
                    start_date_dt = start_date_dt.replace(tzinfo=end_date_dt.tzinfo)

                if start_date_dt > end_date_dt:
                    logger.info(
                        f"Data for {symbol} is already up to date for {end_date_dt.strftime('%Y-%m-%d')}. Skipping."
                    )
                    continue

                start_date = start_date_dt.strftime("%Y-%m-%d")
                logger.info(
                    f"Fetching data for {symbol} starting from {start_date} up to {end_date}"
                )
            else:
                logger.info(
                    f"No existing data for {symbol}, fetching data up to {end_date}."
                )

            raw_data = data_extractor.fetch_market_data(
                symbol=symbol, start=start_date, end=end_date, interval="1d"
            )
            if raw_data is not None:
                data_ingestor.save_both_tables(raw_data)
            time.sleep(random.uniform(0.8, 1.6))
        return symbols

    symbols_fetched = extract()


daily_market_insert_dag = daily_market_insert()


