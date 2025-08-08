from airflow.decorators import dag, task
from datetime import datetime, timedelta

import logging
from src.ingestion.extract import DataExtractor, read_tickers_from_file
from src.ingestion.ingest import DataIngestor
from src.common.logger_config import setup_logging
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

            # Calculate end_date based on Airflow's data_interval_end
            # The end date for data extraction is the data_interval_end, which represents the date for which the DAG run is processing data.
            end_date_dt = data_interval_end
            end_date = end_date_dt.strftime("%Y-%m-%d")

            start_date = None
            if latest_timestamp:
                # Start fetching from the day after the last recorded timestamp
                start_date_dt = latest_timestamp + timedelta(days=1)
                # Make start_date_dt timezone-aware using the timezone from data_interval_end
                if start_date_dt.tzinfo is None and end_date_dt.tzinfo is not None:
                    start_date_dt = start_date_dt.replace(tzinfo=end_date_dt.tzinfo)

                # Ensure start_date is not after end_date
                if start_date_dt > end_date_dt:
                    logger.info(
                        f"Data for {symbol} is already up to date for {end_date_dt.strftime('%Y-%m-%d')}. Skipping."
                    )
                    continue  # Skip to the next symbol

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
                # Save into raw_market_data and market_data directly
                data_ingestor.save_both_tables(raw_data)
            # Small pause between symbols to ease rate limits (also helpful for Stooq/other sources)
            time.sleep(random.uniform(0.8, 1.6))
        return symbols  # Return symbols as metadata for downstream tasks

    symbols_fetched = extract()


daily_market_insert_dag = daily_market_insert()
