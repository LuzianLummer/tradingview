from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from io import StringIO

from ingestion_pipeline.extract import DataExtractor, read_tickers_from_file
from ingestion_pipeline.insert import DataIngestor
from ingestion_pipeline.database_manager import DatabaseManager


@dag(
    schedule_interval="50 23 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=[""],
)
def daily_market_insert():
    """This DAG extracts daily market data and saves it as raw data into the database."""

    @task()
    def extract(data_interval_end: datetime):

        data_extractor = DataExtractor()
        data_ingestor = DataIngestor()

        symbols = read_tickers_from_file()
        if not symbols:
            print("No symbols found in tickers.txt, skipping data extraction.")
            return None

        for symbol in symbols:
            latest_timestamp = data_ingestor.get_latest_timestamp_from_db(symbol)

            # Calculate end_date based on Airflow's data_interval_end
            # This represents the date for which the DAG run is processing data (e.g., for a run on July 31st, it processes July 30th's data)
            end_date_dt = data_interval_end - timedelta(days=1)
            end_date = end_date_dt.strftime("%Y-%m-%d")

            start_date = None
            if latest_timestamp:
                # Start fetching from the day after the last recorded timestamp
                start_date_dt = latest_timestamp + timedelta(days=1)

                # Ensure start_date is not after end_date
                if start_date_dt > end_date_dt:
                    print(f'Data for {symbol} is already up to date for {end_date_dt.strftime("%Y-%m-%d")}. Skipping.')
                    continue  # Skip to the next symbol

                start_date = start_date_dt.strftime("%Y-%m-%d")
                print(f"Fetching data for {symbol} starting from {start_date} up to {end_date}")
            else:
                print(f"No existing data for {symbol}, fetching data up to {end_date}.")

            raw_data = data_extractor.fetch_market_data(
                symbol=symbol, start=start_date, end=end_date, interval="1d"
            )
            if raw_data is not None:
                data_ingestor.save_raw_market_data(raw_data)
        return symbols  # Return symbols as metadata for downstream tasks

    @task()
    def load(symbols: list):
        """
        #### Load task
        A simple Load task which takes the consolidated data and loads it into
        a target database.
        """
        if not symbols:
            print("No transformed symbols to process in load task.")
            return
        print(f"Successfully processed and loaded data for symbols: {symbols}")

    symbols_fetched = extract()
    load(symbols_fetched)


daily_market_insert_dag = daily_market_insert()
