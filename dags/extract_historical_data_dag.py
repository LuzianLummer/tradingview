from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime
import json
import pandas as pd
from io import StringIO

from ingestion_pipeline.extract import DataExtractor, read_tickers_from_file
from ingestion_pipeline.insert import DataIngestor
from ingestion_pipeline.database_manager import DatabaseManager


@dag(
    dag_id="extract_historical_market_data",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["market_data", "extraction", "historical"],
)
def historical_market_insert():
    @task()
    def extract():
        data_extractor = DataExtractor()
        data_ingestor = DataIngestor()
        symbols = read_tickers_from_file()
        if not symbols:
            print("No ticker symbols found in the file, skipping data extraction.")
            return None

        for symbol in symbols:
            raw_data = data_extractor.fetch_market_data(
                symbol=symbol, period="max", interval="1d"
            )
            if raw_data is not None:
                data_ingestor.save_raw_market_data(raw_data)
        return symbols

    @task()
    def load(transformed_symbols: list):
        if not transformed_symbols:
            print("No transformed symbols to process in load task.")
            return
        print(
            f"Successfully processed and loaded data for symbols: {transformed_symbols}"
        )

    symbols_fetched = extract()
    load(symbols_fetched)


historical_market_insert_dag = historical_market_insert()
