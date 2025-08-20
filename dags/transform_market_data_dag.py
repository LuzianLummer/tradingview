from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta

import logging
from src.transform.transformer import DataTransformer
from src.ingestion.database import DatabaseManager
from src.common.utilfunctions import read_tickers_from_file
from src.common.logger_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


@dag(
    schedule_interval="0 0 * * *",  # Run daily at midnight
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["transform", "market_data"],
)
def transform_market_data():
    """This DAG transforms raw market data into classified states using a Markov chain approach."""

    @task()
    def transform_data(data_interval_end: datetime):
        db_url = DatabaseManager.get_db_url()
        data_transformer = DataTransformer(db_url)

        symbols = read_tickers_from_file()
        if not symbols:
            logger.warning(
                "No symbols found in tickers.txt, skipping data transformation."
            )
            return

        for symbol in symbols:
            # Fetch data for the last 30 days to classify
            end_date = data_interval_end
            start_date = end_date - timedelta(
                days=30
            )  # Adjust as needed for your model

            logger.info(
                f"Fetching data for {symbol} from {start_date} to {end_date} for transformation."
            )
            market_data = data_transformer.fetch_data_from_db(
                symbol, start_date, end_date
            )

            if market_data:
                daily_returns = data_transformer.calculate_daily_returns(market_data)
                states = data_transformer.classify_market_data(daily_returns)

                # Ensure market_data objects have their states updated before preparing features
                data_transformer.update_market_data_with_states(market_data, states)

                X, y, label_encoder = data_transformer.prepare_features_and_target(
                    market_data
                )

                if X.size > 0 and y.size > 0 and label_encoder is not None:
                    trained_model = data_transformer.train_transition_model(X, y)

                    if trained_model:
                        transition_matrix_result = (
                            data_transformer.calculate_transition_matrix(
                                trained_model, label_encoder, market_data
                            )
                        )
                        logger.info(
                            f"Transition Matrix for {symbol}: {transition_matrix_result['matrix']}"
                        )
                        logger.info(
                            f"States for matrix: {transition_matrix_result['states']}"
                        )
                    else:
                        logger.warning(
                            f"Could not train model for {symbol}. Skipping transition matrix calculation."
                        )
                else:
                    logger.warning(
                        f"Not enough data or states for {symbol} to prepare features and target. Skipping model training and transition matrix calculation."
                    )

                logger.info(f"States for {symbol}: {states[-5:]} (last 5)")
                # TODO: Implement saving of transformed data or further processing for ML regression
            else:
                logger.info(
                    f"No market data found for {symbol} within the specified range for transformation."
                )

    transform_data_task = transform_data()


transform_market_data_dag = transform_market_data()
