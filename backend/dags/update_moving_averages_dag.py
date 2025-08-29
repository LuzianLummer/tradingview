from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging

from tradingview.transform.moving_averages import MovingAverageCalculator
from tradingview.common.utilfunctions import read_tickers_from_file
from tradingview.common.logger_config import setup_logging


@dag(
    dag_id="update_moving_averages",
    start_date=datetime(2023, 1, 1),
    schedule_interval="0 2 * * *",  # Run daily at 2 AM
    catchup=False,
    tags=["moving_averages", "technical_indicators"],
)
def update_moving_averages():
    """DAG to calculate and update moving averages for all symbols."""
    
    @task()
    def update_moving_averages_task():
        setup_logging()
        logger = logging.getLogger(__name__)
        
        calculator = MovingAverageCalculator()
        symbols = read_tickers_from_file()
        
        if not symbols:
            logger.warning("No ticker symbols found in the file, skipping moving average updates.")
            return None
        
        results = {}
        for symbol in symbols:
            logger.info(f"Updating moving averages for {symbol}")
            success = calculator.update_moving_averages_in_db(symbol)
            results[symbol] = success
            
            if success:
                logger.info(f"Successfully updated moving averages for {symbol}")
            else:
                logger.error(f"Failed to update moving averages for {symbol}")
        
        return results
    
    update_task = update_moving_averages_task()


update_moving_averages_dag = update_moving_averages()
