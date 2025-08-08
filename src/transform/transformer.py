import logging
from datetime import datetime, timedelta
from typing import List

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import LabelEncoder

from src.common.logger_config import setup_logging
from src.ingestion.database import DatabaseManager
from src.ingestion.models import MarketData


setup_logging()
logger = logging.getLogger(__name__)


class DataTransformer:
    def __init__(self, db_url: str):
        DatabaseManager.initialize(db_url)

    def fetch_data_from_db(self, symbol: str, start_date: datetime, end_date: datetime):
        try:
            with DatabaseManager.get_session() as session:
                data = (
                    session.query(MarketData)
                    .filter(
                        MarketData.symbol == symbol,
                        MarketData.timestamp >= start_date,
                        MarketData.timestamp <= end_date,
                    )
                    .order_by(MarketData.timestamp)
                    .all()
                )
                logger.info(
                    f"Fetched {len(data)} records for {symbol} from {start_date} to {end_date}."
                )
                return data
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return []

    def calculate_daily_returns(self, data: List[MarketData]):
        returns = []
        for i in range(1, len(data)):
            current_close = data[i].close
            previous_close = data[i - 1].close
            if previous_close != 0:
                returns.append((current_close - previous_close) / previous_close)
            else:
                returns.append(0.0)
        return returns

    def classify_market_data(self, daily_returns: List[float]):
        states = []
        for r in daily_returns:
            if r > 0.005:
                states.append("Strong Up")
            elif r > 0.001:
                states.append("Slight Up")
            elif r < -0.005:
                states.append("Strong Down")
            elif r < -0.001:
                states.append("Slight Down")
            else:
                states.append("Stable")
        return states

    def update_market_data_with_states(
        self, market_data_records: List[MarketData], states: List[str]
    ):
        try:
            num_records = len(market_data_records)
            num_states = len(states)
            limit = min(num_records - 1, num_states)

            updates = []
            for i in range(1, limit + 1):
                record = market_data_records[i]
                updates.append({"id": record.id, "market_state": states[i - 1]})

            if not updates:
                logger.info("No market data records to update with states.")
                return

            with DatabaseManager.get_session() as session:
                session.bulk_update_mappings(MarketData, updates)
                logger.info(
                    f"Updated {len(updates)} market data records with classified states."
                )
        except Exception as e:
            logger.error(f"Error updating market data with states: {e}")

    def prepare_features_and_target(self, market_data: List[MarketData]):
        features = []
        targets = []
        if len(market_data) < 2:
            return np.array(features), np.array(targets), None

        df = pd.DataFrame(
            [
                {
                    "symbol": md.symbol,
                    "timestamp": md.timestamp,
                    "open": md.open,
                    "high": md.high,
                    "low": md.low,
                    "close": md.close,
                    "volume": md.volume,
                    "market_state": md.market_state,
                }
                for md in market_data
            ]
        )
        df["daily_return"] = df["close"].pct_change().fillna(0)
        df = df.dropna(subset=["market_state"])
        if len(df) < 2:
            return np.array(features), np.array(targets), None

        X_list = []
        y_list = []
        for i in range(len(df) - 1):
            current_row = df.iloc[i]
            next_row = df.iloc[i + 1]
            X_list.append(
                [
                    current_row["open"],
                    current_row["high"],
                    current_row["low"],
                    current_row["close"],
                    current_row["volume"],
                    current_row["daily_return"],
                    current_row["market_state"],
                ]
            )
            y_list.append(next_row["market_state"])

        all_states = sorted(list(df["market_state"].unique()))
        label_encoder = LabelEncoder()
        label_encoder.fit(all_states)

        X_encoded_states = []
        for row_features in X_list:
            encoded_state = label_encoder.transform([row_features[-1]])[0]
            X_encoded_states.append(row_features[:-1] + [encoded_state])
        y_encoded_targets = label_encoder.transform(y_list)

        return np.array(X_encoded_states), np.array(y_encoded_targets), label_encoder

    def train_transition_model(self, X, y):
        if len(X) == 0 or len(y) == 0:
            logger.warning("No data to train the transition model.")
            return None
        model = LogisticRegression(max_iter=1000, solver="liblinear")
        model.fit(X, y)
        logger.info("Logistic Regression model trained successfully.")
        return model

    def calculate_transition_matrix(
        self, model, label_encoder, market_data: List[MarketData]
    ):
        unique_states = label_encoder.classes_
        num_states = len(unique_states)
        state_to_index = {state: i for i, state in enumerate(unique_states)}

        summed_probabilities = {state: np.zeros(num_states) for state in unique_states}
        state_counts = {state: 0 for state in unique_states}

        df = pd.DataFrame(
            [
                {
                    "symbol": md.symbol,
                    "timestamp": md.timestamp,
                    "open": md.open,
                    "high": md.high,
                    "low": md.low,
                    "close": md.close,
                    "volume": md.volume,
                    "market_state": md.market_state,
                }
                for md in market_data
            ]
        )
        df["daily_return"] = df["close"].pct_change().fillna(0)
        df = df.dropna(subset=["market_state"])
        if len(df) < 1:
            return {"matrix": [], "states": unique_states.tolist()}

        for i in range(len(df) - 1):
            current_row = df.iloc[i]
            current_state_str = current_row["market_state"]
            encoded_current_state = label_encoder.transform([current_state_str])[0]
            features_for_prediction = np.array(
                [
                    current_row["open"],
                    current_row["high"],
                    current_row["low"],
                    current_row["close"],
                    current_row["volume"],
                    current_row["daily_return"],
                    encoded_current_state,
                ]
            ).reshape(1, -1)
            predicted_probs = model.predict_proba(features_for_prediction)[0]
            summed_probabilities[current_state_str] += predicted_probs
            state_counts[current_state_str] += 1

        transition_matrix = np.zeros((num_states, num_states))
        for state_str, index in state_to_index.items():
            if state_counts[state_str] > 0:
                transition_matrix[index, :] = (
                    summed_probabilities[state_str] / state_counts[state_str]
                )
        return {"matrix": transition_matrix.tolist(), "states": unique_states.tolist()}
