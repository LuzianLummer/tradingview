import logging
from typing import List, Optional

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import LabelEncoder

from tradingview.common.logger_config import setup_logging
from tradingview.ingestion.models import MarketData

setup_logging()
logger = logging.getLogger(__name__)


class MarkovChainModel:
    def __init__(self):
        self.model = None
        self.label_encoder = None

    def classify_market_data(self, daily_returns: List[float]):
        """Classify market data into states based on daily returns."""
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

    def prepare_features_and_target(self, market_data: List[MarketData]):
        """Prepare features and target for ML model training."""
        features = []
        targets = []
        if len(market_data) < 2:
            return np.array(features), np.array(targets), None

        df = pd.DataFrame([
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
        ])
        df["daily_return"] = df["close"].pct_change().fillna(0)
        df = df.dropna(subset=["market_state"])
        if len(df) < 2:
            return np.array(features), np.array(targets), None

        X_list = []
        y_list = []
        for i in range(len(df) - 1):
            current_row = df.iloc[i]
            next_row = df.iloc[i + 1]
            X_list.append([
                current_row["open"],
                current_row["high"],
                current_row["low"],
                current_row["close"],
                current_row["volume"],
                current_row["daily_return"],
                current_row["market_state"],
            ])
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
        """Train the logistic regression model for state transitions."""
        if len(X) == 0 or len(y) == 0:
            return None
        model = LogisticRegression(max_iter=1000, solver="liblinear")
        model.fit(X, y)
        return model

    def calculate_transition_matrix(self, model, label_encoder, market_data: List[MarketData]):
        """Calculate transition probability matrix for markov chain."""
        unique_states = label_encoder.classes_
        num_states = len(unique_states)
        state_to_index = {state: i for i, state in enumerate(unique_states)}

        summed_probabilities = {state: np.zeros(num_states) for state in unique_states}
        state_counts = {state: 0 for state in unique_states}

        df = pd.DataFrame([
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
        ])
        df["daily_return"] = df["close"].pct_change().fillna(0)
        df = df.dropna(subset=["market_state"])
        if len(df) < 1:
            return {"matrix": [], "states": unique_states.tolist()}

        for i in range(len(df) - 1):
            current_row = df.iloc[i]
            current_state_str = current_row["market_state"]
            encoded_current_state = label_encoder.transform([current_state_str])[0]
            features_for_prediction = np.array([
                current_row["open"],
                current_row["high"],
                current_row["low"],
                current_row["close"],
                current_row["volume"],
                current_row["daily_return"],
                encoded_current_state,
            ]).reshape(1, -1)
            predicted_probs = model.predict_proba(features_for_prediction)[0]
            summed_probabilities[current_state_str] += predicted_probs
            state_counts[current_state_str] += 1

        transition_matrix = np.zeros((num_states, num_states))
        for state_str, index in state_to_index.items():
            if state_counts[state_str] > 0:
                transition_matrix[index, :] = summed_probabilities[state_str] / state_counts[state_str]
        return {"matrix": transition_matrix.tolist(), "states": unique_states.tolist()}

    def fit(self, market_data: List[MarketData]):
        """Fit the markov chain model on market data."""
        X, y, label_encoder = self.prepare_features_and_target(market_data)
        if len(X) == 0:
            logger.warning("No valid data for training markov chain model")
            return False
        
        self.model = self.train_transition_model(X, y)
        self.label_encoder = label_encoder
        return True

    def predict_transition_matrix(self, market_data: List[MarketData]):
        """Predict transition matrix using fitted model."""
        if self.model is None or self.label_encoder is None:
            logger.error("Model not fitted. Call fit() first.")
            return None
        
        return self.calculate_transition_matrix(self.model, self.label_encoder, market_data)
