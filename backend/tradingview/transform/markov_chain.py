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
        # Lags in trading days to use as features alongside current daily return
        # 0 represents the current day's return; others are historical lags
        self.lags = [0, 1, 2, 5, 10, 20]

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

    def prepare_features_and_target(self, market_data: List[MarketData]):
   
        features = []
        targets = []
        if len(market_data) < 2:
            return np.array(features), np.array(targets), None

        df = pd.DataFrame([
            {
                "symbol": md.symbol,
                "timestamp": md.timestamp,
                "close": md.close,
                "market_state": md.market_state,
            }
            for md in market_data
        ])

        # Ensure chronological order for correct lag/return computation
        df = df.sort_values("timestamp").reset_index(drop=True)

        # Compute daily returns
        df["daily_return"] = df["close"].pct_change()

        # Build lagged return features for specified horizons
        feature_cols = []
        for lag in self.lags:
            col_name = f"ret_lag_{lag}"
            if lag == 0:
                df[col_name] = df["daily_return"]
            else:
                df[col_name] = df["daily_return"].shift(lag)
            feature_cols.append(col_name)

        # Target is the next day's market state
        df["target_state"] = df["market_state"].shift(-1)

        # Drop rows where features or target are missing
        model_df = df.dropna(subset=feature_cols + ["target_state"])  # ensures sufficient history and next state
        if len(model_df) == 0:
            return np.array(features), np.array(targets), None

        X = model_df[feature_cols].to_numpy()
        label_encoder = LabelEncoder()
        y = label_encoder.fit_transform(model_df["target_state"].to_numpy())

        return X, y, label_encoder

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

        # Vectorized approach below, no per-state accumulators needed

        df = pd.DataFrame([
            {
                "symbol": md.symbol,
                "timestamp": md.timestamp,
                "close": md.close,
                "market_state": md.market_state,
            }
            for md in market_data
        ])
        df = df.sort_values("timestamp").reset_index(drop=True)
        df["daily_return"] = df["close"].pct_change()

        feature_cols = []
        for lag in self.lags:
            col_name = f"ret_lag_{lag}"
            if lag == 0:
                df[col_name] = df["daily_return"]
            else:
                df[col_name] = df["daily_return"].shift(lag)
            feature_cols.append(col_name)

        df = df.dropna(subset=["market_state"] + feature_cols)
        if len(df) < 1:
            return {"matrix": [], "states": unique_states.tolist()}

        # Predict probabilities for all rows at once
        probs = model.predict_proba(df[feature_cols].to_numpy(dtype=float))  # shape: (N, num_states)
        probs_df = pd.DataFrame(probs, columns=unique_states)
        probs_df["current_state"] = df["market_state"].values

        # Average predicted next-state probabilities conditioned on current state
        grouped_means = probs_df.groupby("current_state")[list(unique_states)].mean()

        # Build transition matrix in the fixed order of unique_states
        transition_matrix = np.zeros((num_states, num_states))
        for state_str, index in state_to_index.items():
            if state_str in grouped_means.index:
                transition_matrix[index, :] = grouped_means.loc[state_str].to_numpy()
            # else leave zeros if this current state did not occur

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


