import logging
from typing import List, Optional

import numpy as np
import pandas as pd

from src.common.logger_config import setup_logging


setup_logging()
logger = logging.getLogger(__name__)


class ValidationError(Exception):
    pass


class DataValidator:
    REQUIRED_MARKET_COLUMNS: List[str] = [
        "symbol",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]

    @staticmethod
    def normalize_raw_market_dataframe(
        df_in: Optional[pd.DataFrame], symbol: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        if df_in is None or len(df_in) == 0:
            return df_in

        df_out = df_in.rename(
            columns={
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            }
        ).copy()

        # If index looks like a datetime index from yfinance/history, move it to a column
        if not any(
            c in df_out.columns for c in ["timestamp", "Datetime", "Date", "index"]
        ):
            df_out.reset_index(inplace=True)

        # Normalize time column name
        rename_candidates = {
            "Datetime": "timestamp",
            "Date": "timestamp",
            "index": "timestamp",
        }
        for old, new in rename_candidates.items():
            if old in df_out.columns and new not in df_out.columns:
                df_out.rename(columns={old: new}, inplace=True)

        # Ensure symbol column
        if symbol is not None:
            df_out["symbol"] = symbol

        # Keep only the standard columns if present
        keep_cols = [
            c for c in DataValidator.REQUIRED_MARKET_COLUMNS if c in df_out.columns
        ]
        if keep_cols:
            df_out = df_out[keep_cols]

        return df_out

    @staticmethod
    def combine_and_clean(
        frames: List[pd.DataFrame], require_non_empty: bool = True
    ) -> pd.DataFrame:
        if frames is None or len(frames) == 0:
            return pd.DataFrame(columns=DataValidator.REQUIRED_MARKET_COLUMNS)

        combined = pd.concat(frames, ignore_index=True)
        combined = DataValidator.enforce_market_schema(combined)
        combined = DataValidator.deduplicate_market_dataframe(combined)
        combined = DataValidator.ensure_sorted_by_timestamp(combined)
        DataValidator.validate_market_dataframe(
            combined, require_non_empty=require_non_empty
        )
        return combined

    @staticmethod
    def to_python_scalar(value):
        if hasattr(value, "to_pydatetime"):
            try:
                return value.to_pydatetime()
            except Exception:
                pass
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                pass
        return value

    @staticmethod
    def enforce_market_schema(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        if df is None or df.empty:
            return df

        df_out = df.copy()

        # Ensure required columns exist before casting
        missing = [
            c for c in DataValidator.REQUIRED_MARKET_COLUMNS if c not in df_out.columns
        ]
        if missing:
            # Try to be forgiving if common alternative names exist
            rename_map = {}
            if "Datetime" in df_out.columns:
                rename_map["Datetime"] = "timestamp"
            if "Date" in df_out.columns:
                rename_map["Date"] = "timestamp"
            if rename_map:
                df_out = df_out.rename(columns=rename_map)
            missing = [
                c
                for c in DataValidator.REQUIRED_MARKET_COLUMNS
                if c not in df_out.columns
            ]
        if missing:
            raise ValidationError(f"Missing required columns: {missing}")

        # Cast dtypes
        df_out["timestamp"] = pd.to_datetime(
            df_out["timestamp"], errors="coerce", utc=False
        )
        df_out["symbol"] = df_out["symbol"].astype(str).str.strip()

        numeric_cols = ["open", "high", "low", "close", "volume"]
        for col in numeric_cols:
            df_out[col] = pd.to_numeric(df_out[col], errors="coerce")

        # Basic cleanup: drop rows where key fields are NA after coercion
        df_out = df_out.dropna(
            subset=["symbol", "timestamp", "open", "high", "low", "close", "volume"]
        )

        # Normalize volume to integer if possible
        try:
            df_out["volume"] = df_out["volume"].round(0).astype(np.int64)
        except Exception:
            # keep as float if casting fails
            pass

        # Reorder columns
        df_out = df_out[DataValidator.REQUIRED_MARKET_COLUMNS]
        return df_out

    @staticmethod
    def deduplicate_market_dataframe(
        df: Optional[pd.DataFrame],
    ) -> Optional[pd.DataFrame]:
        if df is None or df.empty:
            return df
        df_out = df.drop_duplicates(subset=["symbol", "timestamp"]).copy()
        return df_out

    @staticmethod
    def ensure_sorted_by_timestamp(
        df: Optional[pd.DataFrame],
    ) -> Optional[pd.DataFrame]:
        if df is None or df.empty:
            return df
        return df.sort_values(by=["symbol", "timestamp"]).reset_index(drop=True)

    @staticmethod
    def validate_market_dataframe(
        df: Optional[pd.DataFrame], require_non_empty: bool = True
    ) -> None:
        if df is None:
            if require_non_empty:
                raise ValidationError("DataFrame is None")
            return
        if df.empty and require_non_empty:
            raise ValidationError("DataFrame is empty")

        # Columns
        missing = [
            c for c in DataValidator.REQUIRED_MARKET_COLUMNS if c not in df.columns
        ]
        if missing:
            raise ValidationError(f"Missing required columns: {missing}")

        # Types and values
        if not np.issubdtype(df["timestamp"].dtype, np.datetime64):
            raise ValidationError("timestamp column must be datetime64")

        numeric_cols = ["open", "high", "low", "close", "volume"]
        for col in numeric_cols:
            if not np.issubdtype(df[col].dtype, np.number):
                raise ValidationError(f"{col} must be numeric")

        # Value checks
        if (
            (df["open"] < 0).any()
            or (df["high"] < 0).any()
            or (df["low"] < 0).any()
            or (df["close"] < 0).any()
        ):
            raise ValidationError("Price columns contain negative values")
        if (df["volume"] < 0).any():
            raise ValidationError("volume contains negative values")

        # Key uniqueness check per symbol+timestamp
        dup_mask = df.duplicated(subset=["symbol", "timestamp"], keep=False)
        if dup_mask.any():
            dups = df.loc[dup_mask, ["symbol", "timestamp"]].head(5).to_dict("records")
            raise ValidationError(
                f"Duplicate (symbol,timestamp) rows found, e.g. {dups}"
            )

    @staticmethod
    def validate_time_window(
        df: Optional[pd.DataFrame],
        start: Optional[pd.Timestamp],
        end: Optional[pd.Timestamp],
    ) -> None:
        if df is None or df.empty or start is None or end is None:
            return
        if df["timestamp"].min() < start or df["timestamp"].max() > end:
            raise ValidationError("Rows fall outside the requested time window")
