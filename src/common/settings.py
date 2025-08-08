import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DatabaseSettings:
    user: str = os.getenv("POSTGRES_USER", "tradingview")
    password: str = os.getenv("POSTGRES_PASSWORD", "tradingview")
    host: str = os.getenv("POSTGRES_HOST", "postgres")
    db: str = os.getenv("POSTGRES_DB", "tradingview")

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}/{self.db}"
        )


@dataclass(frozen=True)
class AppSettings:
    log_file: str = os.getenv("LOG_FILE", "app.log")
    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()


db_settings = DatabaseSettings()
app_settings = AppSettings()
