## Trading Data Ingestion & Transformation (Airflow + Postgres)   
## WORK IN PROGRESS

This project ingests market data (yfinance with Stooq fallback), stores both raw and normalized data into Postgres via SQLAlchemy (ORM as the single source of truth), and transforms data into market states using a simple ML step. Airflow orchestrates daily and backfill workflows.

### Features
- Robust extraction with retries, chunking for long ranges, and Stooq CSV fallback
- ORM-managed schema for `raw_market_data` and `market_data`
- Directly writes to both raw and normalized tables in one step
- Airflow DAGs for daily ingestion and transformation
- Centralized logging and environment-driven settings

### Repository Layout
- `backend/src/common`: logging and settings
- `backend/src/ingestion`: models, database manager, extractor, ingestor, `tickers.txt`
- `backend/src/transform`: data transformer and simple ML utilities
- `backend/dags`: Airflow DAGs
- `airflow/`: Airflow Dockerfile and requirements
- `frontend/`: Streamlit app, Dockerfile and requirements

### Prerequisites
- Docker and Docker Compose

### Quickstart
1. Copy environment template and adjust if needed:
   ```bash
   cp env.example .env
   # edit .env (DB creds, Airflow secret, logging)
   ```
2. Build and start services:
   ```bash
   docker-compose up --build
   ```
3. Open Airflow Web UI at `http://localhost:8080` (default user created by init step).
4. Place tickers into `backend/src/common/tickers.txt`.
5. Trigger DAGs: `daily_market_insert` (scheduled) and `transform_market_data`.

### Environment Variables
See `.env.example` for all variables. Key ones:
- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_HOST`
- `AIRFLOW__WEBSERVER__SECRET_KEY`
- `LOG_LEVEL`, `LOG_FILE`

### Development
- Install dev tools: `pip install -r requirements-dev.txt`
- Run linters/formatters: `black`, `ruff`, `isort`, `mypy`
- Tests: `pytest`

### Notes
- ORM is the single source of truth. We do not apply `sql/init.sql`.
- Airflow base image already contains Airflow; additional libs installed via `requirements-airflow.txt`.
- Logging is centralized via `src/common/logger_config.py`.

### License
MIT


