FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Copy minimal files first for better layer caching, then install package
COPY pyproject.toml /tmp/backend/pyproject.toml
COPY tradingview /tmp/backend/tradingview
RUN pip install --no-compile --prefer-binary /tmp/backend[ml]


