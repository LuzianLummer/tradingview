FROM python:3.11-slim

WORKDIR /app

# Install runtime deps (kept for CLI/tools if any)
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install shared package
COPY . /tmp/backend
RUN pip install --no-cache-dir /tmp/backend

ENV PYTHONUNBUFFERED=1


