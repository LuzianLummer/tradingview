# Nutze ein schlankes Python-Image als Basis
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy project code
COPY src ./src
COPY dags ./dags

# Helpful for unbuffered logs
ENV PYTHONUNBUFFERED=1