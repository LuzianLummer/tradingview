# Nutze ein schlankes Python-Image als Basis
FROM python:3.11-slim

# Setze das Arbeitsverzeichnis im Container
WORKDIR /app

# Kopiere die requirements.txt und installiere die Abhängigkeiten
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Kopiere den Rest des Codes ins Arbeitsverzeichnis
COPY . .

# Setze die Umgebungsvariable für Python (optional, für saubere Logs)
ENV PYTHONUNBUFFERED=1