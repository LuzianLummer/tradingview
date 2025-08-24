FROM python:3.11-slim

WORKDIR /app

COPY frontend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Install shared package
COPY backend /tmp/backend
RUN pip install --no-cache-dir /tmp/backend

# App code
COPY frontend /app/frontend
WORKDIR /app/frontend

ENV STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0

EXPOSE 8501

CMD ["streamlit", "run", "streamlit_app.py"]

