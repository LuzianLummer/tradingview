FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY frontend/requirements.txt /app/requirements.txt
RUN pip install --no-compile --prefer-binary -r /app/requirements.txt

# Install shared package (core only; heavy deps live in backend image)
COPY backend /tmp/backend
RUN pip install --no-compile --prefer-binary /tmp/backend

# App code
COPY frontend /app/frontend
WORKDIR /app/frontend

ENV STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0

EXPOSE 8501

CMD ["streamlit", "run", "streamlit_app.py"]

