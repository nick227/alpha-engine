FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    INTERNAL_READ_HOST=0.0.0.0 \
    PORT=8090 \
    ALPHA_DB_PATH=/data/alpha.db \
    ALLOW_LIVE_TRADING=false

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Ensure data and logs directories exist for volume mounting
RUN mkdir -p /data /app/logs /app/reports

EXPOSE 8090

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://127.0.0.1:${PORT:-8090}/health || exit 1

CMD ["python", "-m", "app.internal_read_v1"]
