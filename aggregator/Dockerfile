# Aggregator service image
# Base minimal, non-root user (rekomendasi keamanan soal).
FROM python:3.11-slim

# Hindari .pyc & buffering log
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install deps dulu agar layer cache efektif
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Salin kode
COPY . .

# Buat non-root user
RUN useradd --create-home --uid 10001 appuser
USER appuser

EXPOSE 8080

# Jalankan server (BUKAN pytest) sebagai entrypoint
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
