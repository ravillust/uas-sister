# Publisher service image
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN useradd --create-home --uid 10002 appuser
USER appuser

# Default: jalankan generator. Bisa di-override saat 'docker compose run'.
CMD ["python", "publisher.py"]
