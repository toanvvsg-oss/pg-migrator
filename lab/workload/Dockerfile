FROM python:3.12.8-slim

RUN useradd -m -u 10001 appuser && \
    pip install --no-cache-dir --only-binary=:all: "psycopg[binary]==3.2.3"

WORKDIR /app
COPY --chown=appuser:appuser workload.py /app/workload.py

USER appuser
EXPOSE 8092

CMD ["python", "-u", "/app/workload.py"]
