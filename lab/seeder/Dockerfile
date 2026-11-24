FROM python:3.12.8-slim

RUN useradd -m -u 10001 appuser && \
    pip install --no-cache-dir --only-binary=:all: duckdb==1.1.3 "psycopg[binary]==3.2.3" && \
    python -c "import duckdb; duckdb.sql('INSTALL tpch; LOAD tpch;')" && \
    cp -r /root/.duckdb /home/appuser/.duckdb 2>/dev/null || true && \
    chown -R appuser:appuser /home/appuser

WORKDIR /app
COPY --chown=appuser:appuser seed_tpch.py /app/seed_tpch.py

USER appuser
ENTRYPOINT ["python", "-u", "/app/seed_tpch.py"]
