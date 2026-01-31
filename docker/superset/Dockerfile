FROM apache/superset:3.1.0

USER root

RUN pip install --no-cache-dir \
    duckdb-engine>=0.11 \
    duckdb>=0.10 \
    psycopg2-binary>=2.9

COPY superset_config.py /app/superset_config.py
ENV SUPERSET_CONFIG_PATH=/app/superset_config.py

USER superset

HEALTHCHECK CMD ["wget", "-q", "-O", "/dev/null", "http://localhost:8088/health"]

ENTRYPOINT ["/bin/bash", "-c"]
CMD ["superset db upgrade && superset fab create-admin --username admin --firstname admin --lastname admin --email admin@example.com --password admin || true && superset init && superset run -h 0.0.0.0 -p 8088"]
