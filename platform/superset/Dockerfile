FROM apache/superset:4.1.1
USER root
RUN pip install --no-cache-dir psycopg2-binary==2.9.10
COPY superset_config.py /app/pythonpath/superset_config.py
COPY bootstrap_dashboard.py /app/pythonpath/bootstrap_dashboard.py
USER superset
