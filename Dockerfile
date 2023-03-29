FROM apache/airflow:2.4.1
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --user -r /tmp/requirements.txt
RUN pip install psycopg2