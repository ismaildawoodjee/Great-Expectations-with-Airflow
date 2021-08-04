FROM apache/airflow:2.1.2
COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt