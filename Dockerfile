FROM apache/airflow:2.10.3

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt

# QUAN TRỌNG: dùng constraints file khớp đúng version Airflow đang chạy
RUN pip install --no-cache-dir --user \
    -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.12.txt"
