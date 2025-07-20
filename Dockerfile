FROM apache/airflow:2.7.2

# Installer Java (nécessaire pour Spark)
USER root
RUN apt-get update && apt-get install -y openjdk-11-jdk && apt-get clean

# Passer à l'utilisateur airflow pour installer pyspark via pip
USER airflow

RUN pip install --upgrade pip

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

