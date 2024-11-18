FROM apache/airflow:2.9.3-python3.11

USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

WORKDIR /app

RUN pip install apache-airflow-providers-apache-spark==4.9.0 pyspark==3.5.1