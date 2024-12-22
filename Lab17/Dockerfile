FROM prefecthq/prefect:2.11.5-python3.11

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

RUN pip install prefect>=2.12.0 prefect-shell kafka-python pyspark==3.5.0 delta-spark==3.2.0 pandas==2.0.3 "numpy<2"