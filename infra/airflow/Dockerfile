FROM apache/airflow:3.3.0

USER root

# Install OpenJDK 17 for Spark client execution inside Airflow
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps \
        wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Copy requirements and install python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Download required JARs for Oracle JDBC, Iceberg, Hadoop AWS into PySpark jars directory
USER root
RUN PYSPARK_JARS=$(python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") && \
    wget -q https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/19.3.0.0/ojdbc8-19.3.0.0.jar -O ${PYSPARK_JARS}/ojdbc8-19.3.0.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar -O ${PYSPARK_JARS}/iceberg-spark-runtime-3.5_2.12-1.5.2.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -O ${PYSPARK_JARS}/hadoop-aws-3.3.4.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -O ${PYSPARK_JARS}/aws-java-sdk-bundle-1.12.262.jar

USER airflow
