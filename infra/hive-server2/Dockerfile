FROM apache/hive:3.1.3

USER root

# Install wget and curl
RUN apt-get update && apt-get install -y --no-install-recommends wget curl && rm -rf /var/lib/apt/lists/*

# Install PostgreSQL JDBC driver for Hive connection
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.7.2.jar -O /opt/hive/lib/postgresql-42.7.2.jar

# Install Hadoop AWS and AWS SDK bundle compatible with Hive 3.1.3 (Hadoop 3.1.x)
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.1/hadoop-aws-3.1.1.jar -O /opt/hive/lib/hadoop-aws-3.1.1.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar -O /opt/hive/lib/aws-java-sdk-bundle-1.11.271.jar

# Remove any incompatible hadoop-aws versions
RUN rm -f /opt/hive/lib/hadoop-aws-3.3.4.jar /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar 2>/dev/null || true

COPY conf/hive-site.xml /opt/hive/conf/hive-site.xml

EXPOSE 10000 10002

CMD ["/opt/hive/bin/hive", "--service", "hiveserver2"]
