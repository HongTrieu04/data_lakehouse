FROM apache/spark:3.5.1

USER root

# Download Apache Iceberg Spark Runtime, Hadoop AWS, AWS SDK bundle, and Oracle JDBC driver JARs
RUN curl -fSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -fSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fSL https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/19.3.0.0/ojdbc8-19.3.0.0.jar -o /opt/spark/jars/ojdbc8-19.3.0.0.jar

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY conf/log4j2.properties /opt/spark/conf/log4j2.properties

USER 1001
