FROM apache/spark:3.5.1

USER root

# ===========================
# Python deps
# ===========================
COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ===========================
# Remove JARs conflitantes
# ===========================
RUN rm -f /opt/spark/jars/hadoop-aws*.jar \
    /opt/spark/jars/aws-java-sdk* \
    /opt/spark/jars/httpcore*.jar \
    /opt/spark/jars/httpclient*.jar

# ===========================
# Instala Kafka (Spark 3.5.1)
# ===========================
RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o /opt/spark/jars/kafka-clients-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar

# ===========================
# Hadoop AWS + AWS SDK compatíveis
# ===========================

# Hadoop AWS 3.3.4
RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# AWS SDK 1.12.x (compatível com Hadoop 3.3.x)
RUN curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.661.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.661/aws-java-sdk-bundle-1.12.661.jar

# Dependências extras do S3A
RUN curl -L -o /opt/spark/jars/httpcore-4.4.16.jar \
    https://repo1.maven.org/maven2/org/apache/httpcomponents/httpcore/4.4.16/httpcore-4.4.16.jar && \
    curl -L -o /opt/spark/jars/httpclient-4.5.13.jar \
    https://repo1.maven.org/maven2/org/apache/httpcomponents/httpclient/4.5.13/httpclient-4.5.13.jar && \
    curl -L -o /opt/spark/jars/jackson-dataformat-cbor-2.17.1.jar \
    https://repo1.maven.org/maven2/com/fasterxml/jackson/dataformat/jackson-dataformat-cbor/2.17.1/jackson-dataformat-cbor-2.17.1.jar

# ===========================
# Permissões e ambiente
# ===========================
RUN mkdir -p /opt/spark-apps && chmod -R 777 /opt/spark-apps
RUN mkdir -p /home/spark/.aws

COPY ./aws-config /home/spark/.aws
RUN chown -R spark:spark /home/spark/.aws

ENV SPARK_DIST_CLASSPATH="/opt/spark/jars/*"
ENV JAVA_TOOL_OPTIONS="-Dcom.amazonaws.services.s3.enableV4=true"

USER spark
WORKDIR /opt/spark-apps
