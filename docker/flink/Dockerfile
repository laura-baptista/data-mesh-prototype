FROM flink:1.20.0-scala_2.12

USER root

# ===========================
# Python deps
# ===========================
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean

# Create symlink python -> python3 if not exists 
RUN if [ ! -e /usr/bin/python ]; then ln -s /usr/bin/python3 /usr/bin/python; fi

WORKDIR /opt/flink-apps

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# ----------------------------------------------------------------------
# 4. Copiar as credenciais AWS para o local correto (/root/.aws/)
# ----------------------------------------------------------------------
RUN mkdir -p /root/.aws
COPY aws-config/credentials /root/.aws/credentials

# Importante para o boto3 / AWS SDK funcionar
RUN chmod 600 /root/.aws/credentials

# Diretório de libs extras
RUN mkdir -p /opt/flink/custom-jars

# ---------- S3 + Hadoop ----------
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar -P /opt/flink/custom-jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/3.3.4/hadoop-auth-3.3.4.jar -P /opt/flink/custom-jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.3.4/hadoop-hdfs-client-3.3.4.jar -P /opt/flink/custom-jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P /opt/flink/custom-jars && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.760/aws-java-sdk-bundle-1.12.760.jar -P /opt/flink/custom-jars

# --------- S3 filesystem connector (Flink 1.20) ----------
RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.20.0/flink-s3-fs-hadoop-1.20.0.jar -P /opt/flink/custom-jars

# --------- Kafka SQL Connector (Flink 1.20) ----------
RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar -P /opt/flink/custom-jars

# # Parquet format para Flink 1.20.0
# RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-parquet/1.20.0/flink-parquet-1.20.0.jar -P /opt/flink/custom-jars


# # Parquet core libs
# RUN wget -q https://repo1.maven.org/maven2/org/apache/parquet/parquet-common/1.13.1/parquet-common-1.13.1.jar -P /opt/flink/custom-jars \
#     && wget -q https://repo1.maven.org/maven2/org/apache/parquet/parquet-column/1.13.1/parquet-column-1.13.1.jar -P /opt/flink/custom-jars \
#     && wget -q https://repo1.maven.org/maven2/org/apache/parquet/parquet-hadoop/1.13.1/parquet-hadoop-1.13.1.jar -P /opt/flink/custom-jars \
#     && wget -q https://repo1.maven.org/maven2/org/apache/parquet/parquet-encoding/1.13.1/parquet-encoding-1.13.1.jar -P /opt/flink/custom-jars \
#     && wget -q https://repo1.maven.org/maven2/org/apache/parquet/parquet-format-structures/1.13.1/parquet-format-structures-1.13.1.jar -P /opt/flink/custom-jars \
#     && wget -q https://repo1.maven.org/maven2/org/apache/parquet/parquet-format/2.9.0/parquet-format-2.9.0.jar -P /opt/flink/custom-jars

# Copia os novos jars sem remover os originais
RUN cp /opt/flink/custom-jars/*.jar /opt/flink/lib/


USER flink
