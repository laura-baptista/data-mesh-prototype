FROM flink:1.18.0-scala_2.12

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

# Diretório para JARs (lib/ do Flink já está no CLASSPATH)
WORKDIR /opt/flink/lib

# --- Dependências Principais ---
# 1. Conector Apache Kafka
# Flink-SQL-Connector-Kafka (para usar 'connector' = 'kafka' nas tabelas)
# Versão 1.18
RUN wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.18/flink-sql-connector-kafka-3.2.0-1.18.jar

# 2. Conector Iceberg
# flink-runtime, flink-table (para a integração com Flink)
# iceberg-flink-runtime, iceberg-catalog-glue (para o catálogo AWS Glue e I/O de arquivos S3)
# Versão 1.1.0 do Iceberg (compatível com Flink 1.18)
# Certifique-se de que a versão do Iceberg seja compatível com a versão do Flink.
RUN wget https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/1.9.0/iceberg-flink-runtime-1.18-1.9.0.jar && \
    wget https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.9.1/iceberg-aws-bundle-1.9.1.jar 
    # && \
    # wget https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-bundled-guava/1.1.0/iceberg-bundled-guava-1.1.0.jar

# 3. SUPORTE CRÍTICO DO HADOOP (S3 e Core)
# O Iceberg precisa desses JARs para inicializar a configuração.
RUN wget https://repo1.maven.org/maven2/org/apache/commons/commons-configuration2/2.1.1/commons-configuration2-2.1.1.jar && \
wget https://repo1.maven.org/maven2/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar && \
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/3.3.4/hadoop-auth-3.3.4.jar && \
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar && \
wget https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.1.1/hadoop-shaded-guava-1.1.1.jar && \
wget https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar && \
wget https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/5.3.0/woodstox-core-5.3.0.jar && \
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.3.4/hadoop-hdfs-client-3.3.4.jar && \
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/3.3.4/hadoop-mapreduce-client-core-3.3.4.jar


WORKDIR /opt/flink-apps

USER flink