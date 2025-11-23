from pyflink.table import EnvironmentSettings, TableEnvironment, StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.checkpoint_config import CheckpointConfig, ExternalizedCheckpointCleanup
from pyflink.java_gateway import get_gateway

def main():
    # ------------------ stream env (optional checkpointing) ------------------
    # Necessário para operações de streaming
    stream_env = StreamExecutionEnvironment.get_execution_environment()
    # Recomendado para desenvolvimento. Em produção, use um paralelismo maior.
    stream_env.set_parallelism(1) 
    # stream_env.enable_checkpointing(30000) # Habilitar Checkpointing para resiliência

    # ------------------ connection parameters ------------------
    # O nome 'kafka' resolve no Docker Compose
    BOOTSTRAP_SERVERS = "localhost:9093" 
    # Use 's3a' para o conector Hadoop/Flink.
    S3_BASE = "s3a://data-mesh-sells" 
    # Local onde o Iceberg armazenará os metadados e os dados
    ICEBERG_WAREHOUSE = f"{S3_BASE}/warehouse" 
    # Região do AWS S3 e Glue
    AWS_REGION = "us-east-1"
    AWS_PROFILE = "data-mesh"  # Perfil AWS para credenciais (definido em ~/.aws/credentials)

    # --- CONFIGURAÇÃO DE CHECKPOINTING PARA RESILIÊNCIA ---
    
    # # 1. Habilitar Checkpointing: Intervalo de 30 segundos (30000 ms)
    stream_env.enable_checkpointing(30000) 
    
    # # 2. Configurar o armazenamento do Checkpoint no S3
    # CHECKPOINT_DIR = f"{S3_BASE}/checkpoints"
    # stream_env.get_checkpoint_config().set_checkpoint_storage(CHECKPOINT_DIR)

    # # 3. Configurações de Resiliência (Melhores Práticas)
    # # Garante que os resultados sejam exatamente uma vez (EXACTLY_ONCE)
    stream_env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    # # Permite apenas 1 checkpoint concorrente
    stream_env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    # # Define um intervalo mínimo de 5 segundos entre o final de um checkpoint e o início do próximo
    stream_env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)
    # # Define um timeout de 10 minutos para o checkpoint
    stream_env.get_checkpoint_config().set_checkpoint_timeout(60000)
    # # Preservar checkpoints externos após o cancelamento do job para recuperação manual
    stream_env.get_checkpoint_config().set_externalized_checkpoint_cleanup(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )

    #     # 1. Obtém o objeto Java CheckpointConfig subjacente do PyFlink CheckpointConfig
    # j_checkpoint_config = stream_env.get_checkpoint_config()._j_checkpoint_config
    
    # # 2. Obtém a constante Java para o modo de limpeza
    # J_RETAIN_MODE = j_gateway.jvm.org.apache.flink.runtime.checkpoint.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    
    # # 3. Chama o método Java (note o 'E' maiúsculo) diretamente com a constante Java
    # j_checkpoint_config.enableExternalizedCheckpoints(J_RETAIN_MODE)

    # enables the unaligned checkpoints
    stream_env.get_checkpoint_config().enable_unaligned_checkpoints()

    # ------------------ table env ------------------
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    
    t_env = StreamTableEnvironment.create(
        stream_env,
        environment_settings=env_settings
    )
    
    # Configurações para Flink acessar S3 (via S3A - Hadoop)
    # Essas configurações são cruciais para o Iceberg funcionar.
    cfg = t_env.get_config().get_configuration()
        # ------------------ CONFIGURAÇÃO CRÍTICA DO CLASSLOADER ------------------
    # CORREÇÃO: Força o Flink a usar a fábrica de tabelas do Kafka, eliminando ambiguidade 
    # na hora de resolver a classe do conector, caso haja conflitos.
    cfg.set_string(
        'table.exec.connector.kafka.factory.class', 
        'org.apache.flink.connector.kafka.source.table.KafkaSourceTableFactory'
    )
    # cfg.set_string("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # # Se você estiver usando um perfil específico (ex: um que usa ~/.aws/config), 
    # # desative o comentário (e garanta que o arquivo esteja acessível ao Flink Worker):
    # cfg.set_string("fs.s3a.aws.credentials.provider", "ProfileCredentialsProvider")
    # cfg.set_string("fs.s3a.load.config.at.startup", "true") # Para carregar configs do ~/.aws

    # O Flink usará credenciais do ambiente (e.g., variáveis de ambiente AWS_ACCESS_KEY_ID)

    # ------------------ Create Iceberg catalog (AWS Glue Catalog) ------------------
    # O catálogo AWS Glue é preferido para integração com Dremio/Athena.
    # 'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO' é para gerenciar arquivos no S3.
    t_env.execute_sql(f"""
        CREATE CATALOG my_iceberg_catalog WITH (
            'type' = 'iceberg',
            'warehouse' = '{ICEBERG_WAREHOUSE}',
            'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            'glue.region' = '{AWS_REGION}'
            )
    """)

    # Usar o catálogo recém-criado
    t_env.execute_sql("USE CATALOG my_iceberg_catalog")
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS sales")
    t_env.execute_sql("USE sales")
    t_env.execute_sql(f"""DROP TABLE IF EXISTS sales_create""")
    t_env.execute_sql(f"""DROP TABLE IF EXISTS sales_update""")
    t_env.execute_sql(f"""DROP TABLE IF EXISTS sales_delete""")

    # ------------------ KAFKA SOURCES (JSON) ------------------
    # As definições de tabela do Kafka são mantidas
    # ... (código para sales_create, sales_update, sales_cancel é o mesmo)
    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS sales_create (
            event_id STRING,
            sale_id INT,
            client_id INT,
            price DOUBLE,
            items INT,
            raw_timestamp_sec DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales.create.v1',
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
            'scan.startup.mode' = 'earliest-offset',
            'properties.group.id' = 'flink-sales-create-consumer',             -- CORREÇÃO: ID de grupo explícito
            'properties.request.timeout.ms' = '15000',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'false'
        )
    """)

    t_env.execute_sql("SELECT * FROM sales_create").print()

    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS sales_update (
            event_id STRING,
            sale_id INT,
            changed_field STRING,
            new_value STRING,
            raw_timestamp_sec DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales.update.v1',
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
            'scan.startup.mode' = 'earliest-offset',
            'properties.group.id' = 'flink-sales-create-consumer',             -- CORREÇÃO: ID de grupo explícito
            'properties.request.timeout.ms' = '15000',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'false'
        )
    """)
    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS sales_delete (
            event_id STRING,
            sale_id INT,
            reaspn STRING,
            raw_timestamp_sec DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales.delete.v1',
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
            'scan.startup.mode' = 'earliest-offset',
            'properties.group.id' = 'flink-sales-create-consumer',             -- CORREÇÃO: ID de grupo explícito
            'properties.request.timeout.ms' = '15000',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'false'
        )
    """)

    # ------------------ ICEBERG TABLES (RAW + TRUSTED) ------------------
    # As tabelas Iceberg RAW e TRUSTED são mantidas
    # ... (código para sales_raw e sales_trusted é o mesmo)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS sales_raw (
            event_id STRING,
            sale_id INT,
            event_type STRING,
            payload STRING,
            `timestamp` TIMESTAMP(3),
            `year` INT,
            `month` INT,
            `day` INT
        )
        PARTITIONED BY (`year`,`month`, `day`)
        WITH (
            'format-version' = '2'
        )
    """)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS sales_trusted (
            event_id STRING,
            sale_id INT,
            client_id INT,
            price DOUBLE,
            items INT,
            changed_field STRING,
            new_value STRING,
            reason STRING,
            event_type STRING,
            `timestamp` TIMESTAMP(3),
            `year` INT,
            `month` INT,
            `day` INT
        )
        PARTITIONED BY (`year`,`month`, `day`)
        WITH (
            'format-version' = '2'
        )
    """)

    t_env.execute_sql("USE CATALOG default_catalog")


    # ------------------ Unified VIEW to normalize different event types ------------------
    # A View agora é referenciada apenas por 'all_sales' e referencia as tabelas Kafka
    # pelo seu nome totalmente qualificado (catalog.database.table)
    t_env.execute_sql("""
        CREATE OR REPLACE TEMPORARY VIEW all_sales AS
        -- CREATE events
        SELECT
            CAST(event_id AS STRING)                            AS event_id,
            CAST(sale_id AS INT)                                AS sale_id,
            CAST(client_id AS INT)                              AS client_id,
            CAST(price AS DOUBLE)                               AS price,
            CAST(items AS INT)                                  AS items,
            CAST(NULL AS STRING)                                AS changed_field,
            CAST(NULL AS STRING)                                AS new_value,
            CAST(NULL AS STRING)                                AS reason,
            'CREATE'                                            AS event_type,
            -- CORREÇÃO: Multiplica por 1000 para converter segundos em milissegundos
            TO_TIMESTAMP_LTZ(CAST(raw_timestamp_sec * 1000 AS BIGINT), 3) AS `timestamp`,
            CAST(YEAR(TO_TIMESTAMP_LTZ(CAST(raw_timestamp_sec * 1000 AS BIGINT), 3)) AS INT) AS `year`,
            CAST(MONTH(TO_TIMESTAMP_LTZ(CAST(raw_timestamp_sec * 1000 AS BIGINT), 3)) AS INT) AS `month`,
            CAST(DAYOFMONTH(TO_TIMESTAMP_LTZ(CAST(raw_timestamp_sec * 1000 AS BIGINT), 3)) AS INT) AS `day`,
            JSON_STRING(
                JSON_OBJECT(
                    'event_id' VALUE event_id,
                    'sale_id' VALUE sale_id,
                    'client_id' VALUE client_id,
                    'price' VALUE price,
                    'items' VALUE items,
                    'timestamp' VALUE raw_timestamp_sec
                )
            )                                                   AS raw_payload
        FROM my_iceberg_catalog.sales.sales_create 
        UNION ALL
        -- UPDATE events
        SELECT
            CAST(event_id AS STRING)                            AS event_id,
            CAST(sale_id AS INT)                                AS sale_id,
            CAST(NULL AS INT)                                   AS client_id,
            CAST(NULL AS DOUBLE)                                AS price,
            CAST(NULL AS INT)                                   AS items,
            CAST(changed_field AS STRING)                       AS changed_field,
            CAST(new_value AS STRING)                           AS new_value,
            CAST(NULL AS STRING)                                AS reason,
            'UPDATE'                                            AS event_type,
            TO_TIMESTAMP_LTZ(CAST(raw_timestamp_sec * 1000 AS BIGINT), 3) AS `timestamp`,
            CAST(YEAR(TO_TIMESTAMP_LTZ(CAST(raw_timestamp_sec * 1000 AS BIGINT), 3)) AS INT) AS `year`,
            CAST(MONTH(TO_TIMESTAMP_LTZ(CAST(raw_timestamp_sec * 1000 AS BIGINT), 3)) AS INT) AS `month`,
            CAST(DAYOFMONTH(TO_TIMESTAMP_LTZ(CAST(raw_timestamp_sec * 1000 AS BIGINT), 3)) AS INT) AS `day`,
            JSON_STRING(
                JSON_OBJECT(
                    'event_id' VALUE event_id,
                    'sale_id' VALUE sale_id,
                    'changed_field' VALUE changed_field,
                    'new_value' VALUE new_value,
                    'timestamp' VALUE raw_timestamp_sec
                )
            )                                                   AS raw_payload
        FROM my_iceberg_catalog.sales.sales_update
        UNION ALL
        -- DELETE (antigo CANCEL) events
        SELECT
            CAST(event_id AS STRING)                            AS event_id,
            CAST(sale_id AS INT)                                AS sale_id,
            CAST(NULL AS INT)                                   AS client_id,
            CAST(NULL AS DOUBLE)                                AS price,
            CAST(NULL AS INT)                                   AS items,
            CAST(NULL AS STRING)                                AS changed_field,
            CAST(NULL AS STRING)                                AS new_value,
            CAST(reason AS STRING)                              AS reason,
            'DELETE'                                            AS event_type, -- Tipo de evento corrigido para DELETE
            TO_TIMESTAMP_LTZ(CAST(raw_timestamp_sec * 1000 AS BIGINT), 3) AS `timestamp`,
            CAST(YEAR(TO_TIMESTAMP_LTZ(CAST(raw_timestamp_sec * 1000 AS BIGINT), 3)) AS INT) AS `year`,
            CAST(MONTH(TO_TIMESTAMP_LTZ(CAST(raw_timestamp_sec * 1000 AS BIGINT), 3)) AS INT) AS `month`,
            CAST(DAYOFMONTH(TO_TIMESTAMP_LTZ(CAST(raw_timestamp_sec * 1000 AS BIGINT), 3)) AS INT) AS `day`,
            JSON_STRING(
                JSON_OBJECT(
                    'event_id' VALUE event_id,
                    'sale_id' VALUE sale_id,
                    'reason' VALUE reason,
                    'timestamp' VALUE raw_timestamp_sec
                )
            )                                                   AS raw_payload
        FROM my_iceberg_catalog.sales.sales_delete -- Tabela de origem corrigida
    """)

    # ------------------ INSERT INTO ICEBERG RAW ------------------
    t_env.execute_sql("""
        INSERT INTO my_iceberg_catalog.sales.sales_raw
        SELECT
            event_id,
            sale_id,
            event_type,
            raw_payload AS payload,
            `timestamp`,
            `year`,
            `month`,
            `day`
        FROM all_sales
    """)

    # ------------------ INSERT INTO ICEBERG TRUSTED ------------------
    t_env.execute_sql("""
        INSERT INTO my_iceberg_catalog.sales.sales_trusted
        SELECT
            event_id,
            sale_id,
            client_id,
            price,
            items,
            changed_field,
            new_value,
            reason,
            event_type,
            `timestamp`,
            `year`,
            `month`,
            `day`
        FROM all_sales
    """)

    t_env.execute("sales-iceberg-pipeline")

    # Observação: Os statements INSERT INTO submetem jobs de streaming. 
    # O comando 'flink run -py' irá manter o processo vivo.

if __name__ == "__main__":
    main()