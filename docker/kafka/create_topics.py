from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
import time


# --------------------------------------------------------
# CONFIGURAÇÕES
# --------------------------------------------------------
KAFKA_BOOTSTRAP = "kafka:9092"

# Tópicos por domínio
topics = [
    # Domínio Clientes
    "clients.create.v1",
    "clients.update.v1",
    "clients.delete.v1",

    # Domínio Vendas
    "sales.create.v1",
    "sales.update.v1",
    "sales.cancel.v1",
]

PARTITIONS = 3
REPLICATION = 1


# --------------------------------------------------------
# AGUARDAR O KAFKA FICAR ONLINE
# --------------------------------------------------------
def wait_for_kafka(bootstrap):
    admin = AdminClient({"bootstrap.servers": bootstrap})

    for t in range(5):
        try:
            admin.list_topics(timeout=3)
            return True
        except KafkaException:
            print("WARN: Kafka ainda não disponível. Tentando novamente")
            time.sleep(2 ** t)

    return False


# --------------------------------------------------------
# CRIAÇÃO DOS TÓPICOS
# --------------------------------------------------------
def create_kafka_topics():
    admin = AdminClient({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "topic_creator"
    })

    new_topics = [
        NewTopic(
            topic,
            num_partitions=PARTITIONS,
            replication_factor=REPLICATION,
        )
        for topic in topics
    ]

    print("INFO: Criando tópicos")

    # create_topics retorna Futures
    results = admin.create_topics(new_topics, request_timeout=15)

    for topic, future in results.items():
        try:
            future.result()
            print(f"INFO: Criado: {topic}")
        except KafkaException as e:
            if "TOPIC_ALREADY_EXISTS" in str(e):
                print(f"WARN: Já existe: {topic}")
            else:
                print(f"ERROR: Erro ao criar {topic}: {e}")


# --------------------------------------------------------
# EXECUÇÃO
# --------------------------------------------------------
if __name__ == "__main__":
    print("INFO: Aguardando Kafka ficar online...")

    if not wait_for_kafka(KAFKA_BOOTSTRAP):
        print("ERROR: Kafka não subiu a tempo. Abortando.")
        exit(1)

    print("\nINFO: Kafka online! Criando tópicos...\n")
    create_kafka_topics()
