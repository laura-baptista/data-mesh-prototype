from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError

bootstrap_servers = ['localhost:9093']  # Replace with your Kafka broker address(es)

try:
    # Attempt to create a consumer and list topics
    consumer = KafkaConsumer(group_id='test_connection_group',
                             bootstrap_servers=bootstrap_servers,
                             request_timeout_ms=15000,  # Set a timeout for the connection attempt
                             api_version=(0, 10, 1)) # Specify API version for broader compatibility
    topics = consumer.topics()
    print(f"Successfully connected to Kafka. Available topics: {topics}")
    consumer.close()
except NoBrokersAvailable:
    print("Error: No Kafka brokers available at the specified address(es).")
except KafkaTimeoutError:
    print("Error: Kafka connection timed out. Check broker availability and network connectivity.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")