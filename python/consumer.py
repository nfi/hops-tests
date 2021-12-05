from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
import toml

# Load HopsWorks Kafka configuration
conf = toml.load("config.toml")

# Initialize a simple String deserializer for the key and value
string_deserializer = StringDeserializer('utf_8')

# Initialize the consumer
consumer_conf = {'bootstrap.servers': conf['hops']['url']+':'+conf['kafka']['port'],
                    'security.protocol': 'SSL',
                    'ssl.ca.location': conf['project']['ca_file'],
                    'ssl.certificate.location': conf['project']['certificate_file'],
                    'ssl.key.location': conf['project']['key_file'],
                    'ssl.key.password': conf['project']['key_password'],
                    'key.deserializer': string_deserializer,
                    'value.deserializer': string_deserializer,
                    'group.id': conf['kafka']['consumer']['group_id'],
                    'auto.offset.reset': conf['kafka']['consumer']['auto_offset_reset'],
                    }
consumer = DeserializingConsumer(consumer_conf)
# Subscribe to a topic
consumer.subscribe([conf['kafka']['topic']])

# Main loop - polls for Kafka messages
while True:
    try:
        # SIGINT can't be handled when polling, limit timeout to 1 second.
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        event = msg.value()
        if event is not None:
            print("Event record {}: value: {}\n"
                    .format(msg.key(), event))
    except KeyboardInterrupt:
        break
consumer.close()
