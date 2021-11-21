from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
import socket
from datetime import datetime
import toml

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print('Message {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


 # Load HopsWorks Kafka configuration
conf = toml.load('config.toml')
# Initialize a simple String serializer for the key
string_serializer = StringSerializer('utf_8')

producer_conf = {'bootstrap.servers': conf['hops']['url']+':'+conf['kafka']['port'],
                'security.protocol': 'SSL',
                'ssl.ca.location': conf['project']['ca_file'],
                'ssl.certificate.location': conf['project']['certificate_file'],
                'ssl.key.location': conf['project']['key_file'],
                'ssl.key.password': conf['project']['key_password'],
                'key.serializer': string_serializer,
                'value.serializer': string_serializer,
                'client.id': socket.gethostname()}

print(producer_conf)

producer = SerializingProducer(producer_conf)

producer.produce(conf['kafka']['topic'], key="key", value="value", on_delivery=acked)

# Wait up to 1 second for events. Callbacks will be invoked during
# this method call if the message is acknowledged.
producer.poll(1)
