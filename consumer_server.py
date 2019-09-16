from pykafka import KafkaClient
from pykafka.simpleconsumer import OffsetType
import logging

# Logger
logging.getLogger("pykafka.broker").setLevel('ERROR')

# Client
client = KafkaClient(hosts="localhost:9092")

# Topic
topic = client.topics[b'service-calls']

# Consumer
consumer = topic.get_balanced_consumer(
    consumer_group=b'pytkafka-test-2',
    auto_commit_enable=False,
    auto_offset_reset=OffsetType.EARLIEST,
    zookeeper_connect='remotehost:2181'
)

# Messages
for message in consumer:
    if message is not None:
        print(message.offset, message.value)
