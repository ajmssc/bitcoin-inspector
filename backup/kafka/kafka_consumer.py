from kafka import KafkaClient, SimpleProducer, SimpleConsumer

import logging
logging.basicConfig()


# To send messages synchronously
kafka = KafkaClient("cloud.soumet.com:9092")


consumer = SimpleConsumer(kafka, "group1", "realtime")
for message in consumer:
    # e.g., for unicode: `message.decode('utf-8')`
    print(message)

kafka.close()

