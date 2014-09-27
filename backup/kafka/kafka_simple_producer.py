from kafka import KafkaClient, SimpleProducer, SimpleConsumer, KeyedProducer, HashedPartitioner, RoundRobinPartitioner

import logging
logging.basicConfig()


# To send messages synchronously
kafka = KafkaClient("cloud.soumet.com:9092")




producer = SimpleProducer(kafka)
producer.send_messages("realtime", "some message")
producer.send_messages("realtime", "some message")
producer.send_messages("realtime", "some message")
producer.send_messages("realtime", "some message")
producer.send_messages("realtime", "some message")
producer.send_messages("realtime", "some message")
producer.send_messages("realtime", "some message")
producer.send_messages("realtime", "some message")
producer.send_messages("realtime", "some message")





#consumer = SimpleConsumer(kafka, "group1", "bitcoin")
#for message in consumer:
    # message is raw byte string -- decode if necessary!
    # e.g., for unicode: `message.decode('utf-8')`
    #print(message)

#kafka.close()

