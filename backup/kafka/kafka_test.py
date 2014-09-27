from kafka import KafkaClient, SimpleProducer, SimpleConsumer

# To send messages synchronously
kafka = KafkaClient("127.0.0.1:9092")
producer = SimpleProducer(kafka)


# Note that the application is responsible for encoding messages to type str
#producer.send_messages("bitcoin", "some message")


consumer = SimpleConsumer(kafka, "consumer", "bitcoin_blocks")
for message in consumer:
    # message is raw byte string -- decode if necessary!
    # e.g., for unicode: `message.decode('utf-8')`
    print(message)

#kafka.close()

