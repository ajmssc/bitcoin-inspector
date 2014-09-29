from kafka import KafkaClient, SimpleProducer, SimpleConsumer

# To send messages synchronously
kafka = KafkaClient("cloud.soumet.com:9092")
producer = SimpleProducer(kafka)


# Note that the application is responsible for encoding messages to type str
#producer.send_messages("bitcoin", "some message")


consumer = SimpleConsumer(kafka, "consumer", "bitcoin_exchange_tmp", max_buffer_size=1310720000)
for message in consumer:
    # message is raw byte string -- decode if necessary!
    # e.g., for unicode: `message.decode('utf-8')`
    print(message)

#kafka.close()

