from __future__ import absolute_import, print_function#, unicode_literals

import itertools
from streamparse.spout import Spout

import base64
import sys



from kafka import KafkaClient, SimpleProducer, SimpleConsumer
#from kafka.client import KafkaClient
#from kafka.consumer import SimpleConsumer


kafka = KafkaClient("cloud.soumet.com:9092")
kafka_consumer = SimpleConsumer(kafka, "storm", "realtime", max_buffer_size=1310720000)#, max_buffer_size=1310720000)
		
for message in kafka_consumer.get_messages(count=5000, block=False):#, block=True, timeout=4):
	print(message.message.value)

kafka_consumer.commit()
