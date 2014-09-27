from __future__ import absolute_import, print_function#, unicode_literals

import itertools
from streamparse.spout import Spout

from spouts.bitcoin_p2b import TransactionFull
import base64
import sys



from kafka import KafkaClient, SimpleProducer, SimpleConsumer


class KafkaSpout(Spout):

	def initialize(self, stormconf, context):
		# self.words = itertools.cycle(['dog', 'cat',
		# 								'zebra', 'elephant'])
		self.kafka = KafkaClient("cloud.soumet.com:9092")
		self.consumer = SimpleConsumer(self.kafka, "storm", "realtime", max_buffer_size=1310720000)
		



	def next_tuple(self):
		for message in self.consumer.get_messages(count=500, block=False):#, timeout=1):
			#transaction_data = TransactionFull()
			#transaction_data.ParseFromString(base64.b64decode(message.message.value))
			#self.emit([transaction_data])
			self.emit([message.message.value])
		self.consumer.commit()
		#self.emit(["test"])
		## word = next(self.words)
		## self.emit([word])
		#messages = self.kafka_consumer.get_messages(count=100, block=False) #get 5000 messages at a time, non blocking
		#for message in messages: #OffsetAndMessage(offset=43, message=Message(magic=0, attributes=0, key=None, value='some message'))
		#	transaction_data = TransactionFull()
		#	transaction_data.ParseFromString(base64.b64decode(message))
		#	self.emit([transaction_data])
			#self.emit([message])
		#self.kafka_consumer.commit() #save position in the kafka queue

