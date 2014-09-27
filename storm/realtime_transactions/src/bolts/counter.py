from __future__ import absolute_import, print_function, unicode_literals

from collections import Counter
from streamparse.bolt import Bolt
from spouts.bitcoin_p2b import TransactionFull
import base64
import happybase
import time
import numpy
import itertools

hbase = happybase.Connection('cloud.soumet.com')
hbase_realtime_table = hbase.table('realtime_counters')
hbase_realtime_table_batch = hbase_realtime_table.batch(batch_size=1000)



class TransactionCounter(Bolt):

	volume_dict = {}
	trcount_dict = {}
	fees_dict = {}

	def initialize(self, conf, ctx):
		#self.counts = Counter()
		self.volume_dict = {}
		self.trcount_dict = {}
		self.fees_dict = {}

	def process(self, tup):
		transaction_data = tup.values[0]
		transaction = TransactionFull()
		transaction.ParseFromString(base64.b64decode(transaction_data))
		key = "%s" % (transaction.txtime - transaction.txtime % 60)

		
		total_btc = sum([elem.amount for elem in transaction.vout])
		fees = sum([elem.amount for elem in transaction.vin]) - total_btc
		
		if key not in self.volume_dict:
			self.volume_dict[key] = [total_btc]
		else:
			self.volume_dict[key].append(total_btc)

		if key not in self.fees_dict:
			self.fees_dict[key] = [fees]
		else:
			self.fees_dict[key].append(fees)


		if key not in self.trcount_dict:
			self.trcount_dict[key] = 1
		else:
			self.trcount_dict[key] += 1


		#delete old keys
		currenttime = int(time.time()) - (60*11) #11 minutes ago
		minute = str(currenttime - (currenttime % 60))
		if minute in self.volume_dict:
			del self.volume_dict[minute]
		if minute in self.trcount_dict:
			del self.trcount_dict[minute]
		if minute in self.fees_dict:
			del self.fees_dict[minute]

		fees_flat = [i for i in itertools.chain.from_iterable([self.fees_dict[key] for key in self.fees_dict])]
		volume_flat = [i for i in itertools.chain.from_iterable([self.volume_dict[key] for key in self.volume_dict])]

		total_fees = sum(fees_flat)
		average_fees = numpy.mean(fees_flat)
		minimum_fees = min(fees_flat)
		maximum_fees = max(fees_flat)

		total_volume = sum(volume_flat)
		average_volume = numpy.mean(volume_flat)
		minimum_volume = min(volume_flat)
		maximum_volume = max(volume_flat)

		total_transactions = sum([self.trcount_dict[key] for key in self.trcount_dict])


		hbase_realtime_table_batch.put("statistics", {"metadata:total_fees":str(total_fees)})
		hbase_realtime_table_batch.put("statistics", {"metadata:average_fees":str(average_fees)})
		hbase_realtime_table_batch.put("statistics", {"metadata:minimum_fees":str(minimum_fees)})
		hbase_realtime_table_batch.put("statistics", {"metadata:maximum_fees":str(maximum_fees)})
		hbase_realtime_table_batch.put("statistics", {"metadata:total_volume":str(total_volume)})
		hbase_realtime_table_batch.put("statistics", {"metadata:average_volume":str(average_volume)})
		hbase_realtime_table_batch.put("statistics", {"metadata:minimum_volume":str(minimum_volume)})
		hbase_realtime_table_batch.put("statistics", {"metadata:maximum_volume":str(maximum_volume)})
		hbase_realtime_table_batch.put("statistics", {"metadata:total_transactions":str(total_transactions)})
		hbase_realtime_table_batch.send()

		#self.log("transaction: %s" % tup.values[0]["txid"])
		#self.counts[word] += 1
		#self.emit([word, self.counts[word]])
		#self.log('%s: %d' % (word, self.counts[word]))
