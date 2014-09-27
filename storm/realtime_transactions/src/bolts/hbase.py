from __future__ import absolute_import, print_function, unicode_literals

from collections import Counter
from streamparse.bolt import Bolt
from spouts.bitcoin_p2b import TransactionFull
import base64
import happybase

hbase = happybase.Connection('cloud.soumet.com')
hbase_realtime_table = hbase.table('realtime')
hbase_realtime_table_batch = hbase_realtime_table.batch(batch_size=1000)



class HBaseDumper(Bolt):

	def initialize(self, conf, ctx):
		self.counts = Counter()

	def process(self, tup):
		#word = tup.values[0]
		#self.log(tup)
		transaction_data = tup.values[0]
		transaction = TransactionFull()
		transaction.ParseFromString(base64.b64decode(transaction_data))
		self.log(transaction.txid)
		total_btc = sum([elem.amount for elem in transaction.vout])
		fees = sum([elem.amount for elem in transaction.vin]) - total_btc
		key = "transaction_%s_%s" % (transaction.txtime, transaction.txid) 
		hbase_realtime_table_batch.put(str(key), {'metadata:amount':str(total_btc)})
		hbase_realtime_table_batch.put(str(key), {'metadata:fees':str(fees)})
		for i, elem in enumerate(transaction.vin):
			hbase_realtime_table_batch.put(str(key), {'metadata:vin_%s_address' % i :str(elem.address)})
			hbase_realtime_table_batch.put(str(key), {'metadata:vin_%s_amount' % i :str(elem.amount)})
		for i, elem in enumerate(transaction.vout):
			hbase_realtime_table_batch.put(str(key), {'metadata:vout_%s_address' % i :str(elem.address)})
			hbase_realtime_table_batch.put(str(key), {'metadata:vout_%s_amount' % i :str(elem.amount)})


		hbase_realtime_table_batch.send()

		#self.log("transaction: %s" % tup.values[0]["txid"])
		#self.counts[word] += 1
		#self.emit([word, self.counts[word]])
		#self.log('%s: %d' % (word, self.counts[word]))
