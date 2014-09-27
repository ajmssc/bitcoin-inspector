from pyspark import SparkContext, SparkConf
import happybase
import bitcoin_pb2, lzo, base64

hbase = happybase.Connection('localhost')
hbase_transactions_table = hbase.table('block_transactions')
hbase_transactions_table_batch = hbase_transactions_table.batch(batch_size=1000)


def decode_transaction(data):
	transaction_data = bitcoin_pb2.TransactionFull()
	transaction_data.ParseFromString(base64.b64decode(data))
	return transaction_data
	


sc = SparkContext("spark://ip-172-31-36-192.us-west-2.compute.internal:7077", "pyspark") #distributed context
#sc = SparkContext("local", "pyspark")
# rdd = sc.newAPIHadoopRDD("org.apache.hadoop.hbase.mapreduce.TableInputFormat", 
# 	"org.apache.hadoop.hbase.io.ImmutableBytesWritable", 
# 	"org.apache.hadoop.hbase.client.Result", 
# 	conf={"hbase.zookeeper.quorum": "localhost","hbase.mapreduce.inputtable": "block_data"})

text = sc.textFile("/data/bitcoin_trans1")
transactions = text.map(decode_transaction)#.cache()





def time_mapper(transaction):
	yield str("transactioncount_daily_%s" % (transaction.txtime - transaction.txtime % (60*60*24))) #day
	yield str("transactioncount_hourly_%s" % (transaction.txtime - transaction.txtime % (60*60))) #hours
	yield str("transactioncount_minutely_%s" % (transaction.txtime - transaction.txtime % 60)) #minutes

transactions_time = transactions.flatMap(time_mapper).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y).collect()
total_entries = 0
for key, value in transactions_time:
	#print "%s - %s" % (key, value)
	hbase_transactions_table_batch.put(str(key), {'metadata:count':str(value)})
	total_entries += 1
print "Time entries processed: %s" % total_entries
hbase_transactions_table_batch.send()







# def blockdata_mapper(block):
# 	yield (str("blockcount_daily_%s" % (block.time - block.time % (60*60*24))), block.difficulty) #day
# 	yield str("blockcount_hourly_%s" % (block.time - block.time % (60*60))), block.difficulty) #hours
# 	yield str("blockcount_minutely_%s" % (block.time - block.time % 60)), block.difficulty) #minutes

# blocks_data = blocks.flatMap(blockdata_mapper).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y).collect()
# total_entries = 0
# for key, value in blocks_data:
# 	hbase_blocks_table_batch.put(str(key), {'metadata:count':str(value)})
# 	total_entries += 1
# 	#print key, value
# print "Time entries processed: %s" % total_entries
# hbase_blocks_table_batch.send()





	#print time
#.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)
#for (bitcoinhash, data) in counts.collect():
#    print bitcoinhash, data




	# print (lzo.compress(str("test"),1))
	# try:
	# 	#print "Getting JSON for block " + str(block_id)
	# 	block_hash = bitcoinrpc.getblockhash(int(block_id))
	# 	print "Block #" + str(block_id) + " - hash: " + block_hash
	# 	block_json = bitcoinrpc.getblock(block_hash)
	# 	if block_json:
	# 		block_data = bitcoin_pb2.Block()
	# 		block_data.hash = block_json["hash"]
	# 		block_data.confirmations = block_json["confirmations"]
	# 		block_data.size = block_json["size"]
	# 		block_data.height = block_json["height"]
	# 		block_data.version = block_json["version"]
	# 		block_data.time = block_json["time"]
	# 		block_data.nonce = block_json["nonce"]
	# 		block_data.bits = block_json["bits"]
	# 		block_data.difficulty = float(block_json["difficulty"])
	# 		block_data.chainwork = block_json["chainwork"]
	# 		block_data.previousblockhash = block_json["previousblockhash"]
	# 		block_data.nextblockhash = block_json["nextblockhash"]
	# 		for transaction_hash in block_json["tx"]:
	# 			transaction = block_data.tx.add()
	# 			transaction.hash = transaction_hash
	# 		#for key in block_json:
	# 		#	if key == 'tx':
	# 		#		for transaction_hash in block_json['tx']:
	# 		#			get_transaction(transaction_hash)
	# 		log_block(block_id, block_data.SerializeToString())

	# except Exception as e:
	# 	print "****************Error saving block #" + str(block_id) + " " + str(e) + " ****************************"
	# 	raise
