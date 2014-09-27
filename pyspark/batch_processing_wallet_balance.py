from pyspark import SparkContext, SparkConf
import happybase
import bitcoin_pb2, lzo, base64
import mysql.connector



hbase = happybase.Connection('localhost')
hbase_transactions_table = hbase.table('block_transactions')
hbase_transactions_table_batch = hbase_transactions_table.batch(batch_size=1000)


def decode_transaction(data):
	transaction_data = bitcoin_pb2.TransactionFull()
	transaction_data.ParseFromString(base64.b64decode(data))
	return transaction_data
	


sc = SparkContext("spark://ip-172-31-36-192.us-west-2.compute.internal:7077", "pyspark") #distributed context
#sc = SparkContext("local", "pyspark")

text = sc.textFile("/data/bitcoin_trans1")
#text = sc.textFile("/data/wallet_test/20140920120900_13.dat")
transactions = text.map(decode_transaction)#.cache()


def inspect_transaction(transaction):
	for vin in transaction.vin:
		yield (str("walletbalance_%s" % vin.address), 0 - vin.amount)
	for vout in transaction.vout:
		yield (str("walletbalance_%s" % vout.address), vout.amount)
	#yield str("transactioncount_daily_%s" % (transaction.txtime - transaction.txtime % (60*60*24))) #day
	#yield str("transactioncount_hourly_%s" % (transaction.txtime - transaction.txtime % (60*60))) #hours
	#yield str("transactioncount_minutely_%s" % (transaction.txtime - transaction.txtime % 60)) #minutes

transactions_wallets = transactions.flatMap(inspect_transaction).reduceByKey(lambda x, y: x+y).collect()
total_entries = 0
#for key, value in transactions_wallets:
	#print "(%s , %s)" % (key, value)
	# if key == "walletbalance_coinbase":
	# 	print "(%s , %s)" % (key, value)
	#hbase_transactions_table_batch.put(str(key), {'metadata:count':str(value)})
#	total_entries += 1
#print "Time entries processed: %s" % total_entries
#hbase_transactions_table_batch.send()


#write to MySQL
db = mysql.connector.connect(user='root', password='insightPassword0944$!',host='cloud.soumet.com', database='bitcoin')
cursor = db.cursor()
stmt_truncate = "TRUNCATE TABLE wallets"
cursor.execute(stmt_truncate)
db.commit()
stmt_insert = "INSERT INTO wallets (wallet,balance) VALUES (%s,%s)"
cursor.executemany(stmt_insert, transactions_wallets)
db.commit()
