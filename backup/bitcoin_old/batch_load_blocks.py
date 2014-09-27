from jsonrpc.authproxy import AuthServiceProxy
import sys, string, getpass, time, datetime
import happybase
import pprint

rpcuser = "bitcoinrpc"
rpcpass = "AD3gBQJ3z1oaZqQ8sdoNhJeH47BFH888n3tKaPJji4B2"
rpcip = "127.0.0.1"

hbase = None
hbase_settings_table = None
hbase_blocks_table = None
hbase_transactions_table = None
blockchain_height = 0

if rpcpass == "":
	bitcoinrpc = AuthServiceProxy("http://"+rpcip+":8332")
else:
	bitcoinrpc = AuthServiceProxy("http://"+rpcuser+":"+rpcpass+"@"+rpcip+":8332")



def hbase_connect():
	global hbase, hbase_blocks_table, hbase_transactions_table, hbase_settings_table
	hbase = None
	hbase_blocks_table = None
	hbase_transactions_table = None
	hbase = happybase.Connection('localhost')
	hbase_blocks_table = hbase.table('block_data')
	hbase_settings_table = hbase.table('settings')
	hbase_transactions_table = hbase.table('block_transactions')


def get_transaction(transaction_hash):
	hbase_transactions_table.delete(transaction_hash)
	print "fetch_transaction " + transaction_hash
	try:
		transaction_json = bitcoinrpc.getrawtransaction(transaction_hash, 1)
		#pprint.pprint(transaction_json)
		load_dict = {}
		for key in transaction_json:
			if key == 'vin':
				for i, vin in enumerate(transaction_json[key]):
					#load_dict['in:vin' + str(i) + '_scriptSigasm'] = str(vin['scriptSig']['asm'])
					#load_dict['in:vin' + str(i) + '_scriptSighex'] = str(vin['scriptSig']['hex'])
					if 'coinbase' in vin:
						load_dict['in:vin' + str(i) + '_coinbase'] = str(vin['coinbase'])
						load_dict['in:vin' + str(i) + '_sequence'] = str(vin['sequence'])
					else:
						load_dict['in:vin' + str(i) + '_sequence'] = str(vin['sequence'])
						load_dict['in:vin' + str(i) + '_txid'] = str(vin['txid'])
						load_dict['in:vin' + str(i) + '_txvout'] = str(vin['vout'])
			elif key == 'vout':
				for i, vout in enumerate(transaction_json[key]):
					load_dict['out:vout' + str(vout['n']) + '_value'] = str(vout['value'])
					for j, address in enumerate(vout['scriptPubKey']['addresses']):
						load_dict['out:vout' + str(vout['n']) + '_address' + str(j)] = str(address)
			else:
				load_dict['metadata:' + key] = str(transaction_json[key])
		hbase_transactions_table.put(str(transaction_hash), load_dict)
	except KeyError as e:
		print "Error: Transaction# " + transaction_hash + " - keyerror: " + str(e)
		hbase_transactions_table.put(transaction_hash, {'metadata:status' : 'Error loading transaction'})
	except KeyboardInterrupt:
		raise
		#hbase_transactions_table.put(transaction_hash, {'metadata:status' : 'Error loading transaction'})
	#pprint.pprint(load_dict)
	except:
		print "Error: Transaction# " + transaction_hash + " - Could not get transaction data"
		hbase_transactions_table.put(transaction_hash, {'metadata:status' : 'Error loading transaction'})

def get_block_json(block_id):
	hbase_blocks_table.delete(str(block_id))
	try:
		#print "Getting JSON for block " + str(block_id)
		block_hash = bitcoinrpc.getblockhash(int(block_id))
		print "Block #" + str(block_id) + " - hash: " + block_hash
		block_json = bitcoinrpc.getblock(block_hash)
		if block_json:
			load_dict = {}
			for key in block_json:
				if key == 'tx':
					for transaction_hash in block_json['tx']:
						load_dict['transactions:' + transaction_hash] = transaction_hash
						get_transaction(transaction_hash)
				else:
					load_dict['metadata:' + key] = str(block_json[key])
			#pprint.pprint(load_dict)
			#del load_dict['metadata:tx']
			hbase_blocks_table.put(str(block_id), load_dict)
			hbase_settings_table.put('blocks_processed', {'data:' + str(block_id) : 'processed'})

	except Exception as e:
		#hbase_blocks_table.put(str(block_id), {'metadata:status' : 'Error loading block'})
		print "****************Error saving block #" + str(block_id) + " " + str(e) + " ****************************"
		raise


#312489


def get_existing_blocks():
	#existing_blocks = hbase_blocks_table.scan( filter=b'KeyOnlyFilter() AND FirstKeyOnlyFilter()')
	#existing_blocks_list = [key for key, data in existing_blocks]
	blocks_processed = hbase_settings_table.row('blocks_processed', columns=['data'])
	existing_blocks_list = list()
	for key in blocks_processed:
		existing_blocks_list.append(int(key.split(":")[1]))
	return existing_blocks_list

def main():
	global blockchain_height 
	hbase_connect()
	blockchain_height = bitcoinrpc.getblockcount()
	print "Current blockchain height: " + str(blockchain_height)
	print "Bitcoin client # of connections: " + str(bitcoinrpc.getconnectioncount())
	try:
		print "Max blockchain: " + str(blockchain_height)
		existing_blocks_list = get_existing_blocks()
		full_list = [key for key in range(0, blockchain_height)]
		difference = sorted(list(set(full_list) - set(existing_blocks_list)))
		print "Number of records to process: " + str(len(difference))
		for i, block_id in enumerate(difference):
			print "Loading next block " + str(block_id) + "  #" + str(i) + "/" + str(len(difference))
			if i % 20 == 0:
				hbase_connect() #flush
			#try:
			get_block_json(block_id)
				#hbase_import_table.put('bitcoinrpc', {'metadata:latest_block_loaded':str(block_id)})
			#except:
				#print "Error saving block # " + str(block_id)
				#break
	except KeyboardInterrupt:
		sys.exit()
	except KeyError:
		print "Could not connect to HBase."

		
	

main()

#for key, data in import_table.scan():
#    print key, data