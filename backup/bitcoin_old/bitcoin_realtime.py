from jsonrpc.authproxy import AuthServiceProxy
import sys, string, getpass, time, datetime
import happybase
import pprint

rpcuser = "bitcoinrpc"
rpcpass = "AD3gBQJ3z1oaZqQ8sdoNhJeH47BFH888n3tKaPJji4B2"
rpcip = "127.0.0.1"

hbase = happybase.Connection('localhost')
hbase_blocks_table = hbase.table('block_data')
hbase_transactions_table = hbase.table('realtime_transactions')

print "start monitoring"

if rpcpass == "":
    bitcoinrpc = AuthServiceProxy("http://"+rpcip+":8332")
else:
    bitcoinrpc = AuthServiceProxy("http://"+rpcuser+":"+rpcpass+"@"+rpcip+":8332")

blockchain_height = bitcoinrpc.getblockcount()


def get_transaction(transaction_hash):
    hbase_transactions_table.delete(transaction_hash)
    print "Processing transaction " + transaction_hash
    try:
        transaction_json = bitcoinrpc.getrawtransaction(transaction_hash, 1)
        #pprint.pprint(transaction_json)
        load_dict = {'metadata:timestamp':str(time.time())}
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
        print "Saving new block: #" + str(block_id)
        block_hash = bitcoinrpc.getblockhash(int(block_id))
        print "Block #" + str(block_id) + " - hash=" + block_hash
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
    except Exception as e:
        #hbase_blocks_table.put(str(block_id), {'metadata:status' : 'Error loading block'})
        print "****************Error saving block #" + str(block_id) + " " + str(e) + " ****************************"
        raise


def main():
    print "# of connections: " + str(bitcoinrpc.getconnectioncount())
    current_blockcount = bitcoinrpc.getblockcount()
    current_mempool = bitcoinrpc.getrawmempool()
    try:
        while 1:
            time.sleep(1.41)
            print "Checking for transactions"
            new_blockcount = bitcoinrpc.getblockcount()
            new_mempool = bitcoinrpc.getrawmempool()
            diff_mempool = list(set(new_mempool) - set(current_mempool))
            if (len(diff_mempool) > 0):
                print "Processing " + str(len(diff_mempool)) + " new transaction(s)"
                for transaction_hash in diff_mempool:
                    get_transaction(transaction_hash)
                current_mempool = new_mempool
            if new_blockcount > current_blockcount:
                print "New block to process: " + str(new_blockcount)
                get_block_json(new_blockcount)
                current_blockcount = new_blockcount
    except KeyboardInterrupt:
        sys.exit()
 
   







main()


