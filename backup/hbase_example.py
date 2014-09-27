import happybase
import pprint

hbase = happybase.Connection('localhost') #connect to Hbase on localhost

hbase_import_table = hbase.table('batch_import') # load tables into objects
hbase_blocks_table = hbase.table('block_data')
hbase_transactions_table = hbase.table('block_transactions')


hbase_settings = hbase_import_table.row('bitcoinrpc') #get row key=bitcoinrpc


#find rows with column=metadata:status = 'Error loading block'
results = hbase_blocks_table.scan( filter=b"SingleColumnValueFilter('metadata','status',=, 'binary:Error loading block')")

#return all keys in table
results = hbase_blocks_table.scan( filter=b"KeyOnlyFilter() AND FirstKeyOnlyFilter()")



#save row key=bitcoinrpc, column='metadata:latest_block_loaded' value='i'
hbase_import_table.put('bitcoinrpc', {'metadata:latest_block_loaded':str(i)})
