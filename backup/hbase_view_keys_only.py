from jsonrpc.authproxy import AuthServiceProxy
import sys, string, getpass, time, datetime
import happybase
import pprint

#rpcuser = "bitcoinrpc"
#rpcpass = "AD3gBQJ3z1oaZqQ8sdoNhJeH47BFH888n3tKaPJji4B2"
#rpcip = "127.0.0.1"

hbase = happybase.Connection('localhost')

#hbase_blocks_table = hbase.table('block_data')
#hbase_live_transactions_table = hbase.table('realtime_transactions')
settings_table = hbase.table('settings')

#settings_table.put('row1234', {"metadata:time":"213124124"})

settings = settings_table.row('row1234')
pprint.pprint(settings)



#hbase_transactions_table = hbase.table('realtime_transactions')
#results = hbase_transactions_table.scan( filter=b"SingleColumnValueFilter('metadata','timestamp',>, 'int:124124')")
#results = hbase_transactions_table.scan( filter=b"KeyOnlyFilter() AND FirstKeyOnlyFilter()")
		
#results = hbase_live_transactions_table.scan( filter=b"SingleColumnValueFilter('metadata','status',=, 'binary:Error loading block')")
#KeyOnlyFilter() AND FirstKeyOnlyFilter()
#row_start=b'1', row_stop=b'116010',
#live=[{key:data} for data in results]
#pprint.pprint(live)

# full_list = sorted([str(key) for key in range(1, 116010)])
# row_keys = sorted([key for key, data in results])
# difference = list(set(full_list) - set(row_keys))
# pprint.pprint(difference)
# print str(len(full_list))
# print str(len(row_keys))
# print str(len(difference))



#res = hbase_blocks_table.row('99970')
#pprint.pprint(res)