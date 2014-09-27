import sys, string, getpass, time, datetime
import happybase
import pprint

hbase = happybase.Connection('localhost')
hbase_settings_table = hbase.table('settings')
hbase_blocks_table = hbase.table('block_data')




blocks_processed = hbase_settings_table.row('blocks_processed', columns=['data'])
find_existing_blocks = hbase_blocks_table.scan( filter=b'KeyOnlyFilter() AND FirstKeyOnlyFilter()')
find_existing_blocks_list = [key for key, data in find_existing_blocks]

load_dict = {}
for block_id in find_existing_blocks_list:
	load_dict['data:' + block_id] = 'processed'
hbase_settings_table.put('blocks_processed', load_dict)



