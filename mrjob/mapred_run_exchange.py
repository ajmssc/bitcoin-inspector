from mapred_bitcoin_exchange import Bitcoin_job
import happybase
import sys, inspect

hbase = happybase.Connection('cloud.soumet.com')
hbase_blocks_table = hbase.table('exchange_transactions')
hbase_blocks_table_batch = hbase_blocks_table.batch(batch_size=1000)


if __name__ == '__main__':
	sys.argv.append('--jobconf')
	sys.argv.append('mapred.job.name=' + inspect.getmodulename(__file__))
	mr_job = Bitcoin_job(args=sys.argv[1:])
	with mr_job.make_runner() as runner:
		runner.run()
		total_entries = 0
		for line in runner.stream_output():
			key, value = mr_job.parse_output_line(line)
			#print key, value
			hbase_blocks_table_batch.put(str(key), {'metadata:price':str(value)})
			total_entries += 1
		print "Entries processed: %s" % total_entries
		hbase_blocks_table_batch.send()

