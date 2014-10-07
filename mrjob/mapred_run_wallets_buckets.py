from mapred_bitcoin_wallets_buckets import Bitcoin_job
import happybase
import sys, inspect

hbase = happybase.Connection('localhost')
hbase_wallets_table = hbase.table('wallet_classes')
hbase_wallets_table_batch = hbase_wallets_table.batch(batch_size=1000)


if __name__ == '__main__':
	sys.argv.append('--jobconf')
	sys.argv.append('mapred.job.name=' + inspect.getmodulename(__file__))
	mr_job = Bitcoin_job(args=sys.argv[1:])
	with mr_job.make_runner() as runner:
		runner.run()
		total_entries = 0
		for line in runner.stream_output():
			key, value = mr_job.parse_output_line(line)
			hbase_wallets_table_batch.put(str(key), {'metadata:count':str(value)})
			total_entries += 1
		print "Entries processed: %s" % total_entries
		hbase_wallets_table_batch.send()
