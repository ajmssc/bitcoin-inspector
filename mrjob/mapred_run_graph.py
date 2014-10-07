from mapred_bitcoin_graph import Graph_job
import happybase
import sys, inspect

hbase = happybase.Connection('localhost')
hbase_wallets_table = hbase.table('wallet_classes')
hbase_wallets_table_batch = hbase_wallets_table.batch(batch_size=1000)

vertices = open("vertices.dat", "w")
edges = open("edges.dat", "w")

if __name__ == '__main__':
	sys.argv.append('--jobconf')
	sys.argv.append('mapred.job.name=' + inspect.getmodulename(__file__))
	mr_job = Graph_job(args=sys.argv[1:])
	with mr_job.make_runner() as runner:
		runner.run()
		total_entries = 0
		for line in runner.stream_output():
			key, value = mr_job.parse_output_line(line)
			key_arr = key.split('_')
			if key_arr[0] == 'vertex':
				vertices.write(key_arr[1] + '\t' + str(value) + '\n')
			else:
				edges.write(key_arr[1] + '\t' + key_arr[2] + '\t' + str(value) + '\n')
			#print "%s - %s" % (key, value)
			# hbase_wallets_table_batch.put(str(key), {'metadata:count':str(value)})
			total_entries += 1
		print "Entries processed: %s" % total_entries
		# hbase_wallets_table_batch.send()

