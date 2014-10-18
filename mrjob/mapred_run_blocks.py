from mapred_bitcoin_blocks import Bitcoin_job
import happybase
import sys, inspect

hbase = happybase.Connection('localhost')
hbase_blocks_table = hbase.table('block_data')
hbase_blocks_table_batch = hbase_blocks_table.batch(batch_size=1000)




#http://aimotion.blogspot.com/2012/08/introduction-to-recommendations-with.html
if __name__ == '__main__':
	sys.argv += ['--jobconf', 'mapred.job.name=' + inspect.getmodulename(__file__)]
	#--file=bitcoin_pb2.py --hadoop-bin /usr/bin/hadoop -r local 20140925015000_0.dat
	#--file=bitcoin_pb2.py --hadoop-bin /usr/bin/hadoop -r hadoop hdfs:////data/bitcoin_transactions/20140925015000_0.dat
	mr_job = Bitcoin_job(args=sys.argv[1:])
	with mr_job.make_runner() as runner:
		runner.run()
		

		total_entries = 0
		for line in runner.stream_output():
			key, value = mr_job.parse_output_line(line)
			hbase_blocks_table_batch.put(str(key), {'metadata:count':str(value)})
		#	print key
			total_entries += 1
			#print key, value
		print "Time entries processed: %s" % total_entries
		hbase_blocks_table_batch.send()

