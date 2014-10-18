from mapred_bitcoin_wallets import Bitcoin_job
import sys,inspect





###CASSANDRA
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

if __name__ == '__main__':
	sys.argv += ['--jobconf', 'mapred.job.name=' + inspect.getmodulename(__file__)]
	mr_job = Bitcoin_job(args=sys.argv[1:])
	with mr_job.make_runner() as runner:
		runner.run()
		total_entries = 0
		cluster = Cluster(['cassandra1.soumet.com','cassandra2.soumet.com','cassandra3.soumet.com'])
		session = cluster.connect('wallets')
		session.execute("TRUNCATE data")
		
		batch = BatchStatement()
		stmt_insert = session.prepare("INSERT INTO data (balance_int, balance, address) VALUES (?,?,?)")
		
		for line in runner.stream_output():
			try:
				key, value = mr_job.parse_output_line(line)
				batch.add(stmt_insert, [int(value), value, key])
				total_entries += 1
				if total_entries % 65000 == 0:
					session.execute(batch)
					batch = BatchStatement()
			except Exception, e:
				print "Error - %s" % e
				pass
		session.execute(batch)
		print "Entries processed: %s" % total_entries


















# import mysql.connector


# if __name__ == '__main__':
# 	mr_job = Bitcoin_job(args=sys.argv[1:])
# 	with mr_job.make_runner() as runner:
# 		runner.run()
# 		total_entries = 0
		
# 		db = mysql.connector.connect(user='bitcoin', password='insightPassword0944$!',host='bitcoin.soumet.com', database='bitcoin')
# 		cursor = db.cursor()
# 		stmt_truncate = "TRUNCATE TABLE wallets"
# 		cursor.execute(stmt_truncate)
# 		db.commit()
# 		stmt_insert = "INSERT INTO wallets (wallet,balance) VALUES (%s,%s)"
# 		for line in runner.stream_output():
# 			try:
# 				key, value = mr_job.parse_output_line(line)
# 				cursor.execute(stmt_insert, (key, value))
# 				total_entries += 1
# 				if total_entries % 25000 == 0:
# 					db.commit()
# 					#print "Commit %s" % total_entries
# 			except Exception, e:
# 				print "Error - %s" % e
# 				pass
# 		db.commit()
# 		print "Entries processed: %s" % total_entries

