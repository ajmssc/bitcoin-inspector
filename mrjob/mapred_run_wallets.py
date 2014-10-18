from mapred_bitcoin_wallets import Bitcoin_job
import sys, inspect






import mysql.connector


if __name__ == '__main__':
	sys.argv += ['--jobconf', 'mapred.job.name=' + inspect.getmodulename(__file__)]
	mr_job = Bitcoin_job(args=sys.argv[1:])
	with mr_job.make_runner() as runner:
		runner.run()
		total_entries = 0
		
		db = mysql.connector.connect(user='bitcoin', password='insightPassword0944$!',host='bitcoin.soumet.com', database='bitcoin')
		cursor = db.cursor()
		stmt_truncate = "TRUNCATE TABLE wallets"
		cursor.execute(stmt_truncate)
		db.commit()
		stmt_insert = "INSERT INTO wallets (wallet,balance) VALUES (%s,%s)"
		for line in runner.stream_output():
			try:
				key, value = mr_job.parse_output_line(line)
				cursor.execute(stmt_insert, (key, value))
				total_entries += 1
				if total_entries % 25000 == 0:
					db.commit()
					#print "Commit %s" % total_entries
			except Exception, e:
				print "Error - %s" % e
				pass
		db.commit()
		print "Entries processed: %s" % total_entries

