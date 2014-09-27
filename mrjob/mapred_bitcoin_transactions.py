import sys, string, getpass, time, datetime


import bitcoin_pb2, base64


from mrjob.job import MRJob


class Bitcoin_job(MRJob):
	def mapper(self, _, line):
		transaction = bitcoin_pb2.TransactionFull()
		transaction.ParseFromString(base64.b64decode(line))
		yield (str("transactioncount_daily_%s" % (transaction.txtime - transaction.txtime % (60*60*24))), 1) #day
		yield (str("transactioncount_hourly_%s" % (transaction.txtime - transaction.txtime % (60*60))), 1) #hours
		yield (str("transactioncount_minutely_%s" % (transaction.txtime - transaction.txtime % 60)), 1) #minutes

	def combiner(self, word, counts):
		yield (word, sum(counts))

	def reducer(self, word, counts):
		yield (word, sum(counts))


if __name__ == '__main__':
	Bitcoin_job.run()
