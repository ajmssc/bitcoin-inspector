import sys, string, getpass, time, datetime


import bitcoin_pb2, base64


from mrjob.job import MRJob


class Bitcoin_job(MRJob):
	def mapper(self, _, line):
		transaction = bitcoin_pb2.TransactionFull()
		transaction.ParseFromString(base64.b64decode(line))
		for vin in transaction.vin:
			#if vin.address[:1] == "l":
			yield (vin.address, 0 - vin.amount)
		for vout in transaction.vout:
			#if vout.address[:1] == "l":
			yield (vout.address, vout.amount)

	def combiner(self, word, counts):
		yield (word, sum(counts))

	def reducer(self, word, counts):
		yield (word, sum(counts))


if __name__ == '__main__':
	Bitcoin_job.run()
