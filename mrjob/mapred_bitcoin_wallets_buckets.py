import sys, string, getpass, time, datetime


import bitcoin_pb2, base64


from mrjob.job import MRJob


class Bitcoin_job(MRJob):
	def steps(self):
		return [self.mr(mapper=self.mapper1,
						reducer=self.reducer1),
				self.mr(reducer=self.reducer2)]

	def mapper1(self, _, line):
		transaction = bitcoin_pb2.TransactionFull()
		transaction.ParseFromString(base64.b64decode(line))
		for vin in transaction.vin:
			yield (vin.address, 0 - vin.amount)
		for vout in transaction.vout:
			yield (vout.address, vout.amount)

	def reducer1(self, key, counts):
		tmp_sum = sum(counts)
		if tmp_sum < 0:
			tmp_sum = 0
		yield ("wallet_0_%012d" % int(tmp_sum), 1)
		yield ("wallet_1_%012d" % int(tmp_sum/10), 1)
		yield ("wallet_2_%012d" % int(tmp_sum/100), 1)
		yield ("wallet_3_%012d" % int(tmp_sum/1000), 1)

	def reducer2(self, key, counts):
		yield (key, sum(counts))

if __name__ == '__main__':
	Bitcoin_job.run()
