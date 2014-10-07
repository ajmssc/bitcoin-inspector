import sys, string, getpass, time, datetime


import bitcoin_pb2, base64


from mrjob.job import MRJob

def get_hash(btcaddress):
	return hash(btcaddress)
	#return hash(btcaddress) % ((sys.maxsize + 1) * 2)

class Graph_job(MRJob):
	# def steps(self):
	# 	# return [self.mr(mapper=self.mapper1,
	# 	# 				reducer=self.reducer1),
	# 	# 		self.mr(reducer=self.reducer2)]
	# 	return [self.mr(mapper=self.mapper,reducer=self.reducer)]
	def mapper(self, _, line):
		transaction = bitcoin_pb2.TransactionFull()
		transaction.ParseFromString(base64.b64decode(line))
		vin_amount = 0
		vout_amount = 0
		for vin in transaction.vin:
			yield ('vertex_' + str(vin.address), get_hash(vin.address))
		for vout in transaction.vout:
			yield ('vertex_' + str(vout.address), get_hash(vout.address))
		for vin in transaction.vin:
			for vout in transaction.vout:
				yield ('trans_' + str(get_hash(vin.address)) + '_' +  str(get_hash(vout.address)), 1)

	def reducer(self, keys, counts):
		key_arr = keys.split('_')
		if key_arr[0] == 'vertex':
			yield keys, list(counts)[0]
		else: #edge
			vin = key_arr[1]
			vout = key_arr[2]
			yield ('edge_' + vin + '_' + vout, sum(counts))


if __name__ == '__main__':
	Graph_job.run()
