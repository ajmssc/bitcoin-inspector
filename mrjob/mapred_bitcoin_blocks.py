import sys, string, getpass, time, datetime


import bitcoin_pb2, base64


from mrjob.job import MRJob


class Bitcoin_job(MRJob):
	# def steps(self):
	# 	return [self.mr(self.group_by_user_rating, self.count_ratings_users_freq),
	# 			self.mr(self.pairwise_items, self.calculate_similarity),
	# 			self.mr(self.calculate_ranking, self.top_similar_items)
	# 			]
	def mapper(self, _, line):
		#print line
		block = bitcoin_pb2.Block()
		block.ParseFromString(base64.b64decode(line))
		yield (str("blockcount_daily_%s" % (block.time - block.time % (60*60*24))), 1) #day
		yield (str("blockcount_hourly_%s" % (block.time - block.time % (60*60))), 1) #hours
		yield (str("blockcount_minutely_%s" % (block.time - block.time % 60)), 1) #minutes
		#for word in WORD_RE.findall(line):
		#    yield (word.lower(), 1)

	def combiner(self, word, counts):
		yield (word, sum(counts))

	def reducer(self, word, counts):
		yield (word, sum(counts))


if __name__ == '__main__':
	Bitcoin_job.run()
