import sys, string, getpass, time, datetime
import numpy
from mrjob.job import MRJob


class Bitcoin_job(MRJob):
	def steps(self):
		return [self.mr(mapper=self.mapper,reducer=self.reducer)]

	def mapper(self, _, line):
		line_r = line.split(',')
		if len(line_r) >3: #realtime format with currency
			currency = line_r[4]
			volume = float(line_r[3])
			price = float(line_r[2])
			timestamp = int(line_r[1])
		else:
			currency = "USD"
			volume = float(line_r[2])
			price = float(line_r[1])
			timestamp = int(line_r[0])

		if volume > 0:
			yield str("BTC_%s_daily_%s" % (currency, timestamp - timestamp % (60*60*24))), (price, volume) #day
			yield str("BTC_%s_hourly_%s" % (currency, timestamp - timestamp % (60*60))), (price, volume) #hours
			yield str("BTC_%s_minutely_%s" % (currency, timestamp - timestamp % 60)), (price, volume) #minutes


	def reducer(self, key, tuples):
		prices = []
		volumes = []
		for t in tuples:
			prices.append(t[0])
			volumes.append(t[1])
		yield (key, numpy.average(prices, weights=volumes))


if __name__ == '__main__':
	Bitcoin_job.run()
