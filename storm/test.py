
import numpy
import itertools

mydict = {}

if "3" not in mydict:
	mydict["3"] = [4]
else:
	mydict["3"].append(32)
if "3" not in mydict:
	mydict["3"] = [13]
else:
	mydict["3"].append(32)
if "3" not in mydict:
	mydict["3"] = [13]
else:
	mydict["3"].append(32)

if "1" not in mydict:
	mydict["1"] = [1]
else:
	mydict["1"].append(2)
if "1" not in mydict:
	mydict["1"] = [1]
else:
	mydict["1"].append(2)
if "1" not in mydict:
	mydict["1"] = [1]
else:
	mydict["1"].append(2)



print mydict
#print sum([mydict[key] for key in mydict])
print max([i for i in itertools.chain.from_iterable([mydict[key] for key in mydict])]) #flatten dicts
print numpy.mean([i for i in itertools.chain.from_iterable([mydict[key] for key in mydict])])