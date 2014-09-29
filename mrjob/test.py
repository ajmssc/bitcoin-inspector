#import bitcoin_pb2, base64
import sys


# #mystring = "CkAwMDAwMDAwMGJhMzZlYjkyOWRjOTAxNzBhOTZlZTNlZmI3NmNiZWJlZTBlMGU1YzRkYTllYjBiNmU3NGQ5MTI0EOu6EhjXASCfnAEqQgpAYzFiMDlmYTZiZGMwYjEyYjE1Y2MxNDAwZDU5OGZmZWQyOWRkMzNiMmUyODIwOTNhNDg2NDZkMWI3YjM4MGM5ODABOMTSndMEQQAAwAC8K9BBSggxZDAwZmZmZlUAAIA/WkAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwNGUyMDRlMjA0ZTIwYkAwMDAwMDAwMDZlYjVjMjc5OWIwZjVmYWZhYjY0MzVkYWVlY2VmOGU3ZjYwOWI3MzFjOTg3OWMzZjc0ZjI4YzczakAwMDAwMDAwMDc3MGViZTg5NzI3MGNhNWY2ZDUzOWQ4YWZiNGVhNGY0ZTc1Nzc2MWEzNGNhODJlMTcyMDdkODg2"

# #block = bitcoin_pb2.Block()
# #block.ParseFromString(base64.b64decode(mystring))

# #print block

# print sys.maxint

# charmap = {
# 	"1":0, "2":1, "3":2, "4":3, "5":4, "6":5, "7":6, "8":7, "9":8,
# 	"A": 9,	"B": 10, "C": 11, "D": 12, "E": 13, "F": 14, "G": 15, "H": 16,
# 	"J": 17, "K": 18, "L": 19, "M": 20, "N": 21, "P": 22, "Q": 23, "R": 24,
# 	"S": 25, "T": 26, "U": 27, "V": 28, "W": 29, "X": 30, "Y": 31, "Z": 32,
# 	"a": 33, "b": 34, "c": 35, "d": 36, "e": 37, "f": 38, "g": 39, "h": 40,
# 	"i": 41, "j": 42, "k": 43, "m": 44, "n": 45, "o": 46, "p": 47, "q": 48,
# 	"r": 49, "s": 50, "t": 51, "u": 52, "v": 53, "w": 54, "x": 55, "y": 56,
# 	"z": 57
# }

# def bitcoin_address_to_id(address):
# 	if len(address) == 0:
# 		return 0L
# 	wallet_id = 0L
# 	return charmap[address[-1]] + 58 * bitcoin_address_to_id(address[:-1])
# 	#print address[:-1]




# print bitcoin_address_to_id("3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy")




import numpy
tuples = [(123,1.0023), (1243,1.00223), (1523,1.00123), (1213,1.00203), (1263,1.00923), (1523,1.08023), (1263,1.07023), (1273,1.07023), (1253,1.07023), ]

prices = [numpy.float(t[0]) for t in tuples]
volumes = [numpy.float(t[1]) for t in tuples]

print prices
print volumes

print numpy.average(prices, weights=volumes)