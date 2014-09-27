import bitcoin_pb2, lzo, base64

block_data = bitcoin_pb2.Block()


	# newblock = bitcoin_pb2.Block()
	# data = lzo.decompress(txt)
 # 	newblock.ParseFromString(data)
 # 	print newblock.hash


fh = open("testblock.dat","w")




#print (lzo.compress(str("test"),1))

#print "Getting JSON for block " + str(block_id)
block_data = bitcoin_pb2.Block()
block_data.hash = "abcd1234"
block_data.confirmations = 213123
block_data.size = 132321
block_data.height = 123141
block_data.version = 1
block_data.time = 12312312
block_data.nonce = 2132131
block_data.bits = "abcd1234"
block_data.difficulty = float(2421.232121)
block_data.chainwork = "abcd1234"
block_data.previousblockhash = "abcd1234"
block_data.nextblockhash = "abcd1234"
transaction = block_data.tx.add()
transaction.hash = "transaction"
data = lzo.compress(str(block_data.SerializeToString()),1)
data = base64.b64encode(data)
fh.write(data + "\n")
fh.write(data + "\n")
fh.write(data + "\n")
fh.write(data + "\n")
fh.write(data + "\n")


# newblock = bitcoin_pb2.Block()
# newblock.ParseFromString(lzo.decompress(data))
# print newblock.hash


# except Exception as e:
# 	print "****************Error*******"
# 	raise
