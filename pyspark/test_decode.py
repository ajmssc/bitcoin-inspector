
import bitcoin_pb2, lzo, base64




# block_data = bitcoin_pb2.Block()
# block_data.hash = "hash"
# block_data.confirmations = 123123
# block_data.size = 1231231
# block_data.height = 3232
# block_data.version = 1
# block_data.time = 121
# block_data.nonce = 21211
# block_data.bits = "bits"
# block_data.difficulty = float(21.2121)
# block_data.chainwork = "chainwork"
# block_data.previousblockhash = "previousblockhash"
# block_data.nextblockhash = "nextblockhash"
# transaction = block_data.tx.add()
# transaction.hash = "transaction_hash"
# data = base64.b64encode(block_data.SerializeToString())
# #data = "CkAwMDAwMDAwMDk0NDU2MjllNTY2MmI4MTgxNWRhMjZiMGNkZmE2NWM1NGY5YjY0Mzc4YWIyYTNkNTAxMDg3NDM4EJuoExjYASCOJypCCkAxMTZmMTcxNWQ1ZjdkZGU5ZTdhOTY4NTM2MjNmMDUxZmEwMDRlZjBiMjdhZDFjOTFiNGM2NTViZWQ5YzQ0NWFhMAE46oT7zARBAAAgwKrM6kFKCDFkMDBmZmZmVQAAgD9aQDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAxMzhmMTM4ZjEzOGZiQDAwMDAwMDAwZDhlMWZmNzA3NTNiZGNjODZhMDM3ZjkzOTRkNjFjOTQzN2QxYTE0Zjk4NzhjNmI1MjI4NWNiYjdqQDAwMDAwMDAwMmQyZTJjNzQ0ZGNhZDI3ZTM0YjU2NzRjZTAzMTk0NDUxZmM4MmQzNGQwYjExNGFlYTFjOGU4MjM="
# print data
# test_2 = bitcoin_pb2.Block()
# test_2.ParseFromString(base64.b64decode(data))
# print test_2


# test = bitcoin_pb2.Transaction()
# test.hash = "xxxx"
# data = base64.b64encode(test.SerializeToString())

# test_2 = bitcoin_pb2.Transaction()
# test_2.ParseFromString(base64.b64decode(data))
# print test_2

p2b_transaction = bitcoin_pb2.TransactionFull()
p2b_transaction.txid = "xxxxx"
p2b_transaction.version = 1
p2b_transaction.locktime = 0
record = p2b_transaction.vin.add()
record.sequence = 1231231
record.txid = "txin"
record.vout = 1
record.address = "in_address"
record.amount = float(1.232)
record = p2b_transaction.vout.add()
record.address = "address_vout"
record.amount = float(1.22)
record.n = 2

p2b_transaction.blockhash = "block"
p2b_transaction.confirmations = 123123
p2b_transaction.txtime = 131231
p2b_transaction.blocktime = 132131


data = base64.b64encode(p2b_transaction.SerializeToString())
print data

transaction_data = bitcoin_pb2.TransactionFull()
transaction_data.ParseFromString(base64.b64decode(data))

print transaction_data



