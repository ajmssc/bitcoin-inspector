#!/usr/bin/env python
 
import sys
 
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

try:
  transport = TSocket.TSocket('localhost', 4444)
  # Buffering is critical. Raw sockets are very slow
  transport = TTransport.TBufferedTransport(transport)
  # Wrap in a protocol
  protocol = TBinaryProtocol.TBinaryProtocol(transport)
  # Create a client to use the protocol encoder
  client = HelloWorld.Client(protocol)
 
  # Connect!
  transport.open()
 
  client.ping()
  print "ping()"
 
  msg = client.sayHello()
  print msg
  msg = client.sayMsg(HELLO_IN_KOREAN)
  print msg
 
  transport.close()
 
except Thrift.TException, tx:
  print "%s" % (tx.message)