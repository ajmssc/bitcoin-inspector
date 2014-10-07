from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

cluster = Cluster(['cassandra1.soumet.com','cassandra2.soumet.com','cassandra3.soumet.com'])
session = cluster.connect('wallets')


#profile_statement = session.prepare(
#    "UPDATE user_profiles SET email=? WHERE key=?")
user_track_statement = session.prepare("INSERT INTO data (address, balance) VALUES (?, ?)")

# add the prepared statements to a batch
batch = BatchStatement()
batch.add(user_track_statement,
          ["wallet1", 21312.21312])
batch.add(user_track_statement,
          ["wallet2", 2312.2])
batch.add(user_track_statement,
          ["wallet3", 2112.111])

# execute the batch
session.execute(batch)