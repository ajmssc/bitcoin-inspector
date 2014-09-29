bitcoin-inspector
=================

My project as part of Insight Data Science's fellowship program - September 2014

Big data pipeline supporting queries against the bitcoin network.
Live demo currently running at http://bitcoin-inspector.com
Demo also available on Youtube in case of server downtime: https://www.youtube.com/watch?v=3UJqJWoZ8Ro



Read further for more details about the pipeline and instructions.

## Pipeline Overview

![alt tag](http://image.slidesharecdn.com/jean-marc-insightdataengineering-140925170658-phpapp02/95/bitcoin-data-pipeline-insight-data-science-project-september-2014-8-1024.jpg)


### Batch processing

- Python jobs query the bitcoin RPC client and get JSON data. That JSON data is pushed to Kafka. Tracking of current block is done using a HBase entry but can also be done using a temporary file in the folder.
- Python jobs consume the data from Kafka and store it in HDFS. Runs every hour. The file names are tagged using a timestamp.
- Map Reduce is run using MrJob to create multiple key-value pairs into HBase (and MySQL for wallet balance).
- Flask reads from HBase and serves charts using HighCharts/Jquery/Bootstrap

### Realtime data

- A Storm topology reads data from Kafka into a Storm Spout.
- 1 Bolt collects periodic metrics and statistics stored into HBase
- 1 Bolt monitors transactions and writes into HBase. The table has a TTL of 120s to automatically get rid of old transactions.
- The Storm topology is loaded using streamparse.




More details at http://www.slideshare.net/Jeanmarcsoumet/bitcoin-data-pipeline-insight-data-science-project

