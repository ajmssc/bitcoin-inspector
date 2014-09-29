Bitcoin Inspector
=================

My big data project as part of Insight Data Science's Engineering fellowship program - September 2014


# Intro
Bitcoin Inspector is an open-source data pipeline that supports historical and live queries against the bitcoin network. It includes dynamic charts and visualizations with drilldown capabilities to explore bitcoin related data.

For an explanation of Bitcoin please follow [Khan Academy's Bitcoin explanation](https://www.khanacademy.org/economics-finance-domain/core-finance/money-and-banking/bitcoin/v/bitcoin-what-is-it)

# Live Demo
A live demo is currently running at http://bitcoin-inspector.com
The demo is also available on [Youtube](https://www.youtube.com/watch?v=3UJqJWoZ8Ro) in case of a server downtime



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
