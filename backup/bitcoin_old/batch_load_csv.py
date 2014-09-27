import sys, string, getpass, time, datetime
import happybase
from snakebite.client import Client
import pprint
import urllib, json, ast, zlib, os

hdfs = Client("ip-172-31-17-255")
#for x in hdfs.ls(['/']):
#	print x


hbase = happybase.Connection('localhost')
hbase_settings_table = hbase.table('settings')


##get urls and add new ones if necessary
#TODO:uncomment below
# available_symbols_web = urllib.urlopen("http://api.bitcoincharts.com/v1/markets.json")
# available_symbols = json.loads(available_symbols_web.read())
# csv_settings_urls = hbase_settings_table.row('bitcoin_csv', columns=['urls'])
# known_symbols = [ key.split(':')[1] for key,val in csv_settings_urls.items() ]
# load_dict = {}
# for symbol in available_symbols:
# 	if (symbol['symbol'] not in known_symbols):
# 		load_dict['urls:' + symbol['symbol']] = str({'status':'',
# 												'symbol':symbol['symbol'],
# 												'url':'http://api.bitcoincharts.com/v1/trades.csv?symbol=' + symbol['symbol']})
#hbase_settings_table.put('bitcoin_csv', load_dict)

def get_csv_file(hadoop_path, symbol, url):
	csv_data = urllib.urlopen(url)
	output = open("/tmp/" + symbol + ".csv",'wb')
	output.write(zlib.decompress(csv_data.read(), zlib.MAX_WBITS|32))
	output.close()
	os.system("hdfs dfs -mkdir /data/bitcoin_historical/" + symbol)
	os.system("hdfs dfs -copyFromLocal -f /tmp/" + symbol + ".csv /data/bitcoin_historical/" + symbol + "/" + symbol + ".csv")
	os.system("rm -rf /tmp/" + symbol + ".csv")



csv_settings_urls = hbase_settings_table.row('bitcoin_csv', columns=['urls'])
urls = {}
for url, val in csv_settings_urls.items():
	urls[url] = ast.literal_eval(val)
	symbol = url.split(":")[1]
	print symbol
	get_csv_file("", symbol, "http://api.bitcoincharts.com/v1/csv/" + symbol + ".csv.gz")
#pprint.pprint(urls['urls:thAUD'])
#get_csv_file("", "thAUD", "http://api.bitcoincharts.com/v1/csv/thAUD.csv.gz")


#hbase_settings_table.put('bitcoin_csv', load_dict)
	#'bitcoin_csv', 
	#{'urls:btceUSD' : str({"status":"", 
	#						"url":"http://api.bitcoincharts.com/v1/trades.csv?symbol=btceUSD"}),
	# 'urls:btceUSD' : str({"status":"", 
	# 						"url":"http://api.bitcoincharts.com/v1/trades.csv?symbol=btceUSD"}),
	# 'urls:btceUSD' : str({"status":"", 
	# 						"url":"http://api.bitcoincharts.com/v1/trades.csv?symbol=btceUSD"}),
	# 'urls:btceUSD' : str({"status":"", 
	# 						"url":"http://api.bitcoincharts.com/v1/trades.csv?symbol=btceUSD"}),
	# 'urls:btceUSD' : str({"status":"", 
	# 						"url":"http://api.bitcoincharts.com/v1/trades.csv?symbol=btceUSD"}),
	#}



#hbase_settings_table.put('bitcoin_csv', {'urls:http://api.bitcoincharts.com/v1/trades.csv?symbol=btceUSD' : '{"status":""}'})
#hbase_settings_table.delete('bitcoin_csv')

