import twint
import schedule
import time
import yaml
import cx_Oracle
from datetime import datetime



def process_yaml():
	with open("./config.yaml") as file:
		return yaml.safe_load(file)



def create_connection(data):
	connection = str()
	dsn_var = """(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.eu-frankfurt-1.oraclecloud.com))(connect_data=(service_name=g2f4dc3e5463897_db202107142034_high.adb.oraclecloud.com))(security=(ssl_server_cert_dn="CN=adwc.eucom-central-1.oraclecloud.com, OU=Oracle BMCS FRANKFURT, O=Oracle Corporation, L=Redwood City, ST=California, C=US")))"""
	try:
		connection = cx_Oracle.connect(user=data['db']['username'], password=data['db']['password'], dsn=dsn_var)
	except cx_Oracle.DatabaseError:
		print('Error in connection.')
		time.sleep(1)
		connection = cx_Oracle.connect(user=data['db']['username'], password=data['db']['password'], dsn=dsn_var)

	connection.autocommit = True
	return connection



def to_timestamp(timestamp):
	dt_object = datetime.fromtimestamp(timestamp/1000)
	return dt_object



# auxiliary function to get hashtags from a string. (removes duplicates using set then converts to list)
def extract_hash_tags(s):
	return list(set(part[1:] for part in s.split() if part.startswith('#')))



# you can change the name of each "job" after "def" if you'd like.
def search(filter_search, connection):
	print ("Fetching Tweets")
	c = twint.Config()
	# choose username (optional)
	#c.Username = x
	# choose search term (optional)
	c.Search = '{}'.format(filter_search)
	# choose beginning time (narrow results)
	c.Since = "2018-01-01"
	# set limit on total tweets
	#c.Limit = 10
	c.Pandas = True
	
	twint.run.Search(c)
	tweets_df = twint.storage.panda.Tweets_df
	print(tweets_df.columns)
	print(tweets_df.head(1).to_string())

	# Get tweets from db.
	cursor = connection.cursor()
	'''
	['id', 'conversation_id', 'created_at', 'date', 'timezone', 'place',
       'tweet', 'language', 'hashtags', 'cashtags', 'user_id', 'user_id_str',
       'username', 'name', 'day', 'hour', 'link', 'urls', 'photos', 'video',
       'thumbnail', 'retweet', 'nlikes', 'nreplies', 'nretweets', 'quote_url',
       'search', 'near', 'geo', 'source', 'user_rt_id', 'user_rt',
       'retweet_id', 'reply_to', 'retweet_date', 'translate', 'trans_src',
       'trans_dest']
      '''
	consecutive_exceptions = int()
	for index, x in tweets_df.iterrows():
		if consecutive_exceptions > 50:
			break
		geoloc = str()
		try:
			geoloc = x['geo'].get('coordinates')
		except AttributeError:
			pass
		row = [
			filter_search, x['link'], x['username'], x['name'], x['place'], x['tweet'], -1, -1, -1, -1, str(extract_hash_tags(x['tweet'])), to_timestamp(x['created_at']), 
			x['source'], x['language'], x['nretweets'], x['nlikes'], x['nreplies'], geoloc, x['near'], x['retweet_date'] 
		]
		try:
			cursor.execute("insert into admin.public_sentiment values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20)", row)
			print('Inserted new tweet with ID {}'.format(x['link']))
			print("Currently on row: {}; Currently iterrated {}% of rows".format(index, (index + 1)/len(tweets_df.index) * 100))
			consecutive_exceptions = 0
		except Exception as e:
			consecutive_exceptions += 1
			#print('Can not insert {}. Continuing. {}'.format(row, e))



def user(user, connection):
	print ("Fetching Tweets")
	c = twint.Config()
	# choose username (optional)
	c.Username = user
	# choose search term (optional)
	#c.Search = '{}'.format(filter_search)
	# choose beginning time (narrow results)
	c.Since = "2018-01-01"
	# set limit on total tweets
	#c.Limit = 10
	c.Pandas = True
	
	twint.run.Search(c)
	tweets_df = twint.storage.panda.Tweets_df
	print(tweets_df.columns)
	print(tweets_df.head(1).to_string())

	# Get tweets from db.
	cursor = connection.cursor()
	'''
	['id', 'conversation_id', 'created_at', 'date', 'timezone', 'place',
       'tweet', 'language', 'hashtags', 'cashtags', 'user_id', 'user_id_str',
       'username', 'name', 'day', 'hour', 'link', 'urls', 'photos', 'video',
       'thumbnail', 'retweet', 'nlikes', 'nreplies', 'nretweets', 'quote_url',
       'search', 'near', 'geo', 'source', 'user_rt_id', 'user_rt',
       'retweet_id', 'reply_to', 'retweet_date', 'translate', 'trans_src',
       'trans_dest']
      '''
	sconsecutive_exceptions = int()
	for index, x in tweets_df.iterrows():
		if consecutive_exceptions > 50:
			break
		geoloc = str()
		try:
			geoloc = x['geo'].get('coordinates')
		except AttributeError:
			pass
		row = [
			filter_search, x['link'], x['username'], x['name'], x['place'], x['tweet'], -1, -1, -1, -1, str(extract_hash_tags(x['tweet'])), to_timestamp(x['created_at']), 
			x['source'], x['language'], x['nretweets'], x['nlikes'], x['nreplies'], geoloc, x['near'], x['retweet_date'] 
		]
		try:
			cursor.execute("insert into admin.user_sentiment values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20)", row)
			print('Inserted new tweet with ID {}'.format(x['link']))
			print("Currently on row: {}; Currently iterrated {}% of rows".format(index, (index + 1)/len(tweets_df.index) * 100))
			consecutive_exceptions = 0
		except Exception as e:
			consecutive_exceptions += 1
			#print('Can not insert {}. Continuing. {}'.format(row, e))



def read_file(path):
	f = open(path)
	word_list = f.readlines()
	for x in range(len(word_list)):
		word_list[x] = word_list[x].rstrip() # Clean spaces and delimiting characters
	f.close()
	return word_list



def main():
	yaml_data = process_yaml()
	conn = create_connection(yaml_data)

	ats = read_file('./data/ats.txt')
	keywords = read_file('./data/keywords.txt')

	for x in keywords:
		search(x, conn)

	for x in ats:
		search('@{}'.format(x), conn) # look for @ mentions and analyze sentiment
		search('#{}'.format(x), conn) # also look for the hashtags
		user(x, conn)


	conn.close()



if __name__ == '__main__':
	main()
