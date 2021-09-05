import twint
import schedule
import time

# you can change the name of each "job" after "def" if you'd like.
def jobone(filter):
	print ("Fetching Tweets")
	c = twint.Config()
	# choose username (optional)
	#c.Username = x
	# choose search term (optional)
	c.Search = '{}'.format(filter)
	# choose beginning time (narrow results)
	c.Since = "2018-01-01"
	# set limit on total tweets
	c.Limit = 10
	c.Pandas = True

	'''c.Format = '{}''username', 'created_at', 'timestamp', 'user_id', 'username', 'name', 'place', 'tweet', 'mentions', 'urls', 'photos', 'replies_count', 'retweets_count',
		'likes_count', 'hashtags', 'link', 'retweet', 'video', 'near', 'geo', 'source', 'retweet_date']'''
	'''
	# format of the csv
	c.Custom = ["date", "time", "username", "tweet", "link", "likes", "retweets", "replies", "mentions", "hashtags"]
	# change the name of the csv file
	c.Output = "filename.csv"
	'''
	twint.run.Search(c)
	tweets_df = twint.storage.panda.Tweets_df
	print(tweets_df)



def jobtwo(x):
	print ("Fetching Tweets")
	c = twint.Config()
	# choose username (optional)
	c.Username = x
	# choose search term (optional)
	#c.Search = ''
	# choose beginning time (narrow results)
	c.Since = "2018-01-01"
	# set limit on total tweets
	#c.Limit = 1000000
	# no idea, but makes the csv format properly
	c.Store_csv = False
	# format of the csv
	c.Custom = ["date", "time", "username", "tweet", "link", "likes", "retweets", "replies", "mentions", "hashtags"]
	# change the name of the csv file
	c.Output = "filename.csv"
	twint.run.Search(c)



filters = ['@oracle', '@google', '@awscloud', '@googlecloud', '@googlecloudtech', '@oraclecx', '@oracleopenworld', '@oracledatabase', '@oraclemarketing', '@ibm', '@ibmcloud']
for x in filters:
	jobone(x) # look for Oracle mentions and analyze sentiment