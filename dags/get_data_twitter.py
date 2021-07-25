# Import Libraries
import pandas as pd
import nltk
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('vader_lexicon')
import re
import string
from textblob import TextBlob
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from sklearn.feature_extraction.text import CountVectorizer
from datetime import *
from google.oauth2 import service_account
import pandas_gbq
from google.cloud import bigquery
import twint
import sqlalchemy

directory = '/usr/local/airflow/dags/'


date_since = datetime.now() - timedelta(1)
date_until = datetime.now()
date_since = datetime.strftime(date_since, '%Y-%m-%d')
date_until = datetime.strftime(date_until, '%Y-%m-%d')

c = twint.Config()
c.Search = "Covid"
c.Since = date_since
c.Until = date_until
c.Limit = 1000
c.Popular_tweets = True
c.Pandas = True

twint.run.Search(c)

df_tweets = twint.storage.panda.Tweets_df

df_tweets = df_tweets.loc[df_tweets['language']=='en']


#################################

# Create a function to clean the tweets
def cleanTxt(text):
 text = re.sub('@[A-Za-z0–9]+', '', text) 
 text = re.sub('#', '', text) 
 text = re.sub(':', '', text) 
 text = re.sub('RT[\s]+', '', text) 
 text = re.sub('https?\/\/\S+', '', text) 
 text = re.sub('https?:\/\/\S+', '', text) 
 
 return text


# Clean the tweets
df_tweets['text'] = df_tweets['tweet'].apply(cleanTxt)



#Calculating Negative, Positive, Neutral and Compound values
df_tweets[['polarity', 'subjectivity']] = df_tweets['text'].apply(lambda Text: pd.Series(TextBlob(Text).sentiment))
for index, row in df_tweets['text'].iteritems():
 score = SentimentIntensityAnalyzer().polarity_scores(row)
 neg = score['neg']
 neu = score['neu']
 pos = score['pos']
 comp = score['compound']
 if neg > pos:
     df_tweets.loc[index, 'sentiment'] = "negative"
 elif pos > neg:
     df_tweets.loc[index, 'sentiment'] = "positive"
 else:
     df_tweets.loc[index, 'sentiment'] = "neutral"
     df_tweets.loc[index, 'neg'] = neg
     df_tweets.loc[index, 'neu'] = neu
     df_tweets.loc[index, 'pos'] = pos
     df_tweets.loc[index, 'compound'] = comp
    



#Calculating tweet’s lenght and word count
df_tweets['text_len'] = df_tweets['text'].astype(str).apply(len)
df_tweets['text_word_count'] = df_tweets['text'].apply(lambda x: len(str(x).split()))

stopword = nltk.corpus.stopwords.words('english')

#Function to ngram
def get_top_n_gram(corpus,ngram_range,n=None):
 vec = CountVectorizer(ngram_range=ngram_range,stop_words=stopword).fit(corpus)
 bag_of_words = vec.transform(corpus)
 sum_words = bag_of_words.sum(axis=0) 
 words_freq = [(word, sum_words[0, idx]) for word, idx in vec.vocabulary_.items()]
 words_freq =sorted(words_freq, key = lambda x: x[1], reverse=True)
 return words_freq[:n]
#n2_bigram
n2_bigrams = get_top_n_gram(df_tweets['text'],(2,2),20)
n2_bigrams = pd.DataFrame(n2_bigrams)
n2_bigrams['date'] = date_since
n2_bigrams = n2_bigrams.rename(columns={0:"words",1:"total"})

#n3_trigram
n3_trigrams = get_top_n_gram(df_tweets['text'],(3,3),20)
n3_trigrams = pd.DataFrame(n3_trigrams)
n3_trigrams['date'] = date_until
n3_trigrams = n3_trigrams.rename(columns={0:"words",1:"total"})

df_tweets = df_tweets[["date","tweet","text","sentiment"]]

# Carregar no big query

pk_json_input = directory+"credentials/tcc-pucminas-bot-analytics-462531da5c8e.json"

project_input = "tcc-pucminas-bot-analytics"

auth = service_account.Credentials.from_service_account_file(pk_json_input)

df_tweets.to_gbq(
    'botanalytics_log.tb_twitter_data',credentials=auth, project_id=project_input,if_exists='append',
)

n2_bigrams.to_gbq(
    'botanalytics_log.tb_twitter_bigrams',credentials=auth, project_id=project_input,if_exists='append',
)

n3_trigrams.to_gbq(
    'botanalytics_log.tb_twitter_trigrams',credentials=auth, project_id=project_input,if_exists='append',
)

database_connection_try1 = sqlalchemy.create_engine('postgresql://airflow:airflow@dags_postgres_1/postgres')
database_connection_try2 = sqlalchemy.create_engine('postgresql://airflow:airflow@0.0.0.0/postgres')

#database_connection

#df_final_teste = df_final.tail(1)
try:
    df_tweets.to_sql(con=database_connection_try1,name='tb_twitter_data',if_exists='append')
except:
    df_tweets.to_sql(con=database_connection_try2,name='tb_twitter_data',if_exists='append')
    
try:
    n2_bigrams.to_sql(con=database_connection_try1,name='tb_twitter_bigrams',if_exists='append')
except:
    n2_bigrams.to_sql(con=database_connection_try2,name='tb_twitter_bigrams',if_exists='append')

try:
    n3_trigrams.to_sql(con=database_connection_try1,name='tb_twitter_trigrams',if_exists='append')
except:
    n3_trigrams.to_sql(con=database_connection_try2,name='tb_twitter_trigrams',if_exists='append')