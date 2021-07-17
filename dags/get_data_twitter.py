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


date_since = datetime.now() - timedelta(1)
date_until = datetime.now()
date_since = datetime.strftime(date_since, '%Y-%m-%d')
date_until = datetime.strftime(date_until, '%Y-%m-%d')

print("date defined")

c = twint.Config()
c.Search = "Covid"
c.Since = date_since
c.Until = date_until
c.Limit = 1000
c.Popular_tweets = True
c.Pandas = True
#.Min_likes = 1000
#c.Store_csv = True
#c.Output = ""

twint.run.Search(c)

df_tweets = twint.storage.panda.Tweets_df

df_tweets = df_tweets.loc[df_tweets['language']=='en']


directory = '/usr/local/airflow/dags/'

#################################

# Create a function to clean the tweets
def cleanTxt(text):
 text = re.sub('@[A-Za-z0–9]+', '', text) #Removing @mentions
 text = re.sub('#', '', text) # Removing '#' hash tag
 text = re.sub(':', '', text) # Removing '#' hash tag               
 text = re.sub('RT[\s]+', '', text) # Removing RT
 text = re.sub('https?\/\/\S+', '', text) # Removing https//
 text = re.sub('https?:\/\/\S+', '', text) # Removing https://
 
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
round(pd.DataFrame(df_tweets.groupby("sentiment").text_len.mean()),2)
round(pd.DataFrame(df_tweets.groupby("sentiment").text_word_count.mean()),2)


#Removing Punctuation
def remove_punct(text):
 text = "".join([char for char in text if char not in string.punctuation])
 text = re.sub('[0–9]+', '', text)
 return text
df_tweets['punct'] = df_tweets['text'].apply(lambda x: remove_punct(x))
#Appliyng tokenization
def tokenization(text):
    text = re.split('\W+', text)
    return text
df_tweets['tokenized'] = df_tweets['punct'].apply(lambda x: tokenization(x.lower()))
#Removing stopwords

stopword = nltk.corpus.stopwords.words('english')
def remove_stopwords(text):
    text = [word for word in text if word not in stopword]
    return text
    
df_tweets['nonstop'] = df_tweets['tokenized'].apply(lambda x: remove_stopwords(x))
#Appliyng Stemmer
ps = nltk.PorterStemmer()
def stemming(text):
    text = [ps.stem(word) for word in text]
    return text
df_tweets['stemmed'] = df_tweets['nonstop'].apply(lambda x: stemming(x))
#Cleaning Text
def clean_text(text):
    text_lc = "".join([word.lower() for word in text if word not in string.punctuation]) # remove puntuation
    text_rc = re.sub('[0-9]+', '', text_lc)
    tokens = re.split('\W+', text_rc)    # tokenization
    text = [ps.stem(word) for word in tokens if word not in stopword]  # remove stopwords and stemming
    return text

df_tweets['cleaned_text'] = df_tweets['stemmed'].apply(lambda x: clean_text(x))

df_tweets['text_clean'] = [','.join(map(str, l)) for l in df_tweets['stemmed']]
df_tweets['text_clean'] = df_tweets['text_clean'].str.replace(',',' ')

#Appliyng Countvectorizer


countVectorizer = CountVectorizer(analyzer=clean_text) 
countVector = countVectorizer.fit_transform(df_tweets['text_clean'])
print('{} Number of reviews has {} words'.format(countVector.shape[0], countVector.shape[1]))
#print(countVectorizer.get_feature_names())
#1281 Number of reviews has 2966 words
count_vect_df = pd.DataFrame(countVector.toarray(), columns=countVectorizer.get_feature_names())
count_vect_df.head()


# Most Used Words
count = pd.DataFrame(count_vect_df.sum())
countdf = count.sort_values(0,ascending=False).head(20)
countdf[1:11]


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


#Appliyng Countvectorizer

df_tweets = df_tweets[["date","tweet","punct","sentiment","text_clean"]]

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
    df_tweets.to_sql(con=database_connection_try1,name='tb_twitter_bigrams',if_exists='append')
except:
    df_tweets.to_sql(con=database_connection_try2,name='tb_twitter_bigrams',if_exists='append')

try:
    df_tweets.to_sql(con=database_connection_try1,name='tb_twitter_trigrams',if_exists='append')
except:
    df_tweets.to_sql(con=database_connection_try2,name='tb_twitter_trigrams',if_exists='append')