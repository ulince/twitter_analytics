from DB import DB
import tweepy
import sys
import time
from nltk.tokenize import TweetTokenizer
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from collections import defaultdict
import os
import string

json_dir = r".\\json\\"
vocabulary_file = r"vocabulary.json"
unique_tweets_file = r"unique.json"

current_constituent = ['BMW']

all_constituents_1 = ['DAX', 'Allianz', 'adidas',
                    'BASF', 'Bayer', 'Beiersdorf',
                    'BMW', 'Commerzbank','Continental',
                    'Daimler']
all_constituents_2 = ['Deutsche Bank', 'Deutsche Börse',
                    'Deutsche Post', 'Deutsche Telekom', 'EON',
                    'Fresenius Medical Care', 'Fresenius', 'HeidelbergCement',
                    'Henkel vz', 'Infineon']
all_constituents_3 = ['Linde',
                    'Lufthansa', 'Merck',
                    'Münchener Rückversicherungs-Gesellschaft', 'ProSiebenSat1 Media',
                    'RWE', 'SAP', 'Siemens',
                    'thyssenkrupp', 'Volkswagen','Vonovia']

''''
args:
0: connection string
1: database
2: collection
3: language

'''


def main(argv):
    API_KEY = "fAFENmxds3YFgUqHt974ZGsov"
    API_SECRET = 'zk8IRc6WQPZ8dc2yGh8gJClEMDlL6I3L4DYIC4ZkoHvjIw4QgN'

    api = load_api(API_KEY, API_SECRET)

    # this is what we're searching for
    maxTweets = 10000000  # Some arbitrary large number
    #maxTweets = 10
    tweetsPerQry = 100  # this is the max the API permits
    language = argv[3]

    database = DB(argv[0], argv[1])

    for constituent in current_constituent:
        #searchQuery = constituent + " " + "-sale"
        searchQuery = constituent
        sinceId = None
        max_id = -1
        tweetCount = 0


        print("Downloading max {0} tweets".format(maxTweets))
        while tweetCount < maxTweets:
            list_of_tweets = []
            try:
                if (max_id <= 0):
                    if (not sinceId):
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry, lang=language)
                    else:
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry,
                                                since_id=sinceId, lang=language)
                else:
                    if (not sinceId):
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry,
                                                max_id=str(max_id - 1), lang=language)
                    else:
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry,
                                                max_id=str(max_id - 1),
                                                since_id=sinceId, lang=language)
                if not new_tweets:
                    print("No more tweets found")
                    break
                for tweet in new_tweets:
                    document = tweet._json

                    processed_text = preprocess_tweet(document['text'], language)
                    if "bmw" not in processed_text:
                        continue

                    if language == 'en':
                        document['processed_text'] = processed_text
                    if language == 'de':
                        document['processed_text_de'] = processed_text
                    document['search_term'] = searchQuery
                    document['constituent'] = constituent


                    if 'retweeted_status' in document:
                        ''''
                        original_tweet = document['retweeted_status']
                        original_tweet['processed_text'] = preprocess_tweet(original_tweet['text'])
                        original_tweet['search_term'] = constituent
                        original_tweet['constituent'] = constituent

                        list_of_tweets.append(original_tweet)
                        '''
                        document.pop('retweeted_status', None)


                    list_of_tweets.append(document)

                database.bulk_insert(argv[2], list_of_tweets)

                tweetCount += len(new_tweets)
                print("Downloaded {0} tweets".format(tweetCount))
                max_id = new_tweets[-1].id
            except tweepy.TweepError as e:
                # Just exit if any error
                print("some error : " + str(e))
                break

        print("Downloaded {0} total tweets for {1}".format(tweetCount, constituent))
        #time.sleep(15 * 60)


def load_api(API_KEY, API_SECRET):
    auth = tweepy.AppAuthHandler(API_KEY, API_SECRET)

    api = tweepy.API(auth, wait_on_rate_limit=True,
                     wait_on_rate_limit_notify=True)

    if (not api):
        print("Can't Authenticate")
        sys.exit(-1)
    else:
        return api

def preprocess_tweet(text:str, language):
    if language == 'en':
        lang = 'english'
    if language == 'de':
        lang = 'german'

    #Tokenize the tweet text
    tokenizer = TweetTokenizer(preserve_case=False,reduce_len=True,strip_handles=False)
    tokens = tokenizer.tokenize(text)
    #remove stop words
    stop_words = set(stopwords.words('english'))
    punct = string.punctuation
    punct_1 = punct.replace('#', '')
    punct_2 = punct_1.replace('@', '')
    stop_words.update(punct_2)
    stop_words.add('...')

    filtered_tokens = [word for word in tokens if not word in stop_words]
    filtered_no_url = [word for word in filtered_tokens if not 'http' in word]

    #stemming
    if language == 'en':
        stemmer = PorterStemmer()
        stemmed_tokens = [stemmer.stem(word) if (word[0] != '#' and word[0] != '@' ) else word for word in filtered_no_url]
        return stemmed_tokens
    if language == 'german':
        return filtered_no_url

    return None



if __name__ == "__main__":
    main(sys.argv[1:])