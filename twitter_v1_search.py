# twitter_v1_search.py
import tweepy
import json
from kafka import KafkaProducer
import time

# Load credentials
with open('credential.json', 'r') as f:
    creds = json.load(f)

# Kafka setup
topic = 'spotify-tweets'
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Twitter v1.1 API setup
auth = tweepy.OAuth1UserHandler(
    creds['twitter_api_key'],
    creds['twitter_api_secret'],
    creds['twitter_token'],
    creds['twitter_token_secret']
)
api = tweepy.API(auth)

print("Starting Twitter v1.1 search...")

while True:
    try:
        # Search using v1.1 API
        tweets = api.search_tweets(q='spotify.com', count=10, result_type='recent')
        
        for tweet in tweets:
            if hasattr(tweet, 'entities') and 'urls' in tweet.entities:
                for url in tweet.entities['urls']:
                    if 'spotify.com' in url.get('expanded_url', ''):
                        message = {
                            'created_at': tweet.created_at.strftime('%a %b %d %H:%M:%S +0000 %Y'),
                            'expanded_url': url['expanded_url']
                        }
                        producer.send(topic, message)
                        print(f"Sent: {message}")
        
        producer.flush()
        time.sleep(30)  # Wait 30 seconds between searches
        
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(60)