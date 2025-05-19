# twitter_search_periodic.py
import tweepy
import json
from kafka import KafkaProducer
import time
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load credentials
with open('credential.json', 'r') as f:
    credentials = json.load(f)

# Twitter client - v2 API
client = tweepy.Client(
    consumer_key=credentials['twitter_api_key'],
    consumer_secret=credentials['twitter_api_secret'],
    access_token=credentials['twitter_token'],
    access_token_secret=credentials['twitter_token_secret'],
    wait_on_rate_limit=True
)

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'spotify-tweets'
last_tweet_id = None
tweet_count = 0
monthly_limit = 1500  # Free tier limit

logger.info("Starting Twitter periodic search for Spotify links...")

while tweet_count < monthly_limit:
    try:
        # Search for tweets with Spotify links
        logger.info(f"Searching for tweets... (Total this month: {tweet_count})")
        
        tweets = client.search_recent_tweets(
            query='open.spotify.com/track -is:retweet lang:en',
            max_results=10,  # Conservative to stay within limits
            since_id=last_tweet_id,
            tweet_fields=['created_at', 'entities', 'public_metrics', 'author_id'],
        )
        
        if tweets.data:
            logger.info(f"Found {len(tweets.data)} tweets")
            
            for tweet in tweets.data:
                if tweet.entities and 'urls' in tweet.entities:
                    for url in tweet.entities['urls']:
                        expanded_url = url.get('expanded_url', '')
                        if 'open.spotify.com/track' in expanded_url:
                            message = {
                                'created_at': tweet.created_at.strftime('%a %b %d %H:%M:%S +0000 %Y'),
                                'expanded_url': expanded_url,
                                'tweet_id': str(tweet.id),
                                'author_id': str(tweet.author_id) if tweet.author_id else None,
                                'retweet_count': tweet.public_metrics.get('retweet_count', 0),
                                'like_count': tweet.public_metrics.get('like_count', 0)
                            }
                            producer.send(topic, message)
                            logger.info(f"Sent tweet with Spotify link: {expanded_url}")
                            tweet_count += 1
            
            # Update last_tweet_id to avoid duplicates
            if tweets.data:
                last_tweet_id = tweets.data[0].id
            producer.flush()
        else:
            logger.info("No new tweets found")
        
        # Rate limiting: 180 requests per 15 minutes = 1 request every 5 seconds
        # We'll be more conservative with 1 request per minute
        logger.info(f"Waiting 60 seconds before next search... (Monthly count: {tweet_count}/{monthly_limit})")
        time.sleep(60)
        
    except tweepy.TooManyRequests:
        logger.warning("Rate limit reached. Waiting 15 minutes...")
        time.sleep(900)  # Wait 15 minutes
    except tweepy.Unauthorized:
        logger.error("Unauthorized: Check your Twitter API credentials")
        break
    except Exception as e:
        logger.error(f"Error: {e}")
        time.sleep(60)

logger.warning(f"Monthly limit reached: {tweet_count} tweets")