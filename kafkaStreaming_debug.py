# kafkaStreaming_debug2.py
import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from cassandra.cluster import Cluster
from datetime import datetime

# Load credentials
print("Loading credentials...")
with open('credential.json', 'r') as f:
    credentials = json.load(f)

# Spotify setup
print("Setting up Spotify...")
client_credentials_manager = SpotifyClientCredentials(
    client_id=credentials['SPOTIFY_CLIENT_ID'],
    client_secret=credentials['SPOTIFY_CLIENT_SECRET']
)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Cassandra setup
print("Connecting to Cassandra...")
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('spotify')

# Spark setup
print("Setting up Spark...")
spark = SparkSession.builder \
    .appName("SpotifyKafkaStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka parameters
kafka_topic = sys.argv[1] if len(sys.argv) > 1 else 'spotify-tweets'
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": kafka_topic,
    "startingOffsets": "latest"
}

print(f"Starting Kafka streaming from topic: {kafka_topic}")

# Define schema
schema = StructType([
    StructField("created_at", StringType()),
    StructField("expanded_url", StringType())
])

# UDFs with debug output
def extract_track_id(url):
    if url and 'spotify.com/track/' in url:
        track_id = url.split('/track/')[-1].split('?')[0]
        print(f"Extracted track ID: {track_id} from {url}")
        return track_id
    return None

def get_artist_from_spotify(track_id):
    try:
        if track_id:
            print(f"Looking up track: {track_id}")
            track = sp.track(track_id)
            if track and 'artists' in track and len(track['artists']) > 0:
                artist = track['artists'][0]['name']
                print(f"Found artist: {artist}")
                return artist
    except Exception as e:
        print(f"Error getting artist for track {track_id}: {e}")
    return None

def parse_twitter_date(date_str):
    try:
        dt = datetime.strptime(date_str, '%a %b %d %H:%M:%S %z %Y')
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Register UDFs
get_artist_udf = udf(get_artist_from_spotify, StringType())
extract_track_id_udf = udf(extract_track_id, StringType())
parse_date_udf = udf(parse_twitter_date, StringType())

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .options(**kafka_params) \
    .load()

# Debug: Show raw Kafka data
def debug_raw_data(batch_df, batch_id):
    print(f"\n=== Batch {batch_id} ===")
    if not batch_df.isEmpty():
        print(f"Batch size: {batch_df.count()}")
        batch_df.select("value").show(5, truncate=False)
    else:
        print("Empty batch")

# First, let's just see what's coming from Kafka
query1 = df.writeStream \
    .foreachBatch(debug_raw_data) \
    .outputMode("append") \
    .trigger(processingTime='5 seconds') \
    .start()

print("Debug streaming started. Waiting for data...")

# Wait a bit to see raw data
import time
time.sleep(15)
query1.stop()

# Now let's process the data
print("\nNow processing data...")

parsed_df = df.select(
    col("value").cast("string").alias("json_str")
).select(
    from_json(col("json_str"), schema).alias("data")
).select(
    col("data.created_at"),
    col("data.expanded_url")
)

artist_df = parsed_df \
    .withColumn("track_id", extract_track_id_udf(col("expanded_url"))) \
    .withColumn("artist", get_artist_udf(col("track_id"))) \
    .filter(col("artist").isNotNull()) \
    .withColumn("tweet_count", lit(1)) \
    .withColumn("created_at_str", parse_date_udf(col("created_at")))

def write_to_cassandra(batch_df, batch_id):
    print(f"\n=== Processing Batch {batch_id} for Cassandra ===")
    if not batch_df.isEmpty():
        print(f"Batch size: {batch_df.count()}")
        batch_df.show(5, truncate=False)
        
        rows = batch_df.select("created_at_str", "artist", "tweet_count").collect()
        
        for row in rows:
            created_at_str = row['created_at_str']
            artist = row['artist']
            tweet_count = row['tweet_count']
            
            created_at = datetime.strptime(created_at_str, '%Y-%m-%d %H:%M:%S')
            
            query = "INSERT INTO artistshare (created_at, artist, count) VALUES (%s, %s, %s)"
            
            try:
                session.execute(query, (created_at, artist, tweet_count))
                print(f"Successfully inserted: {artist} at {created_at}")
            except Exception as e:
                print(f"Error inserting: {e}")
    else:
        print("Empty batch for Cassandra")

# Start the main processing
query2 = artist_df.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("append") \
    .trigger(processingTime='5 seconds') \
    .start()

query2.awaitTermination()