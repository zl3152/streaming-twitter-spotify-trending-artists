# Create fixed version
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
with open('credential.json', 'r') as f:
    credentials = json.load(f)

# Spotify setup
spotify_client_id = credentials['SPOTIFY_CLIENT_ID']
spotify_client_secret = credentials['SPOTIFY_CLIENT_SECRET']
client_credentials_manager = SpotifyClientCredentials(client_id=spotify_client_id, client_secret=spotify_client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Cassandra setup
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

# Create keyspace and table if they don't exist
keyspace = "spotify"
table = "artistshare"
session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
session.set_keyspace(keyspace)
session.execute(f"""
    CREATE TABLE IF NOT EXISTS {table} (
        created_at timestamp,
        artist text,
        count int,
        PRIMARY KEY(artist, created_at)
    )
""")

# Spark setup with legacy datetime parser
spark = SparkSession.builder \
    .appName("SpotifyKafkaStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("ERROR")

# Kafka parameters
kafka_topic = sys.argv[1] if len(sys.argv) > 1 else 'spotify-tweets'
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": kafka_topic,
    "startingOffsets": "latest"
}

print(f"Starting Kafka streaming from topic: {kafka_topic}")

# Define schema for incoming data
schema = StructType([
    StructField("created_at", StringType()),
    StructField("expanded_url", StringType())
])

# Function to extract track ID from Spotify URL
def extract_track_id(url):
    if url and 'spotify.com/track/' in url:
        return url.split('/track/')[-1].split('?')[0]
    return None

# Function to get artist from Spotify
def get_artist_from_spotify(track_id):
    try:
        if track_id:
            track = sp.track(track_id)
            if track and 'artists' in track and len(track['artists']) > 0:
                return track['artists'][0]['name']
    except Exception as e:
        print(f"Error getting artist for track {track_id}: {e}")
    return None

# Function to parse Twitter date format and return string for Cassandra
def parse_twitter_date(date_str):
    try:
        # Parse Twitter's date format: "Sat May 10 17:00:00 +0000 2025"
        dt = datetime.strptime(date_str, '%a %b %d %H:%M:%S %z %Y')
        # Return ISO format string that Cassandra can parse
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        # Return current time if parsing fails
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# UDFs
get_artist_udf = udf(get_artist_from_spotify, StringType())
extract_track_id_udf = udf(extract_track_id, StringType())
parse_date_udf = udf(parse_twitter_date, StringType())

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .options(**kafka_params) \
    .load()

# Parse JSON data
parsed_df = df.select(
    col("value").cast("string").alias("json_str")
).select(
    from_json(col("json_str"), schema).alias("data")
).select(
    col("data.created_at"),
    col("data.expanded_url")
)

# Extract track ID and get artist
artist_df = parsed_df \
    .withColumn("track_id", extract_track_id_udf(col("expanded_url"))) \
    .withColumn("artist", get_artist_udf(col("track_id"))) \
    .filter(col("artist").isNotNull()) \
    .withColumn("tweet_count", lit(1)) \
    .withColumn("created_at_str", parse_date_udf(col("created_at")))

# Write to Cassandra using foreachBatch
def write_to_cassandra(batch_df, batch_id):
    if not batch_df.isEmpty():
        # Convert to list of rows
        rows = batch_df.select("created_at_str", "artist", "tweet_count").collect()
        
        # Insert into Cassandra
        for row in rows:
            created_at_str = row['created_at_str']
            artist = row['artist']
            tweet_count = row['tweet_count']
            
            # Convert string to timestamp for Cassandra
            created_at = datetime.strptime(created_at_str, '%Y-%m-%d %H:%M:%S')
            
            query = f"""
            INSERT INTO {keyspace}.{table} (created_at, artist, count)
            VALUES (%s, %s, %s)
            """
            
            try:
                session.execute(query, (created_at, artist, tweet_count))
                print(f"Inserted: {artist} at {created_at}")
            except Exception as e:
                print(f"Error inserting into Cassandra: {e}")
                print(f"Data: created_at={created_at}, artist={artist}, count={tweet_count}")

# Start the streaming query
query = artist_df.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("append") \
    .trigger(processingTime='5 seconds') \
    .start()

print("Streaming started. Waiting for data...")
query.awaitTermination()
