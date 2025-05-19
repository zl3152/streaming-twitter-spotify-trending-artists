# # kafkaStreaming_fixed.py
# import sys
# import json
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
# import spotipy
# from spotipy.oauth2 import SpotifyClientCredentials
# from cassandra.cluster import Cluster
# from datetime import datetime
# import pytz

# # Load credentials
# with open('credential.json', 'r') as f:
#     credentials = json.load(f)

# # Spotify setup
# client_credentials_manager = SpotifyClientCredentials(
#     client_id=credentials['SPOTIFY_CLIENT_ID'],
#     client_secret=credentials['SPOTIFY_CLIENT_SECRET']
# )
# sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# # Cassandra setup
# cluster = Cluster(['127.0.0.1'])
# session = cluster.connect('spotify')

# # Spark setup
# spark = SparkSession.builder \
#     .appName("SpotifyKafkaStreaming") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
#     .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("ERROR")

# # Kafka parameters
# kafka_topic = sys.argv[1] if len(sys.argv) > 1 else 'spotify-tweets'
# kafka_params = {
#     "kafka.bootstrap.servers": "localhost:9092",
#     "subscribe": kafka_topic,
#     "startingOffsets": "latest"
# }

# print(f"Starting Kafka streaming from topic: {kafka_topic}")

# # Define schema
# schema = StructType([
#     StructField("created_at", StringType()),
#     StructField("expanded_url", StringType())
# ])

# def extract_track_id(url):
#     if url and 'spotify.com/track/' in url:
#         return url.split('/track/')[-1].split('?')[0]
#     return None

# def get_artist_from_spotify(track_id):
#     try:
#         if track_id:
#             track = sp.track(track_id)
#             if track and 'artists' in track and len(track['artists']) > 0:
#                 return track['artists'][0]['name']
#     except Exception as e:
#         print(f"Error getting artist for track {track_id}: {e}")
#     return None

# # UDFs
# get_artist_udf = udf(get_artist_from_spotify, StringType())
# extract_track_id_udf = udf(extract_track_id, StringType())

# # Read from Kafka
# df = spark.readStream \
#     .format("kafka") \
#     .options(**kafka_params) \
#     .load()

# # Parse JSON data
# parsed_df = df.select(
#     col("value").cast("string").alias("json_str")
# ).select(
#     from_json(col("json_str"), schema).alias("data")
# ).select(
#     col("data.created_at"),
#     col("data.expanded_url")
# )

# # Extract track ID and get artist
# artist_df = parsed_df \
#     .withColumn("track_id", extract_track_id_udf(col("expanded_url"))) \
#     .withColumn("artist", get_artist_udf(col("track_id"))) \
#     .filter(col("artist").isNotNull()) \
#     .withColumn("tweet_count", lit(1))

# # Write to Cassandra using foreachBatch
# def write_to_cassandra(batch_df, batch_id):
#     if not batch_df.isEmpty():
#         rows = batch_df.select("created_at", "artist", "tweet_count").collect()
        
#         for row in rows:
#             try:
#                 # Parse the Twitter date format directly
#                 created_at_str = row['created_at']
#                 created_at = datetime.strptime(created_at_str, '%a %b %d %H:%M:%S %z %Y')
                
#                 # Convert to UTC if needed
#                 if created_at.tzinfo is not None:
#                     created_at = created_at.astimezone(pytz.UTC)
                
#                 artist = row['artist']
#                 tweet_count = row['tweet_count']
                
#                 query = """
#                 INSERT INTO spotify.artistshare (created_at, artist, count)
#                 VALUES (%s, %s, %s)
#                 """
                
#                 session.execute(query, (created_at, artist, tweet_count))
#                 print(f"Inserted: {artist} at {created_at}")
                
#                 # Verify insertion
#                 check_query = "SELECT * FROM spotify.artistshare WHERE artist = %s AND created_at = %s"
#                 result = session.execute(check_query, (artist, created_at))
#                 if result:
#                     print(f"Verified: Data exists in Cassandra")
                
#             except Exception as e:
#                 print(f"Error inserting: {e}")
#                 import traceback
#                 traceback.print_exc()

# # Start the streaming query
# query = artist_df.writeStream \
#     .foreachBatch(write_to_cassandra) \
#     .outputMode("append") \
#     .trigger(processingTime='5 seconds') \
#     .start()

# print("Streaming started. Waiting for data...")
# query.awaitTermination()


import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from cassandra.cluster import Cluster
from datetime import datetime
import pytz

# Load credentials
with open('credential.json', 'r') as f:
    credentials = json.load(f)

# Spotify setup
client_credentials_manager = SpotifyClientCredentials(
    client_id=credentials['SPOTIFY_CLIENT_ID'],
    client_secret=credentials['SPOTIFY_CLIENT_SECRET']
)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Cassandra setup
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('spotify')

# Spark setup
spark = SparkSession.builder \
    .appName("SpotifyKafkaStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

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

def extract_track_id(url):
    if url and 'spotify.com/track/' in url:
        return url.split('/track/')[-1].split('?')[0]
    return None

def get_artist_from_spotify(track_id):
    try:
        if track_id:
            track = sp.track(track_id)
            if track and 'artists' in track and len(track['artists']) > 0:
                return track['artists'][0]['name']
    except Exception as e:
        print(f"Error getting artist for track {track_id}: {e}")
    return None

# UDFs
get_artist_udf = udf(get_artist_from_spotify, StringType())
extract_track_id_udf = udf(extract_track_id, StringType())

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
    .withColumn("tweet_count", lit(1))

# Write to Cassandra using foreachBatch
def write_to_cassandra(batch_df, batch_id):
    if not batch_df.isEmpty():
        rows = batch_df.select("created_at", "artist", "tweet_count").collect()
        
        for row in rows:
            try:
                # Parse the Twitter date format directly
                created_at_str = row['created_at']
                created_at = datetime.strptime(created_at_str, '%a %b %d %H:%M:%S %z %Y')
                
                # Convert to UTC if needed
                if created_at.tzinfo is not None:
                    created_at = created_at.astimezone(pytz.UTC)
                
                artist = row['artist']
                tweet_count = row['tweet_count']
                
                query = """
                INSERT INTO spotify.artistshare (created_at, artist, count)
                VALUES (%s, %s, %s)
                """
                
                session.execute(query, (created_at, artist, tweet_count))
                print(f"Inserted: {artist} at {created_at}")
                
            except Exception as e:
                print(f"Error inserting: {e}")

# Start the streaming query
query = artist_df.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("append") \
    .trigger(processingTime='5 seconds') \
    .start()

print("Streaming started. Waiting for data...")
query.awaitTermination()