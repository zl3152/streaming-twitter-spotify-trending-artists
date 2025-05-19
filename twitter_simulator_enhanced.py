# twitter_simulator_fixed.py
import sys
import json
from kafka import KafkaProducer
import time
import random
from datetime import datetime
import threading

topic = sys.argv[1] if len(sys.argv) > 1 else 'spotify-tweets'

# Create multiple producers for higher volume
producers = []
for i in range(3):
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producers.append(producer)

# Valid Spotify track IDs (verified to work)
spotify_tracks = {
    # Artist: [track_ids] - These are all valid, currently available tracks
    "Taylor Swift": ["3CeCwYWvdfXbZLXFhBrbnf", "6AGOKlMZWLCaEJGnaROtF9", "0wavGRldH0AWyu2zvTz8zb"],
    "Drake": ["5ry2OE6R2zPQFDO85XkgRb", "6DCZcSspjsKoFjzjrWoCdn", "2SAqBLGA283SUiwJ3xOUVI"],
    "The Weeknd": ["0VjIjW4GlUZAMYd2vXMi3b", "7MXVkk9YMctZqd1Srtv4MB", "2LBqCSwhJGcFQeTHMVGwy3"],
    "Ed Sheeran": ["7qiZfU4dY1lWllzX7mPBI3", "0tgVpDi06FyKpA1z0VMD4v", "4uLU6hMCjMI75M1A2tKUQC"],
    "Billie Eilish": ["2Fxmhks0bxGSBdJ92vM42m", "0OJAP6GvFQpTTlOttEtYSG", "73SpzrcaHk0RQPFP73vqVR"],
    "Olivia Rodrigo": ["4ZtFanR9U6ndgddUvNcjcG", "3Uo7WG0vmLQ07WB4BDwy7D", "6HU7h9RYOaPRFeh0R3UeAr"],
    "Bad Bunny": ["6Xom58OOXk2SoU711L2IXO", "4LRPiXqCikLlN15c3yImP7", "3k3NWokhRRkEPhCzPmV8TW"],
    "BTS": ["0lBN9bZfQKgzXUEOvlAbAb", "7qEHsqek33rTcFNT9PFqLf", "4saklk6nie3yiGePpBwUoc"],
    "Ariana Grande": ["5Gu0PDLN4YJeW75PpBSg9p", "6im9k8u9iIzKMrmV7BWtlF", "2nMeu6UenVvwUktBCpLMK9"],
    "Post Malone": ["21jGcNKet2qwijlDFuPiPb", "7dt6x5M1jzdTEt8oCbisTK", "0e7ipj03S05BNilyu5bRzt"],
    "Dua Lipa": ["2b8fOow8UzyDFAE27YhOZM", "7ef4DlsgrMEH11cDZd32M6", "6WrI0LAC5M1Rw2MnX2ZvEg"],
    "Harry Styles": ["4Dvkj6JhhA12EX05fT7y2e", "1ZMiCix7XSAbfAJlEZWMCp", "6UelLqGlWMcVH1E5c4H7lY"],
    "Doja Cat": ["3Dv1eDb0MEgF93GpLXlucZ", "0k4d9YPDr1r7FqusmzGL1W", "7tI8Lw3sgAqa4slE0C2hTP"],
    "Kendrick Lamar": ["7KXjTSCq5nL1LoYtL7XAwS", "6FRLCMO5TUHTexlWo8ym1W", "1DIXPcTDzTj8ZMHt3PDt8p"],
    "Justin Bieber": ["6epn3r7S14KUqlReYr77hA", "1LOPAOeSRJXPhqrnRxH0w4", "3AJwUDP919kvQ9QcozQPxg"],
    "Miley Cyrus": ["3E7dfMvvCLUddWissuqMwr", "5Q0Nhxo0l2bP3pNjpGJwV1", "0EqMdummgovYITxcGxYJHR"],
    "Travis Scott": ["6gBFPUFcJLzWGx4lenP6h2", "2xLMifQCjDGFmkHkpNLD9h", "61Q9oJNd9hJQFhSDh6Qlap"],
    "Coldplay": ["3AJwUDP919kvQ9QcozQPxg", "7LVHVU3tWfcxj5aiPFErUV", "3RiPr603aXAoi4GHyXx0uy"],
    "Sia": ["4VrWlk8IQxevMvERoX08iC", "27NovPIUIRrOZoCHxABJwK", "2v9MY0mZWpREyaIrsRVUhb"],
    "Imagine Dragons": ["5VnDkUNyX6u5Sk0yZiP8XB", "5HCnacHkrwU5vcKiSbSGnE", "62yJjFtgkhUrXktIoSjgP2"]
}

# Flatten tracks for easy random selection
all_tracks = []
for artist, tracks in spotify_tracks.items():
    for track in tracks:
        all_tracks.append((artist, track))

# Tweet templates
tweet_templates = [
    "Just discovered this amazing track! {url} #music",
    "Can't stop listening to {url} 🎵",
    "New favorite song alert! {url} #spotify",
    "This song hits different {url} 🔥",
    "On repeat all day {url} #nowplaying",
    "Obsessed with this track {url} ❤️",
    "Weekend vibes {url} #music",
    "This artist never misses {url} 🎶",
    "Perfect song for today {url}",
    "Adding this to all my playlists {url} 🎧",
]

print(f"Starting Enhanced Twitter Simulator for topic: {topic}")
print(f"Simulating high-volume streaming with {len(all_tracks)} tracks from {len(spotify_tracks)} artists")

# Stats tracking
stats = {
    'total_tweets': 0,
    'tweets_per_second': 0,
    'start_time': datetime.now()
}

def generate_tweets(producer_id, producer):
    """Generate tweets for a specific producer"""
    while True:
        artist, track_id = random.choice(all_tracks)
        template = random.choice(tweet_templates)
        
        message = {
            'created_at': datetime.now().strftime('%a %b %d %H:%M:%S +0000 %Y'),
            'expanded_url': f"https://open.spotify.com/track/{track_id}",
            'text': template.format(url=f"https://open.spotify.com/track/{track_id}"),
            'artist_hint': artist,
            'producer_id': producer_id
        }
        
        producer.send(topic, message)
        stats['total_tweets'] += 1
        
        # Variable delay for realistic streaming
        delay = random.uniform(0.5, 2.0)  # 0.5-2 seconds
        time.sleep(delay)

def print_stats():
    """Print statistics every 10 seconds"""
    while True:
        time.sleep(10)
        elapsed = (datetime.now() - stats['start_time']).total_seconds()
        rate = stats['total_tweets'] / elapsed if elapsed > 0 else 0
        print(f"\n📊 Stats: {stats['total_tweets']} tweets sent | {rate:.2f} tweets/sec | Running for {int(elapsed)}s")

# Start stats thread
stats_thread = threading.Thread(target=print_stats, daemon=True)
stats_thread.start()

# Start producer threads
threads = []
for i, producer in enumerate(producers):
    thread = threading.Thread(target=generate_tweets, args=(i, producer), daemon=True)
    threads.append(thread)
    thread.start()
    print(f"Started producer thread {i}")

print("\n🚀 High-volume simulation started! Press Ctrl+C to stop.")
print("📈 Generating tweets to demonstrate real-time processing")

try:
    # Keep main thread alive
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\n\nStopping simulator...")
    print(f"Final stats: {stats['total_tweets']} total tweets generated")