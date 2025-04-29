from googleapiclient.discovery import build
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json

# Replace with your actual YouTube API key and Kafka config
API_KEY = 'AIzaSyDJgPiL-a4n5XJzYp-Ps8pvroup3nl49gI'
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'
KAFKA_TOPIC = 'youtube_most_viewed'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']  # Update if using different host/port

def get_most_viewed_video_last_7_days(channel_id):
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)
    seven_days_ago = (datetime.utcnow() - timedelta(days=7)).isoformat("T") + "Z"

    search_request = youtube.search().list(
        part="id",
        channelId=channel_id,
        publishedAfter=seven_days_ago,
        type="video",
        maxResults=50
    )
    search_response = search_request.execute()
    video_ids = [item['id']['videoId'] for item in search_response.get('items', [])]

    if not video_ids:
        return None

    video_request = youtube.videos().list(
        part="snippet,statistics",
        id=','.join(video_ids)
    )
    video_response = video_request.execute()

    most_viewed = None
    max_views = -1

    for item in video_response['items']:
        view_count = int(item['statistics'].get('viewCount', 0))
        if view_count > max_views:
            max_views = view_count
            most_viewed = {
                'video_id': item['id'],
                'title': item['snippet']['title'],
                'view_count': view_count,
                'published_at': item['snippet']['publishedAt'],
                'channel_title': item['snippet']['channelTitle']
            }

    return most_viewed

def publish_to_kafka(data, topic, servers):
    producer = KafkaProducer(
        bootstrap_servers=servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send(topic, data)
    producer.flush()
    print(f"\n✅ Published to Kafka topic: {topic}")

# === Main Program ===
if __name__ == "__main__":
    channel_id = input("Enter YouTube channel ID: ").strip()
    most_viewed_video = get_most_viewed_video_last_7_days(channel_id)

    if most_viewed_video:
        print("\nMost Viewed Video in the Last 7 Days:")
        for key, value in most_viewed_video.items():
            print(f"{key}: {value}")

        publish_to_kafka(most_viewed_video, KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS)
    else:
        print("No videos found in the last 7 days.")
