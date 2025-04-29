from kafka import KafkaConsumer
import json
import mysql.connector
from mysql.connector import errorcode


# Kafka Configuration
KAFKA_TOPIC = 'youtube_most_viewed'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

# MySQL Configuration
DB_HOST = 'localhost'
DB_USER = 'root'  # Change this to your MySQL username
DB_PASSWORD = 'Yvss@123'  # Change this to your MySQL password
DB_NAME = 'youtube_videos'  # Database name

# Create the database if it doesn't exist
def create_database():
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Create database if it does not exist
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        print(f"Database `{DB_NAME}` is ready!")

        conn.commit()
        conn.close()
    except mysql.connector.Error as err:
        print(f"Error creating database: {err}")
        exit(1)

# Initialize MySQL database and tables
def init_db():
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    cursor = conn.cursor()

    # Temp table (raw incoming data)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS temp_videos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            video_id VARCHAR(255) UNIQUE,
            title TEXT,
            view_count INT,
            published_at DATETIME,
            channel_title TEXT
        )
    ''')

    # Final table (deduplicated data)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS final_videos (
            video_id VARCHAR(255) PRIMARY KEY,
            title TEXT,
            view_count INT,
            published_at DATETIME,
            channel_title TEXT
        )
    ''')

    conn.commit()
    conn.close()

from datetime import datetime

# Store data into temp table
def store_temp_video(data):
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    cursor = conn.cursor()

    # Convert the 'published_at' field to MySQL-compatible format
    published_at = data['published_at']
    try:
        # Parse the timestamp (assuming the format is ISO 8601 with 'Z' at the end)
        published_at = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        print(f"Error parsing date: {published_at}")
        return  # Exit if there's an invalid date format

    # Insert the data into the temp_videos table
    cursor.execute('''
        INSERT INTO temp_videos (video_id, title, view_count, published_at, channel_title)
        VALUES (%s, %s, %s, %s, %s)
    ''', (data['video_id'], data['title'], data['view_count'], published_at, data['channel_title']))

    conn.commit()
    conn.close()


# Deduplicate and move to final table
def deduplicate_to_final():
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    cursor = conn.cursor()

    cursor.execute('''
        INSERT IGNORE INTO final_videos (video_id, title, view_count, published_at, channel_title)
        SELECT video_id, title, view_count, published_at, channel_title
        FROM temp_videos
    ''')

    conn.commit()
    conn.close()

# Kafka Consumer
def consume_from_kafka():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='youtube_consumer_group'
    )

    print("\n📥 Listening to Kafka topic and storing into temp table...\n")

    for message in consumer:
        video_data = message.value
        print(f"Received from Kafka: {video_data['title']} ({video_data['video_id']})")

        store_temp_video(video_data)
        deduplicate_to_final()

# === Main Program ===
if __name__ == "__main__":
    create_database()  # Create the database if it doesn't exist
    init_db()  # Initialize DB and create tables
    consume_from_kafka()  # Start consuming from Kafka and processing the data












