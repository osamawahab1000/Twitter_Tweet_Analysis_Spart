from kafka import KafkaProducer
import pandas as pd
import json
import time

# Step 1: Load 16M Tweets from CSV
df = pd.read_csv('training.1600000.processed.noemoticon.csv', encoding='latin-1', header=None)
tweets = df[5].tolist()

# Step 2: Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Step 3: Send tweets
for idx, tweet in enumerate(tweets):
    message = {"text": tweet}
    producer.send('twitter_sentiment', value=message)
    if idx % 1000 == 0:
        print(f"✅ Sent {idx} tweets so far...")
    # Optional: time.sleep(0.01)

# Step 4: Close producer
producer.flush()
producer.close()

print("✅✅ All 16M tweets sent successfully!")
