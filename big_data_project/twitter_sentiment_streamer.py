from kafka import KafkaProducer
import pandas as pd
import json
import random
import time

# Step 1: Load Tweets
df = pd.read_csv('training.1600000.processed.noemoticon.csv', encoding='latin-1', header=None)
tweets = df[5].tolist()

# Step 2: Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Step 3: Continuously send random tweets
try:
    while True:
        tweet = random.choice(tweets)  # Randomly pick a tweet
        message = {"text": tweet}
        producer.send('twitter_sentiment', value=message)
        producer.flush()  # ensure the message is immediately sent
        print(f"✅ Sent tweet: {tweet[:60]}...")  # show first 60 chars
        time.sleep(1)  # send 1 tweet per second
except KeyboardInterrupt:
    print("\n❌ Stopped by user.")

# Step 4: Cleanly close when exiting
producer.close()
