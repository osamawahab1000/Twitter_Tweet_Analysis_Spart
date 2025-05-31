#!/bin/bash

echo "📦 Step 1: Starting Zookeeper..."
x-terminal-emulator -e "bash -c 'cd /home/osama/Documents/kafka_2.13-3.9.0 && bin/zookeeper-server-start.sh config/zookeeper.properties; exec bash'" &

sleep 5

echo "🚀 Step 2: Starting Kafka Broker..."
x-terminal-emulator -e "bash -c 'cd /home/osama/Documents/kafka_2.13-3.9.0 && bin/kafka-server-start.sh config/server.properties; exec bash'" &

sleep 5

echo "🧠 Step 3: Starting Spark Streaming Sentiment Job..."
x-terminal-emulator -e "bash -c '/home/osama/Documents/big_data_project/venv/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /home/osama/Documents/big_data_project/spark_streaming_sentiment.py; exec bash'" &

sleep 3

echo "💬 Step 4: Starting Tweet Stream Producer..."
x-terminal-emulator -e "bash -c 'source /home/osama/Documents/big_data_project/venv/bin/activate && python /home/osama/Documents/big_data_project/twitter_sentiment_streamer.py; exec bash'" &

echo "✅ All components started in separate terminals!"

