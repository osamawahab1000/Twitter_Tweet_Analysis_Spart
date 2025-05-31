from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StringType, StructType, StructField
import torch
from transformers import BertTokenizer, BertForSequenceClassification

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("TwitterSentimentStreaming") \
    .master("local[*]") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Step 2: Load Trained BERT Model and Tokenizer
model = BertForSequenceClassification.from_pretrained("./bert_sentiment_model")
tokenizer = BertTokenizer.from_pretrained("./bert_sentiment_model")

# Ensure model is in evaluation mode
model.eval()

# Step 3: Define Schema of Incoming Kafka Messages
schema = StructType([
    StructField("text", StringType(), True)
])

# Step 4: Define UDF (User Defined Function) for Sentiment Prediction
def predict_sentiment(tweet_text):
    inputs = tokenizer(tweet_text, return_tensors="pt", truncation=True, padding=True, max_length=64)
    with torch.no_grad():
        outputs = model(**inputs)
        logits = outputs.logits
        predicted_class = torch.argmax(logits, dim=1).item()
    if predicted_class == 1:
        return "😊 Positive"
    else:
        return "😞 Negative"

# Register UDF
predict_sentiment_udf = udf(predict_sentiment, StringType())

# Step 5: Read Stream from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "twitter_sentiment") \
    .option("startingOffsets", "latest") \
    .load()

# Step 6: Decode Kafka value and parse JSON
tweets_df = kafka_stream.selectExpr("CAST(value AS STRING) as json_value")

tweets_parsed = tweets_df.select(from_json(col("json_value"), schema).alias("data")).select("data.text")

# Step 7: Apply Sentiment Prediction UDF
predicted_df = tweets_parsed.withColumn("sentiment", predict_sentiment_udf(col("text")))

# Step 8: Write Output to Console
query = predicted_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 10) \
    .start()

query.awaitTermination()
