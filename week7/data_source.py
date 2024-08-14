from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql.functions import split, current_timestamp

# Initialize Spark session
spark = SparkSession.builder.appName("FileSourceStreamingExample").getOrCreate()

# Define schema
file_schema = StructType([
    StructField("status_id", StringType(), True),
    StructField("status_type", StringType(), True),
    StructField("status_published", StringType(), True),
    StructField("num_reactions", StringType(), True),
    StructField("num_comments", StringType(), True),
    StructField("num_shares", StringType(), True),
    StructField("num_likes", StringType(), True),
    StructField("num_loves", StringType(), True),
    StructField("num_wows", StringType(), True),
    StructField("num_hahas", StringType(), True),
    StructField("num_sads", StringType(), True),
    StructField("num_angrys", StringType(), True)
])

# Read the streaming data
lines = spark \
    .readStream \
    .format("csv") \
    .option("maxFilesPerTrigger", 1) \
    .option("header", True) \
    .option("path", "C:/Users/ADMIN/Desktop/beeyahh/files") \
    .schema(file_schema) \
    .load()

# Transform the data
words = lines.withColumn("date", split(lines["status_published"], " ").getItem(1)) \
             .withColumn("timestamp", current_timestamp()) \
             .withWatermark("timestamp", "10 seconds")

# Group and count the data
wordCounts = words.groupBy("date", "status_type", "timestamp").count()

# Write the streaming data to a file
wordCounts.writeStream \
    .format("csv") \
    .option("path", "C:/Users/ADMIN/Desktop/beeyahh/data") \
    .trigger(processingTime='5 seconds') \
    .option("checkpointLocation","C:/Users/ADMIN/Desktop/beeyahh/data") \
    .outputMode("append") \
    .option("truncate", False) \
    .start().awaitTermination()
