from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Initialize SparkSession
spark = SparkSession.builder.appName("SocketStreamExample").getOrCreate()

# Read from socket stream
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 8000) \
    .load()

# Split the lines into words
words = lines.select(explode(split(lines.value, " ")).alias("word"))

# Group and count each word
wordCounts = words.groupBy("word").count()

# Start running the query that prints the word counts to the console
query = wordCounts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Await termination of the query
query.awaitTermination()

