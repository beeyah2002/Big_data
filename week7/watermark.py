from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql.functions import split, current_timestamp, window
from pyspark.sql.functions import explode, split

# Initialize Spark session
spark = SparkSession.builder.appName("FileSourceStreamingExample").getOrCreate()

# Read the streaming data from the socket
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 8000) \
    .load()

# Split the lines into words and add a timestamp column
words = lines.select(explode(split(lines.value, " ")).alias("word")) \
             .withColumn("timestamp", current_timestamp())

# Perform a windowed count
windowCounts = words.groupBy(window(words.timestamp, "10 seconds",\

"5 seconds"), words.word).count()



# Output the results to the console
query = windowCounts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Await termination of the query
query.awaitTermination()
