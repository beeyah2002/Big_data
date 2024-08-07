
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the CSV file with header information
read_file = spark.read.format("csv") \
    .option("header", "true") \
    .load("fb_live_thailand.csv")

# Print the schema of the DataFrame
read_file.printSchema()