from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


rdd = spark.sparkContext.textFile('fb_live_thailand.csv', 5)
print('Number of partitions: ' + str(rdd.getNumPartitions()))

count_distinct = rdd.distinct().count()
print("Numbger of distinct ercord: ", count_distinct)


filter_rdd = rdd.filter(lambda x: x.split(',')[1] == 'link').collect()

print(filter_rdd)