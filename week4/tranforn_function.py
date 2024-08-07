from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


rdd = spark.sparkContext.textFile('fb_live_thailand.csv', 5)


flatmap = rdd.flatMap(lambda x:x.split(','))
pair = flatmap.map(lambda x: (x,1))
take_pair = pair.take(200)
for f in take_pair :
    if str(f[0]) == 'photo' or str(f[0]) == 'video':
        print(str(f[0]), str(f[1]))

sort_data = pair.sortByKey().collect()
for f in sort_data:
    print(str(f[0]), str(f[1]))

