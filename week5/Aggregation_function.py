# from pyspark.sql.types import *
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.getOrCreate()
# from pyspark.sql.functions import countDistinct

# read_file = spark.read.format("csv") \
#     .option("header", "true") \
#     .load("data/*")
# read_file.creatOrReplacTempView("data_a")
# sqlDf = spark.sql("SELECT * FROM data_a")
# sqlDf.show(10)

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, countDistinct, first, last, min, max, sum, sumDistinct, avg
from pyspark.sql.types import IntegerType

# สร้าง SparkSession
spark = SparkSession.builder.appName("AggregationFunctions").getOrCreate()

# โหลดข้อมูลจากไฟล์ CSV
df1 = spark.read.csv('/content/drive/MyDrive/Colab Notebooks/data/fb_live_thailand2.csv', header=True, inferSchema=True)
df2 = spark.read.csv('/content/drive/MyDrive/Colab Notebooks/data/fb_live_thailand3.csv', header=True, inferSchema=True)

# รวม DataFrame
sqlDF = df1.union(df2)

# การนับจำนวนแถว (count)
sqlDF.select(count("status_id")).show()

# การนับจำนวนกลุ่มที่ไม่ซ้ำ (countDistinct)
sqlDF.select(countDistinct("status_type")).show()

# การดึงค่าที่เป็นแรกและสุดท้าย (first and last)
sqlDF.select(first("num_reactions"), last("num_reactions")).show()

# การหาค่าต่ำสุดและค่าสูงสุด (min and max)
sqlDF = sqlDF.withColumn('num_reactions_int', sqlDF['num_reactions'].cast(IntegerType()))
sqlDF.select(min("num_reactions_int"), max("num_reactions_int")).show()

# การหาผลรวมและผลรวมเฉพาะค่า (sum and sumDistinct)
sqlDF.select(sum("num_reactions_int")).show()
sqlDF.select(sumDistinct("num_reactions_int")).show()

# การหาค่าเฉลี่ย (avg)
sqlDF.select(avg("num_reactions_int")).show()