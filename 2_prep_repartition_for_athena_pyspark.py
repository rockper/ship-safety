# Why repartition the dataset?:
# Collision Avoidance can be queried using Vessel Tracks.
# To verify a case of Collision Avoidance we look at trajectories of
# other vessels near that time & location. 
# We retrieve these local vessel tracks by querying on "base_dt", "lat" & "lon". 
# Amazon Athena and Google BigQuery cost is based on bytes read in a query.
# To reduce the bytes read, we need a dataset partitioned at the day level.
# Instead of reading 3 years of data for each query, we'll read just 1 or 2 days.
# --------------------------------------------
import os
import pyspark
import pyspark.sql.functions as sqlf

conf = pyspark.SparkConf().setMaster("local").setAppName("Athena").setSparkHome("/opt/spark-2.4.0")
spark = pyspark.sql.SparkSession.builder.config(conf=conf) \
.config("spark.driver.memory", "110g") \
.config("spark.driver.maxResultSize", "100g") \
.config("spark.local.dir", "/home/ubuntu/datadisk") \
.getOrCreate()

os.chdir('/home/ubuntu/datadisk/code/')
CODE_DIR = '/home/ubuntu/datadisk/code/'
DATA_DIR = '/home/ubuntu/datadisk/output/'

readFile = DATA_DIR + 'puget_vessels_3y_pqt'
sdf = spark.read.parquet(readFile)  # Spark DataFrame

# Create a column named "localtime"
sdf2 = sdf.withColumn("localtime", sqlf.from_utc_timestamp(sdf.base_dt, "America/Los_Angeles"))

# Compute year, month, day and day-of-week from "localtime"
sdf3 = sdf2.withColumn("year", sqlf.year(sdf2.localtime)) \
.withColumn("month", sqlf.month(sdf2.localtime)) \
.withColumn("day", sqlf.dayofmonth(sdf2.localtime)) \
.withColumn("dow", sqlf.dayofweek(sdf2.localtime))

# Repartition the Spark DataFrame and save it as parquet:
writeFile = DATA_DIR + 'by_ymd_3y_pqt'
sdf3.write.partitionBy('year', 'month', 'day').parquet(writeFile)

spark.sparkContext.stop()