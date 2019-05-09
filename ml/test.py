TABLE = "/obd/obd_raw_table"
APP_NAME = "Connected Car ML"

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName(APP_NAME) \
    .getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)

# Read from Table:
df1 = sqlContext.read\
     .format("com.mapr.db.spark.sql.DefaultSource")\
     .option("tableName", TABLE).load()

print("Something")
