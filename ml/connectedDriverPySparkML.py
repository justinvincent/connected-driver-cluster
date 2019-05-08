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

df2 = df1.groupby('vin', date_format('hrTimeStamp', 'yyyy-MM-dd').alias('date')).agg(corr("rpm","engineCoolant").alias('r'))
df3 = df1.groupby('vin', date_format('hrTimeStamp', 'yyyy-MM-dd').alias('date')).agg({"target": "max"})

feature_table = df2.join(df3, ["vin", "date"]).select('vin', 'date', 'r', col("max(target)").alias("engineFailure"))
feature_table.createOrReplaceTempView("feature_table_view")

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier as RF
from pyspark.ml.classification import LogisticRegression 
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


myFeatures = ['r']
# put data in the format that many Spark ML algorithms require
assembler_features = VectorAssembler(inputCols=myFeatures, outputCol='features')
labelIndexer = StringIndexer(inputCol='engineFailure', outputCol="label")
tmp = [assembler_features, labelIndexer]
pipeline = Pipeline(stages=tmp)

allData = pipeline.fit(feature_table).transform(feature_table.fillna(0))
allData.cache() # Create object in memory if possible, if not write to disk

trainingDF = sqlContext.sql('SELECT * FROM feature_table_view WHERE date < CURRENT_DATE')
trainingData = pipeline.fit(trainingDF).transform(trainingDF.fillna(0))
trainingData.cache() # Create object in memory if possible, if not write to disk

testingDF = sqlContext.sql('SELECT * FROM feature_table_view WHERE date = CURRENT_DATE')

testDF = sqlContext.sql('SELECT * FROM feature_table_view WHERE date = CURRENT_DATE')
testData = pipeline.fit(testDF).transform(testDF.fillna(0))
testData.cache() # Create object in memory if possible, if not write to disk

rf = RF(labelCol='label', featuresCol='features',numTrees=200)
fitRF = rf.fit(trainingData)

lr = LogisticRegression(maxIter=10, regParam=0.01)
fitLR = lr.fit(trainingData)

transformedRF = fitRF.transform(testData)
transformedLR = fitLR.transform(testData)

from pyspark.mllib.evaluation import BinaryClassificationMetrics as metric

theOne = transformedRF.select('vin','prediction').filter("vin = '2HMDJJFB2JJ000017'").collect()

print "theOne: ", theOne[0]    #Row(vin=u'2HMDJJFB2JJ000017', prediction=1.0)

if ( theOne[0]['prediction'] == 1.0 ) :
 print "YES"

 print "Something Else"
 # Save to Messages Table
 from pyspark.sql import Row

 messages = [("99","2HMDJJFB2JJ000017", "02-13-2019", "Engine Status: OVERHEATING", "1")]
 rows = sc.parallelize(messages).map(lambda p: Row(_id=p[0], vin=p[1], date=p[2], message=p[3], severity=p[4]))
 dataframe = spark.createDataFrame(rows)
 dataframe.show()

 spark.saveToMapRDB(dataframe, "/obd/obd_messages", create_table=False)
