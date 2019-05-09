# Connected Driver PySpark ML
## Create the Spark Context
fixedDate = "2019-02-10"
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

## Import the Data from MapR DB into a Spark DataFrame
print("Importing the Data from MapR DB into a Spark DataFrame")
df1 = sqlContext.read\
     .format("com.mapr.db.spark.sql.DefaultSource")\
     .option("tableName", TABLE).load()

# Feature Engineering
## Select Unique VIN / Date Combination
## Compute Correlation between Engine RPM and Engine Coolant Temperature
print("")
print("Performing Feature Engineering")
df2 = df1.groupby('vin', date_format('hrTimeStamp', 'yyyy-MM-dd').alias('date')).agg(corr("rpm","engineCoolant").alias('r'))

## Collect Target Events (Past Engine Failures that Required Service)
print("Collecting Target Events")
df3 = df1.groupby('vin', date_format('hrTimeStamp', 'yyyy-MM-dd').alias('date')).agg({"target": "max"})

## Join DataFrames into Feature Table
feature_table = df2.join(df3, ["vin", "date"]).select('vin', 'date', 'r', col("max(target)").alias("engineFailure"))
feature_table.createOrReplaceTempView("feature_table_view")

# Build Model to Predict Engine Failure 
## Import functions needed to prepare data, train model, and visualize results
print("Preparing ML Pipeline")
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier as RF
from pyspark.ml.classification import LogisticRegression 
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

## Prepare the data for training the ML Ensemble
### select historical, predictive features (e.g. correlation between Engine RPM and Engine Coolant Temperature, etc.)
### assign engineFailure as the target
### Put data in the format that many Spark ML algorithms require
myFeatures = ['r']
assembler_features = VectorAssembler(inputCols=myFeatures, outputCol='features')
labelIndexer = StringIndexer(inputCol='engineFailure', outputCol="label")
tmp = [assembler_features, labelIndexer]
pipeline = Pipeline(stages=tmp)

## Apply pipeline to the data and view result
allData = pipeline.fit(feature_table).transform(feature_table.fillna(0))
allData.cache() # Create object in memory if possible, if not write to disk

## Split data into training and testing
### Assign Training Data
#trainingDF = sqlContext.sql('SELECT * FROM feature_table_view WHERE date < CURRENT_DATE')
print("Assigning Training Data")
query = "SELECT * FROM feature_table_view WHERE date < '{}'".format(fixedDate)
trainingDF = sqlContext.sql(query)
trainingData = pipeline.fit(trainingDF).transform(trainingDF.fillna(0))
trainingData.cache() # Create object in memory if possible, if not write to disk

### Assign Test Data
#testDF = sqlContext.sql('SELECT * FROM feature_table_view WHERE date = CURRENT_DATE')
print("Assinging Test Data")
query = "SELECT * FROM feature_table_view WHERE date = '{}'".format(fixedDate)
testDF = sqlContext.sql(query)
testData = pipeline.fit(testDF).transform(testDF.fillna(0))
testData.cache() # Create object in memory if possible, if not write to disk

# Train Supervised Learning Models
## Random Forests
print("Training Random Forests Model")
rf = RF(labelCol='label', featuresCol='features',numTrees=200)
fitRF = rf.fit(trainingData)

### Logistic Regression
print("Training Logistic Regression Model")
lr = LogisticRegression(maxIter=10, regParam=0.01)
fitLR = lr.fit(trainingData)

# Apply models to the test set
## Random Forests
print("Applying Random Forests Model")
transformedRF = fitRF.transform(testData)
## Logistic Regression
print("Applying Logistic Regression Model")
transformedLR = fitLR.transform(testData)

# Write Output Message to MapR DB obd_messages Table
print("Writing Output Message to MapR DB obd_messages Table")
theOne = transformedRF.select('vin','prediction').filter("vin = '2HMDJJFB2JJ000017'").collect()
print("Predicted Breakdown: ", theOne[0])    #Row(vin=u'2HMDJJFB2JJ000017', prediction=1.0)

if ( theOne[0]['prediction'] == 1.0 ) :
 from pyspark.sql import Row

 messages = [("99","2HMDJJFB2JJ000017", "02-10-2019", "Engine Status: OVERHEATING", "1")]
 rows = sc.parallelize(messages).map(lambda p: Row(_id=p[0], vin=p[1], date=p[2], message=p[3], severity=p[4]))
 dataframe = spark.createDataFrame(rows)
 dataframe.show()

 spark.saveToMapRDB(dataframe, "/obd/obd_messages", create_table=False)
