# The following code should read CSV into a spark-data-frame

import pyspark
from pyspark import SQLContext

sc = pyspark.SparkContext()
sql = SQLContext(sc)

df = (sql.read
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .load("/path/to_csv.csv"))

# these lines are equivalent in Spark 2.0 - using [SparkSession][1]
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

spark.read.format("csv").option("header", "true").load("/path/to_csv.csv")
spark.read.option("header", "true").csv("/path/to_csv.csv")

# https://spark.apache.org/docs/1.6.2/api/python/pyspark.sql.html
# call function using 'python test.py mbti.csv' in the command line from the project folder
