from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

# creating spark configuration
my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master", "local[*]")

# creating sparksession
spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

# Q1.loadiing the database file using spark reader API
databaseDF = spark.read.format("csv") \
.option("header", True) \
.option("inferSchema", True) \
.option("path", "/Users/manoj/Desktop/task/database.csv") \
.load()

databaseDF.createOrReplaceTempView("earthquakes")
#sql query
distanceDF = spark.sql(""" select Date, Time, Latitude, Longitude,
                      (sqrt(pow(latitude - 0, 2) + pow(longitude - 0, 2))) AS distance_from_reference
                      FROM earthquakes""")
#showing the result
distanceDF.show()

distanceDF.write\
.mode('overwrite')\
.csv("/users/Desktop/Aidetic/data")


spark.stop()