# importing sparksession packages
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

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

# creating spark sql using below command and giving custom name to the tabel as "data"
databaseDF.createOrReplaceTempView("data")

averageDepthMagDF = spark.sql(""" select Type,avg(Depth),avg(Magnitude)
                                  from data
                                  group by Type """)
#showing the result of average depth and magnitude
averageDepthMagDF.show()

averageDepthMagDF.write\
.mode('overwrite')\
.csv("/users/Desktop/Aidetic/data")

spark.stop()