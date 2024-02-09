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

#Q1: Loadign dataset and showing tables containing below header
InitialDF = spark.sql("select Date,Time,Latitude,Longitude,Type,Depth,Magnitude from data")
InitialDF.show()

# writing spark sql query
dateTimeCastDF = spark.sql("select to_timestamp(concat(Date,' ',Time)) as Timestamp from data")

dateTimeCastDF.show()
#printing the schema to the timestamp conversion
dateTimeCastDF.printSchema()

# saving to repository as CSV file using spart writer API
databaseDF.write\
.mode('overwrite')\
.csv("/users/Desktop/Aidetic/data")

#stopping the job
spark.stop()


