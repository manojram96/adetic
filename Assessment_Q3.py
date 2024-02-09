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

#filtering the data based on magnitude greater than 5.0
magnitudeDF = spark.sql("""select Date, Time,Latitude, Longitude, Type,Depth,Magnitude 
                                 from data 
                                 where Magnitude > 5.0""")
#printing the result having the magnitude greater than 5.0
magnitudeDF.show()

# saving to repository as CSV file using spart writer API
magnitudeDF.coalesce(1).write.option("header", "true").csv("magnitude.csv")

spark.stop()