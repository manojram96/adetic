from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

#calling function to find the level
def categorize_earthquake(Magnitude):
    if Magnitude < 5.5:
        return 'Low'
    elif Magnitude < 6.4:
        return 'Moderate'
    else:
        return 'High'

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

spark.udf.register("categorize_earthquake_udf", categorize_earthquake, StringType())

# Applying the UDF using Spark SQL
databaseDF.createOrReplaceTempView("earthquakes")

#writing spark sql query
result_df = spark.sql("""select Date, Time,Latitude, Longitude, Type,Depth,Magnitude,categorize_earthquake_udf(Magnitude) AS Category 
                         FROM earthquakes""")

# Showing the result
result_df.show()

result_df.write\
.mode('overwrite')\
.csv("/users/Desktop/Aidetic/data")


spark.stop()