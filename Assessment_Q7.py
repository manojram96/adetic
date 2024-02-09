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

# Registering the DataFrame as a temporary view
databaseDF.createOrReplaceTempView("earthquakes")

# Executing Spark SQL to filter and aggregate the data
filtered_df = spark.sql("""
    SELECT Latitude, Longitude, Magnitude
    FROM earthquakes
    WHERE Magnitude >= 5.0
""")

# Converting PySpark DataFrame to Pandas DataFrame
pandas_df = filtered_df.toPandas()

# Closing SparkSession (if no longer needed)
spark.stop()

# Now, using the Pandas DataFrame with Folium
import folium

# Creating a map centered around the mean latitude and longitude of the earthquakes
map = folium.Map(location=[pandas_df['Latitude'].mean(), pandas_df['Longitude'].mean()], zoom_start=2)

# Adding markers for each earthquake
for index, row in pandas_df.iterrows():
    folium.Marker(location=[row['Latitude'], row['Longitude']],
                  popup=f"Magnitude: {row['Magnitude']}",
                  icon=folium.Icon(color='red')).add_to(map)

# Displaying the map
map.save('earthquake_map.html')

