import findspark
findspark.init()
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Create a DataFrame using SparkSession
spark = (SparkSession.builder.appName("AuthorsAges").getOrCreate())

# Create a DataFrame
data_df = spark.createDataFrame([("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)], ["name", "age"])

# Group the same names together, aggregate their ages, and compute an average
avg_df = data_df.groupBy("name").agg(avg("age"))

# Show the results of the final execution
avg_df.show()




spark = (SparkSession.builder.appName("tema4").getOrCreate())
sqlContext = SQLContext(spark)


csv_file = "C:/Users/javier.martin/Desktop/BigData/Curso_Spark/Capitulo_4/DDBB/departuredelays.csv"

Oschema = "`date` STRING, `delay` INT, `distance` INT,`origin` STRING, `destination` STRING"

df =spark.read.csv(csv_file, header=True, schema=Oschema)
#df.createOrReplaceTempView("us_delay_flights_tbl")

