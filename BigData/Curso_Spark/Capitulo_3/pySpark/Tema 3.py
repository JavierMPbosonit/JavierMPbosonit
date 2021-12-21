
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = (SparkSession.builder.appName("tema4").getOrCreate())
sqlContext = SQLContext(spark)

csv_file = "C:/Users/javier.martin/Desktop/Curso_Spark/Capitulo_3/DDBB"

Oschema = "`date` STRING, `delay` INT, `distance` INT,`origin` STRING, `destination` STRING"

df =spark.read.csv(csv_file, header=True, schema=Oschema)
#df.createOrReplaceTempView("us_delay_flights_tbl")

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('DDBB')
print('███████████████████████████████████████████████████████████████████')

df.show()

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('Convert the date column into a readable format:')
print('███████████████████████████████████████████████████████████████████')
# # Convert the date column into a readable format

df2 = df.select( col("date"),
                date_format(to_timestamp(col("date"), "MMddHHmm"),"MM dd HH:mm").alias("NewDate"),
                date_format(to_timestamp(col("date"), "MMddHHmm"),"MM").alias("NewDateMM"),
                date_format(to_timestamp(col("date"), "MMddHHmm"),"dd").alias("NewDatedd"),
                lit(1).alias("Count"),
                col("delay"))

df2.show()

input("Press Enter to continue...")
print('███████████████████████████████████████████████████████████████████')
print('Find the days when these delays were most common:')
print('███████████████████████████████████████████████████████████████████')

print('► PySpark')

(df2.select("NewDatedd","count","delay")
 .where(col("delay")>0)
 .groupBy("NewDatedd")
 .count().withColumnRenamed("count",'Ndelays')
 .orderBy("count", ascending=False)
 .show(31))

print('► SQL')

df2.createOrReplaceTempView('df_SQL')

sqlContext.sql("select NewDatedd, Count(NewDatedd) AS Ndelays  FROM df_SQL WHERE delay>0 GROUP BY NewDatedd ORDER BY Ndelays DESC").show(31)

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('Find the months when these delays were most common:')
print('███████████████████████████████████████████████████████████████████')

print('► Spark')

(df2.select("NewDateMM","count")
 .where(col("delay")>0)
 .groupBy("NewDateMM")
 .count().withColumnRenamed("count",'Ndelays')
 .orderBy("count", ascending=False)
 .show())

print('► SQL')

df2.createOrReplaceTempView('df_SQL')
sqlContext.sql("select NewDateMM, Count(NewDateMM) AS Ndelays  FROM df_SQL WHERE delay>0 GROUP BY NewDateMM ORDER BY Ndelays DESC").show(31)

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('First, we’ll find all flights whose distance is greater than 1,000 miles:')
print('███████████████████████████████████████████████████████████████████')


print('► Spark')

(df.select("distance", "origin", "destination")
.where(col("distance") > 1000)
.orderBy(desc("distance"))).show(10)


print('► SQL')

df.createOrReplaceTempView('df_SQL1')

sqlContext.sql("""SELECT distance, origin, destination
FROM df_SQL1 WHERE distance > 1000
ORDER BY distance DESC""").show(10)

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('Find all flights between San Francisco (SFO) and Chicago (ORD) with at least a two-hour delay:')
print('███████████████████████████████████████████████████████████████████')

print('► Spark')

(df.select("date", "delay", "origin", "destination")
.where((col("ORIGIN") =='SFO') & (col("destination") =='ORD') & (col("delay")> 120))
.orderBy(desc("delay"))).show(10)


print('► SQL')

df.createOrReplaceTempView('df_SQL1')

sqlContext.sql("""SELECT date, delay, origin, destination
FROM df_SQL1
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
ORDER by delay DESC""").show(10)

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print(' We want to label all US flights, regardless of origin and destination, with an indication of the delays they experienced: Very Long Delays (> 6 hours), Long Delays (2–6 hours), etc. We’ll add these human-readable labels in a new column called Flight_Delays:')
print('███████████████████████████████████████████████████████████████████')

print('► Spark')

df2 = df.withColumn("Flight_Delays", when(df.delay > 360 , 'Very Long Delays')
                                 .when(df.delay > 120 , 'Long Delays')
                                 .when(df.delay > 60 , 'Short Delays')
                                 .when(df.delay > 0 , 'Tolerable Delays')
                                 .when(df.delay == 0 , 'No Delays')
                                 .when(df.delay < 0 , 'Early')
                               )
df2.show()

print('► SQL')

df.createOrReplaceTempView('df_SQL1')

sqlContext.sql("""SELECT date,delay,distance, origin, destination,
CASE
WHEN delay > 360 THEN 'Very Long Delays'
WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
WHEN delay = 0 THEN 'No Delays'
ELSE 'Early'
END AS Flight_Delays
FROM df_SQL1""").show(20)

# # Writing DataFrames to CSV files

df.write.format("csv").mode("overwrite").save("file:///Users/javier.martin/Curso Spark/BD/df_csv")

#df2.write.format("csv").save("C:/Users/javier.martin/Curso Spark/BD/df_csv")

#df.write.mode('append').csv(os.path.join(tempfile.mkdtemp(), 'data'))

#df2.write.csv('mycsv.csv')
