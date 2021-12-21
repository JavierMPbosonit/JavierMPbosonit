# San Francisco Fire Calls

## Cargamos los paquetes y libresrias
import findspark
findspark.init()
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


# In[10]:


## Creamos la Sparksesion y Definimos la ruta

spark = (SparkSession.builder.appName("Example 3.7").getOrCreate())

sf_fire_file = "C:/Users/javier.martin/Desktop/BigData/Curso_Spark/Capitulo_3/DDBB/Fire_Incidents.csv"


# In[11]:


# Definimos el schems
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                     StructField('UnitID', StringType(), True),
                     StructField('IncidentNumber', IntegerType(), True),
                     StructField('CallType', StringType(), True),
                     StructField('CallDate', StringType(), True),
                     StructField('WatchDate', StringType(), True),
                     StructField('CallFinalDisposition', StringType(), True),
                     StructField('AvailableDtTm', StringType(), True),
                     StructField('Address', StringType(), True),
                     StructField('City', StringType(), True),
                     StructField('Zipcode', IntegerType(), True),
                     StructField('Battalion', StringType(), True),
                     StructField('StationArea', StringType(), True),
                     StructField('Box', StringType(), True),
                     StructField('OriginalPriority', StringType(), True),
                     StructField('Priority', StringType(), True),
                     StructField('FinalPriority', IntegerType(), True),
                     StructField('ALSUnit', BooleanType(), True),
                     StructField('CallTypeGroup', StringType(), True),
                     StructField('NumAlarms', IntegerType(), True),
                     StructField('UnitType', StringType(), True),
                     StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                     StructField('FirePreventionDistrict', StringType(), True),
                     StructField('SupervisorDistrict', StringType(), True),
                     StructField('Neighborhood', StringType(), True),
                     StructField('Location', StringType(), True),
                     StructField('RowID', StringType(), True),
                     StructField('Delay', FloatType(), True)])


# In[12]:


fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)


# In[45]:


## para guardarlo en memoria y que no se forme cadavez que se le llama e y las acciones vayayan mas rapido te imprime el schema del data frame
fire_df.cache()


# In[46]:


## Contar las lineas
fire_df.count()


# In[47]:


## para ver el schema del data frame de foma esquematica
fire_df.printSchema()


# In[48]:


## para ver el schema del data frame en una linea y pone nombre : tipo
## (nullable = true) es que permite que datos sean null
print(fire_df.limit(5))


# In[49]:


##  llamamos al df y luego ponemos .where(col("_______") ==/!=/</>/>=/<= "categoria" o valor))
## truncate hace que salga la fila entera o no si es larga:  fl-> |27th Av. / Cabrillo St.|   tr-> |27th Av. / Cabril...|
few_fire_df = (fire_df.select("IncidentNumber", "AvailableDtTm", "CallType")
              .where(col("CallType") != "Medical Incident"))

few_fire_df.show(5, truncate=False)


# In[50]:


## How many distinct CallTypes were recorded as the causes of the fire calls?
## .distinct()
##
fire_df.select("CallType").where(col("CallType").isNotNull()).distinct().count()


# In[51]:


## We can list the distinct call types in the data set using these queries:
fire_df.select("CallType").where(col("CallType").isNotNull()).distinct().show(10, False)


# In[52]:


## Find out all response or delayed times greater than 5 mins?
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
new_fire_df.select( "ResponseDelayedinMins").where(col( "ResponseDelayedinMins") > 1).show(5, False)


# In[53]:


new_fire_df.select(to_date(col("AvailableDtTm"), "MM/dd/yyyy").alias("IncidentDate")).show()


# In[36]:


df_fechas_tipo = new_fire_df.select(to_date(col("CallDate"), "yyyy-MM-ddThh:mm:ss").alias("date"), col("CallType"))
df_fechas_tipo.createOrReplaceTempView("tabla_fire_calls")


# In[69]:


## Covertimos las fechas en formato string a fecha
fire_ts_df = new_fire_df.select(to_date(col("CallDate"), "MM/dd/yyyy").alias("IncidentDate"),col("CallDate"),
                                to_date(col("WatchDate"), "MM/dd/yyyy").alias("OnWatchDate"),col("WatchDate"),
                                to_date(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a").alias("AvailableDtTS"),col("AvailableDtTm"))

fire_ts_df.select("IncidentDate","OnWatchDate","AvailableDtTS","CallDate","WatchDate","AvailableDtTm").show(3)


# In[56]:


#Guardamos en cache
fire_ts_df.cache()

fire_ts_df.columns


# In[76]:


#Check the transformed columns with Spark Timestamp type
fire_ts_df.select("IncidentDate", "OnWatchDate", "AvailableDtTS").where(col("IncidentDate").isNotNull()).show(30, False)


# In[85]:


## What were the most common call types?
(new_fire_df
 .select("CallType").where(col("CallType").isNotNull())
 .groupBy("CallType")
 .count().withColumnRenamed("count",'numero de llamadas')
 .orderBy("numero de llamadas", ascending=False)
 .show(n=10, truncate=False))


# In[17]:


## What zip codes accounted for most common calls?
(fire_ts_df
 .select("CallType", "ZipCode")
 .where(col("CallType").isNotNull())
 .groupBy("CallType", "Zipcode")
 .count()
 .orderBy("count", ascending=False)
 .show(10, truncate=False))


# In[87]:


## What San Francisco neighborhoods are in the zip codes 94102 and 94103
new_fire_df.select("Neighborhood", "Zipcode").where((col("Zipcode") == 94102) | (col("Zipcode") == 94103)).distinct().show(10, truncate=False)


# In[88]:


new_fire_df.select(sum("NumAlarms"), avg("ResponseDelayedinMins"), min("ResponseDelayedinMins"), max("ResponseDelayedinMins")).show()


# In[20]:


fire_ts_df.select(year('IncidentDate')).distinct().orderBy(year('IncidentDate')).show()


# In[90]:


fire_ts_df.filter(year('IncidentDate') == 2018).groupBy(weekofyear('IncidentDate')).count().orderBy('count', ascending=False).show()


# In[94]:


fire_ts_df.filter(year("IncidentDate") == 2001).show(10, False)


# In[110]:


fire_ts_df.write.format("csv").mode("overwrite").save("C:/Users/javier.martin/YYY").coalesce(1)


# In[ ]:


#get_ipython().run_line_magic('fs', 'ls /tmp/fireServiceParquet/')


# In[ ]:


fire_ts_df.write.format("parquet").mode("overwrite").saveAsTable("FireServiceCalls")


# In[ ]:


#get_ipython().run_line_magic('sql', '')
#CACHE TABLE FireServiceCalls


# In[ ]:


#get_ipython().run_line_magic('sql', '')
#SELECT * FROM FireServiceCalls LIMIT 10




file_parquet_df = spark.read.format("parquet").load("/tmp/fireServiceParquet/")


display(file_parquet_df.limit(10))


