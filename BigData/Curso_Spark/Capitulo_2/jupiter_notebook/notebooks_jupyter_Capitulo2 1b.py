#!/usr/bin/env python
# coding: utf-8

# In[111]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# In[112]:


# Cargarmos los datos y construimos el Data Frame
spark = (SparkSession
.builder
.appName("MM_TIME")
.getOrCreate())
MM = "gs://bosonitspark/notebooks/jupyter/mnm_dataset.csv"
MM_df = (spark.read.format("csv")
.option("header", "true")
.option("inferSchema", "true")
.load(MM))


# In[9]:


# Para ver el Data Frame
MM_df.schema


# In[20]:


# Prueba para imprimir en consoloa elegante con pyspark
for i in MM_df.head(3):
    print(i[0],i[1],i[2])


# In[40]:


#1.b.i. Otras operaciones de agregación como el Max con otro tipo de ordenamiento (descendiente).(1FORMA)
MM_df.sort(desc("Count")).show()


# In[45]:


#1.b.i. Otras operaciones de agregación como el Max con otro tipo de ordenamiento (descendiente).(2FORMA)
MM_df.orderBy(col("Count").desc()).show()


# In[58]:


#1.b.ii. hacer un ejercicio como el “where” de CA que aparece en el libro pero indicando más opciones de estados (p.e. NV, TX, CA, CO).
li=[ "NV","TX","CA","CO"]
MM_df.where(MM_df.State.isin(li)).show()


# In[126]:


#Imprimir el valor maximo de una lolumna
MM_df.agg({'Count':'max'}).show()


# In[92]:


#1.b.iii. Hacer un ejercicio donde se calculen en una misma operación el Max, Min, Avg, Count.
import pyspark.sql.functions as F # sele nombra apara que ala hora de llamar a funciones sea mas simple decir de F la funcion tal...
MM_df.select(F.sum("Count"), F.avg("Count"),F.min("Count"), F.max("Count")).show()


# In[120]:


# TempView que es lo que necesitamos para procesar con codigo SQL 
MM_df.createOrReplaceTempView("MMdf")
# Para señalar que estamos escribiendo codigo SQL se trabaja con TempView y se escribe:
spark.sql("select * from MMdf").show()


# In[105]:


# Esto no es codigo SQL se trabaja con TempView que es lo que necesitamos para procesar SQL pero es SQL. Guardamos el DF en una variable.
G=MM_df.select(F.count(MM_df.Count).alias("count"), F.avg(MM_df.Count).alias("promedio"), F.min(MM_df.Count).alias("minimo"), F.max(MM_df.Count).alias("maximo"))


# In[107]:


G.show()


# In[108]:


G.schema


# In[122]:


#1.b.i.(SQL) Hacer también ejercicios en SQL creando tmpView. Otras operaciones de agregación como el Max con otro tipo de ordenamiento (descendiente).
spark.sql("select * from MMdf order by Count Desc").show()


# In[121]:


#1.b.ii.(SQL) Hacer también ejercicios en SQL creando tmpView. Hacer un ejercicio como el “where” de CA que aparece en el libro pero indicando más opciones de estados (p.e. NV, TX, CA, CO).
spark.sql("select * from MMdf WHERE State IN ('NV','TX','CA','CO')").show()


# In[123]:


#1.b.iii.(SQL) Hacer también ejercicios en SQL creando tmpView. Hacer un ejercicio donde se calculen en una misma operación el Max, Min, Avg, Count.
spark.sql("select MAX(Count) as Max,MIN(Count) as min,AVG(Count) as Avg,COUNT(Count) as Count,SUM(Count) as Sum from MMdf order by Count Desc").show()


# In[ ]:




