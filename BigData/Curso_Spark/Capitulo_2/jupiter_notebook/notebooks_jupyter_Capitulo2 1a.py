#!/usr/bin/env python
# coding: utf-8

# In[25]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# In[74]:


# Build a SparkSession using the SparkSession APIs.
# If one does not exist, then create an instance. There
# can only be one SparkSession per JVM.
spark = (SparkSession
.builder
.appName("QUIJOTE_TIME")
.getOrCreate())
quijote_path = "gs://bosonitspark/notebooks/jupyter/el_quijote.txt"
quijote_df = spark.read.text(quijote_path)


# In[75]:


print(quijote_df.count())


# In[61]:


print(quijote_df.head(3)) #head() o head(n) muestra las primeras "n" líneas en forma de estructura Row


# In[76]:


print(quijote_df.take(5)) #take(n) igual que head(n)


# In[77]:


print(quijote_df.show(truncate=15, n = 10), "\n") #show permite ver el valor y pueden cambiarse tanto truncate como n
#truncate acepta tanto boolean como int (siendo int para saber el numero de carácteres que truncar)
#n indica el numero de líneas a mostrar (por defecto: 20)


# In[67]:


print(quijote_df.first(), "\n")


# In[79]:


quijote_df.schema


# In[ ]:




