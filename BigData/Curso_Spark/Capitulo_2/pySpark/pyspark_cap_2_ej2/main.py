import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Cargarmos los datos y construimos el Data Frame
spark = (SparkSession
.builder
.appName("MM_TIME")
.getOrCreate())
MM = "C:/Users/javier.martin/Desktop/BigData/Curso_Spark/Capitulo_2/DDBB/mnm_dataset.csv"
MM_df = (spark.read.format("csv")
.option("header", "true")
.option("inferSchema", "true")
.load(MM))


# In[9]:


input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('Para ver el schema del DDBB')
print('███████████████████████████████████████████████████████████████████')
print(MM_df.schema)


input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('Prueba para imprimir en consoloa en modo python con pyspark')
print('███████████████████████████████████████████████████████████████████')

for i in MM_df.head(3):
    print(i[0],i[1],i[2])

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('1.b.i. Otras operaciones de agregación como el Max con otro tipo de ordenamiento (descendiente).(1FORMA)')
print('███████████████████████████████████████████████████████████████████')

MM_df.sort(desc("Count")).show()

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('1.b.i. Otras operaciones de agregación como el Max con otro tipo de ordenamiento (descendiente).(2FORMA)')
print('███████████████████████████████████████████████████████████████████')

MM_df.orderBy(col("Count").desc()).show()

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('1.b.ii. hacer un ejercicio como el “where” de CA que aparece en el libro pero indicando más opciones de estados (p.e. NV, TX, CA, CO).')
print('███████████████████████████████████████████████████████████████████')

li=[ "NV","TX","CA","CO"]
MM_df.where(MM_df.State.isin(li)).show()

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('Imprimir el valor maximo de una lolumna')
print('███████████████████████████████████████████████████████████████████')

#Imprimir el valor maximo de una lolumna
MM_df.agg({'Count':'max'}).show()


input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('1.b.iii. Hacer un ejercicio donde se calculen en una misma operación el Max, Min, Avg, Count.')
print('███████████████████████████████████████████████████████████████████')

print('► PySpark')

import pyspark.sql.functions as F # sele nombra apara que ala hora de llamar a funciones sea mas simple decir de F la funcion tal...
G1=MM_df.select(F.sum("Count"), F.avg("Count"),F.min("Count"), F.max("Count"))
G1.show()
print('► SQL')

# TempView que es lo que necesitamos para procesar con codigo SQL
MM_df.createOrReplaceTempView("MMdf")
# Para señalar que estamos escribiendo codigo SQL se trabaja con TempView y se escribe:
#spark.sql("select * from MMdf").show()
# Esto no es codigo SQL se trabaja con TempView que es lo que necesitamos para procesar SQL pero es SQL. Guardamos el DF en una variable.
G=MM_df.select(F.count(MM_df.Count).alias("count"), F.avg(MM_df.Count).alias("promedio"), F.min(MM_df.Count).alias("minimo"), F.max(MM_df.Count).alias("maximo"))
G.show()

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('Schemas de los dos DF')
print('███████████████████████████████████████████████████████████████████')

print('► PySpark')
print(G1.schema)
print('► SQL')
print(G.schema)


input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('1.b.i.(SQL) Hacer también ejercicios en SQL creando tmpView. Otras operaciones de agregación como el Max con otro tipo de ordenamiento (descendiente).')
print('███████████████████████████████████████████████████████████████████')

spark.sql("select * from MMdf order by Count Desc").show()

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('1.b.ii.(SQL) Hacer también ejercicios en SQL creando tmpView. Hacer un ejercicio como el “where” de CA que aparece en el libro pero indicando más opciones de estados (p.e. NV, TX, CA, CO).')
print('███████████████████████████████████████████████████████████████████')

spark.sql("select * from MMdf WHERE State IN ('NV','TX','CA','CO')").show()

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('1.b.iii.(SQL) Hacer también ejercicios en SQL creando tmpView. Hacer un ejercicio donde se calculen en una misma operación el Max, Min, Avg, Count.')
print('███████████████████████████████████████████████████████████████████')

spark.sql("select MAX(Count) as Max,MIN(Count) as min,AVG(Count) as Avg,COUNT(Count) as Count,SUM(Count) as Sum from MMdf order by Count Desc").show()





