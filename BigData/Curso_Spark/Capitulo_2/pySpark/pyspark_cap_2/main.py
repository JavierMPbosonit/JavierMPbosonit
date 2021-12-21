import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = (SparkSession
.builder
.appName("QUIJOTE_TIME")
.getOrCreate())
quijote_path = "C:/Users/javier.martin/Desktop/BigData/Curso_Spark/Capitulo_2/DDBB/el_quijote.txt"
quijote_df = spark.read.text(quijote_path)


input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('Aplicar .count()')
print('███████████████████████████████████████████████████████████████████')

print(quijote_df.count())

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('El quijote sin indicar numero de lineas y sin truncar')
print('███████████████████████████████████████████████████████████████████')

quijote_df.show(truncate=False)

#df.show(df.count(), False) para ver todas las lineas

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('El quijote sin indicar numero de lineas y truncado')
print('███████████████████████████████████████████████████████████████████')

quijote_df.show()

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('El quijote 4 lineas sin truncar')
print('███████████████████████████████████████████████████████████████████')

quijote_df.show(4,False)

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('El quijote 4 lineas truncado')
print('███████████████████████████████████████████████████████████████████')

quijote_df.show(4)

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('El quijote .show(truncate=15, n = 10)')
print('███████████████████████████████████████████████████████████████████')

print(quijote_df.show(truncate=15, n = 10), "\n")
#show permite ver el valor y pueden cambiarse tanto truncate como n
#truncate acepta tanto boolean como int (siendo int para saber el numero de carácteres que truncar)
#n indica el numero de líneas a mostrar (por defecto: 20)

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('Aplicar .head()')
print('███████████████████████████████████████████████████████████████████')

print(quijote_df.head(3)) #head() o head(n) muestra las primeras "n" líneas en forma de estructura Row

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('Aplicar .take()')
print('███████████████████████████████████████████████████████████████████')

print(quijote_df.take(5)) #take(n) igual que head(n)

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('Aplicar .first()')
print('███████████████████████████████████████████████████████████████████')

print(quijote_df.first(), "\n")

input("Press Enter to continue...")

print('███████████████████████████████████████████████████████████████████')
print('Schema del DDBB')
print('███████████████████████████████████████████████████████████████████')

print(quijote_df.schema)