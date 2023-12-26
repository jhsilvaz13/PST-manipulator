#Este script usa pyspark para realizar el filtrado
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower

import time
import warnings

pyspark = warnings.filterwarnings("ignore", category=DeprecationWarning)
# Inicializar Spark
spark = SparkSession.builder.appName("FiltrarCorreos").getOrCreate()

warnings.resetwarnings() # Eliminar warnings
# Ruta al archivo CSV
archivo_csv = './data/backup'

start=time.time()
palabras_clave = []
print("Ingrese las palabras clave para filtrar los correos: ")
for i in range(5):
    palabras_clave.append(input(f"Palabra clave {i+1}: "))
# Leer el archivo CSV como un DataFrame de Spark
df = spark.read.csv(archivo_csv+".csv", header=True, inferSchema=True)
df=df.withColumn("asunto", lower(col("asunto"))) # Convertir a minúsculas el asunto de cada email

# Aplicar el filtro por la palabra clave en el asunto
df_filtrado = df.filter(col("asunto").contains(palabras_clave[0].lower()) | 
                                 col("asunto").contains(palabras_clave[1].lower()) | 
                                 col("asunto").contains(palabras_clave[2].lower()) | 
                                 col("asunto").contains(palabras_clave[3].lower()) | 
                                 col("asunto").contains(palabras_clave[4].lower()))
# Mostrar resultados
df_filtrado.show(df_filtrado.count(), truncate=False)
#Convertir a csv el df filtrado
df_filtrado.toPandas().to_csv(archivo_csv+"_filtrado.csv", index=False)
end=time.time()
print(f"Se encontraron {df_filtrado.count()} correos que contienen al menos una de las palabras clave.")
print(f"Tiempo de ejecución: {end-start} segundos.")
spark.stop()   # Detener Spark