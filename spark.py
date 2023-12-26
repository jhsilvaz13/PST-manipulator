#Este script usa pyspark para realizar el filtrado
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

# Inicializar Spark
spark = SparkSession.builder.appName("FiltrarCorreos").getOrCreate()

# Ruta al archivo CSV
archivo_csv = './data/backup.csv'

start=time.time()
# Leer el archivo CSV como un DataFrame de Spark
df = spark.read.csv(archivo_csv, header=True, inferSchema=True)

# Palabra clave a buscar
palabra_clave = 'UnAL'

# Aplicar el filtro por la palabra clave en el asunto o el cuerpo
df_filtrado = df.filter(col('Asunto').contains(palabra_clave.lower()))

# Mostrar resultados
df_filtrado.show(df.count(), truncate=False)
end=time.time()
print(f"Se encontraron {df_filtrado.count()} correos con la palabra clave en el asunto o el cuerpo en el archivo CSV.")
print(f"Tiempo de ejecución: {end-start} segundos.")
spark.stop()