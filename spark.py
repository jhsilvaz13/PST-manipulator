from pyspark.sql import SparkSession

def buscar_palabra_clave_en_correos(archivo_pst, palabra_clave):
    spark = SparkSession.builder.appName("BuscarCorreos").getOrCreate()

    # Cargar el archivo PST como un DataFrame
    df = spark.read.format("com.databricks.spark.pyspark").load(archivo_pst)

    # Filtrar correos que contengan la palabra clave en el asunto o el cuerpo
    df_filtrado = df.filter(df['subject'].contains(palabra_clave) | df['body'].contains(palabra_clave))

    # Mostrar resultados
    df_filtrado.show()

    # Cerrar la sesión de Spark
    spark.stop()

# Ruta al archivo PST
archivo_pst = './data/backup.pst'

# Palabra clave a buscar
palabra_clave = 'convocatoria'

# Buscar correos con la palabra clave utilizando Apache Spark
buscar_palabra_clave_en_correos(archivo_pst, palabra_clave)
