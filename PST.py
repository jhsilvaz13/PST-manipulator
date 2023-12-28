import pypff
import argparse
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower
import time
import warnings

def to_csv(folder, csv_writer):
    count=0
    # Ruta al archivo CSV intermedio
    for item in folder.sub_items:
        # Verificar si es un correo electrónico
        if isinstance(item, pypff.message):
            # Obtener los datos del correo
            asunto = item.subject
            fecha = item.delivery_time
            enviado_por = item.sender_name
            adjuntos =0
            """ if item.number_of_attachments > 0:
                adjuntos = item.number_of_attachments
                #ACA IMPLEMENTAR LOS ADJUNTOS
            else:
                adjuntos = 0
            """
            # Escribir los datos del correo en el archivo CSV
            csv_writer.writerow([asunto, fecha, enviado_por, adjuntos])
            
            count += 1  # Incrementar el contador de correos encontrados

        # Si hay carpetas anidadas, buscar recursivamente en ellas y sumar al contador
        if isinstance(item, pypff.folder):
            count += to_csv(item, csv_writer)
    return count

def search_with_spark(archivo_csv, palabras_clave: list):
    print("Filtrando correos... con SPARK", end='\n\n')
    # Ignorar warnings
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    # Inicializar Spark
    spark = SparkSession.builder.appName("FiltrarCorreos").getOrCreate()
    warnings.resetwarnings() # Eliminar warnings
    
    # Leer el archivo CSV como un DataFrame de Spark
    df = spark.read.csv(archivo_csv, header=True, inferSchema=True)
    df=df.withColumn("asunto", lower(col("asunto"))) # Convertir a minúsculas el asunto de cada email

    # Aplicar el filtro por la palabra clave en el asunto
    df_filtrado = df.filter(col("asunto").contains(palabras_clave[0].lower()) | 
                                    col("asunto").contains(palabras_clave[1].lower()) | 
                                    col("asunto").contains(palabras_clave[2].lower()) | 
                                    col("asunto").contains(palabras_clave[3].lower()) | 
                                    col("asunto").contains(palabras_clave[4].lower()))
    # Mostrar resultados: DESCOMENTAR para mostrar en consola
    #df_filtrado.show(df_filtrado.count(), truncate=False)
    #Convertir a csv el df filtrado
    df_filtrado.toPandas().to_csv(archivo_csv+"_filtrado.csv", index=False)
    print(f"Se encontraron {df_filtrado.count()} correos que contienen al menos una de las palabras clave.")
    spark.stop()   # Detener Spark

def maisearch_without_spark(archivo_pst, palabras_clave: list):
    try:
        # Abrir el archivo PST
        pst = pypff.file()
        pst.open(archivo_pst)

        # Acceder a la raíz del archivo PST
        root_folder = pst.get_root_folder()


        # Función para listar los correos electrónicos que contienen la palabra clave y mostrar detalles
        def listar_correos_con_palabra_clave(folder):
            count = 0  # Inicializar contador de correos encontrados en esta carpeta
            for item in folder.sub_items:
                # Verificar si es un correo electrónico
                if isinstance(item, pypff.message):
                    if item.subject in palabras_clave:
                        print(f"Asunto: {item.subject}")
                        print(f"Remitente: {item.sender_name}")  # Mostrar el remitente del correo
                        print(f"Fecha de recepción: {item.get_delivery_time()}")  # Mostrar la fecha de recepción
                        if item.number_of_attachments > 0:
                            print("Este correo tiene archivos adjuntos.")
                        else:
                            print("Este correo no tiene archivos adjuntos.")

                        count += 1  # Incrementar el contador de correos encontrados

                # Si hay carpetas anidadas, buscar recursivamente en ellas y sumar al contador
                if isinstance(item, pypff.folder):
                    count += listar_correos_con_palabra_clave(item)
            return count

        # Contar los correos electrónicos que contienen la palabra clave en el archivo PST
        total_correos = listar_correos_con_palabra_clave(root_folder)

        print(f"Se encontraron {total_correos} correos con la palabra clave en el asunto en el archivo PST.")

        # Cerrar el archivo PST
        pst.close()
    except Exception as e:
        print(f"Ocurrió un error: {str(e)}")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('PST_FILE', help="PST File Format from Microsoft Outlook")
    palabras_clave=[]
    for i in range(5):
        palabras_clave.append(input("Palabra clave "+str(i)+": "))
    
    args=parser.parse_args()
    archivo_pst = args.PST_FILE
    print('Archivo PST: '+archivo_pst)
    archivo_csv = './data/'+archivo_pst.split('/')[-1].split('.')[0]+'.csv'
    print('Archivo CSV: '+archivo_csv)
    # Abrir el archivo PST
    pst = pypff.file()
    pst.open(archivo_pst)

    # Acceder a la raíz del archivo PST
    root_folder = pst.get_root_folder()
    # Convertir PST a CSV
    with open(archivo_csv, 'w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['asunto', 'fecha', 'enviado_por', 'adjuntos'])
        total_correos = to_csv(root_folder, csv_writer)
    
    print(f'Se encontraron {total_correos} correos electrónicos y se guardaron en {archivo_csv}')
    search_with_spark(archivo_csv, palabras_clave)
    print('Los correos que contienen la palabra clave se guardaron en '+archivo_csv+'_filtrado.csv')