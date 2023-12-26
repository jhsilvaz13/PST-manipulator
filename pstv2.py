import pypff

from elasticsearch import Elasticsearch

# Conectar a Elasticsearch (asegúrate de que Elasticsearch esté en ejecución)
es = Elasticsearch(HOST="http://localhost", PORT=9200)

# Crear un índice para almacenar correos electrónicos
index_name = 'correos'
es.indices.create(index=index_name, ignore=400)

def indexar_correos_en_elasticsearch(archivo_pst):
    pst_file = pypff.file()
    pst_file.open(archivo_pst)

    # Obtener la raíz del árbol de carpetas
    root_folder = pst_file.get_root_folder()

    # Iterar sobre los elementos de la bandeja de entrada
    for message in root_folder.get_sub_folder(0x3):
        subject = message.get_subject()
        body = message.get_plain_text_body()

        # Indexar el correo electrónico en Elasticsearch
        es.index(index=index_name, body={'subject': subject, 'body': body})

    # Cerrar el archivo PST
    pst_file.close()

# Indexar correos electrónicos desde el archivo PST
archivo_pst = 'ruta/al/archivo.pst'
indexar_correos_en_elasticsearch(archivo_pst)

def buscar_en_elasticsearch(palabra_clave):
    query = {'query': {'match': {'body': palabra_clave}}}
    resultados = es.search(index=index_name, body=query)

    # Procesar y mostrar los resultados
    correos_encontrados = resultados['hits']['hits']
    for correo in correos_encontrados:
        print(correo['_source'])

# Realizar una búsqueda en Elasticsearch
palabra_clave = 'tu_palabra_clave'
buscar_en_elasticsearch(palabra_clave)
