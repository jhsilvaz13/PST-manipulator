#Este script convierte un archivo PST a CSV
import pypff
import csv

def listar_correos(folder, csv_writer):
    count = 0  # Inicializar contador de correos encontrados en esta carpeta
    for item in folder.sub_items:
        # Verificar si es un correo electrónico
        if isinstance(item, pypff.message):
            # Obtener los datos del correo
            asunto = item.subject
            fecha = item.delivery_time
            enviado_por = item.sender_name
            
            # Escribir los datos del correo en el archivo CSV
            csv_writer.writerow([asunto, fecha, enviado_por])
            
            count += 1  # Incrementar el contador de correos encontrados

        # Si hay carpetas anidadas, buscar recursivamente en ellas y sumar al contador
        if isinstance(item, pypff.folder):
            count += listar_correos(item, csv_writer)
    return count

# Ruta al archivo PST
archivo_pst = './data/backup.pst'

pst = pypff.file()
pst.open(archivo_pst)

# Acceder a la raíz del archivo PST
root_folder = pst.get_root_folder()

# Ruta al archivo CSV intermedio
archivo_csv = './data/backup.csv'

# Convertir PST a CSV
with open(archivo_csv, 'w', newline='', encoding='utf-8') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(['asunto', 'fecha', 'enviado_por'])
    total_correos = listar_correos(root_folder, csv_writer)

print(f'Se encontraron {total_correos} correos electrónicos y se guardaron en {archivo_csv}')
