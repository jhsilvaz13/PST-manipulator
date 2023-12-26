import pypff
import time
archivo_pst = './data/backup.pst'

try:
    # Abrir el archivo PST
    start=time.time()
    pst = pypff.file()
    pst.open(archivo_pst)
    
    # Acceder a la raíz del archivo PST
    root_folder = pst.get_root_folder()

    # Solicitar palabra clave por consola
    palabra_clave = input("Ingrese la palabra clave para filtrar los correos: ")
    
    # Función para listar los correos electrónicos que contienen la palabra clave y contar su cantidad
    def listar_correos_con_palabra_clave(folder):
        count = 0  # Inicializar contador de correos encontrados en esta carpeta
        for item in folder.sub_items:
            # Verificar si es un correo electrónico
            if isinstance(item, pypff.message):
                if palabra_clave.lower() in item.subject.lower():
                    print(f"Asunto: {item.subject}")
                    print(f"Fecha: {item.delivery_time}")
                    print("------")
                    count += 1  # Incrementar el contador de correos encontrados
            
            # Si hay carpetas anidadas, buscar recursivamente en ellas y sumar al contador
            if isinstance(item, pypff.folder):
                count += listar_correos_con_palabra_clave(item)
        return count
    
    # Contar los correos electrónicos que contienen la palabra clave en el archivo PST
    total_correos = listar_correos_con_palabra_clave(root_folder)
    end=time.time()
    
    print(f"Se encontraron {total_correos} correos con la palabra clave en el asunto en el archivo PST.")
    print(f"Tiempo de ejecución: {end-start} segundos.")
    
    # Cerrar el archivo PST
    pst.close()
except Exception as e:
    print(f"Ocurrió un error: {str(e)}")