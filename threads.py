import pypff
import threading
import time

archivo_pst = './data/backup.pst'

try:
    start=time.time()
    # Abrir el archivo PST
    pst = pypff.file()
    pst.open(archivo_pst)
    
    # Acceder a la raíz del archivo PST
    root_folder = pst.get_root_folder()

    # Solicitar palabra clave por consola
    palabra_clave = input("Ingrese la palabra clave para filtrar los correos: ")

    # Mutex para garantizar la exclusión mutua al imprimir en la consola
    print_lock = threading.Lock()

    # Función para listar los correos electrónicos que contienen la palabra clave y contar su cantidad
    def listar_correos_con_palabra_clave(folder):
        count = 0  # Inicializar contador de correos encontrados en esta carpeta
        for item in folder.sub_items:
            # Verificar si es un correo electrónico
            if isinstance(item, pypff.message):
                if palabra_clave.lower() in item.subject.lower():
                    with print_lock:
                        print(f"Asunto: {item.subject}")
                        print(f"Fecha: {item.delivery_time}")
                        print("------")
                    count += 1  # Incrementar el contador de correos encontrados
            
            # Si hay carpetas anidadas, buscar recursivamente en ellas y sumar al contador
            if isinstance(item, pypff.folder):
                count += listar_correos_con_palabra_clave(item)
        return count
    
    # Función que se ejecuta en un hilo para buscar en una carpeta específica
    def buscar_en_carpeta(folder):
        count = listar_correos_con_palabra_clave(folder)
        return count

    # Lista para almacenar los hilos
    threads = []

    # Contar los correos electrónicos que contienen la palabra clave en el archivo PST utilizando hilos
    for item in root_folder.sub_items:
        if isinstance(item, pypff.folder):
            thread = threading.Thread(target=buscar_en_carpeta, args=(item,))
            threads.append(thread)
            thread.start()
    # Esperar a que todos los hilos hayan terminado
    for thread in threads:
        thread.join()

    # Cerrar el archivo PST
    pst.close()
    end=time.time()

    print(f"Tiempo de ejecución: {end-start} segundos.")  

except Exception as e:
    print(f"Ocurrió un error: {str(e)}")
