import pypff
import pandas as pd

df=pd.DataFrame(columns=['Asunto','Fecha'])

def listar_correos(folder):
        count = 0  # Inicializar contador de correos encontrados en esta carpeta
        for item in folder.sub_items:
            # Verificar si es un correo electrónico
            if isinstance(item, pypff.message):
                df.loc[count]={'Asunto':item.subject,'Fecha':item.delivery_time}
                count += 1  # Incrementar el contador de correos encontrados
            
            # Si hay carpetas anidadas, buscar recursivamente en ellas y sumar al contador
            if isinstance(item, pypff.folder):
                count += listar_correos(item)
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

listar_correos(root_folder)

df.to_csv(archivo_csv, index=False)