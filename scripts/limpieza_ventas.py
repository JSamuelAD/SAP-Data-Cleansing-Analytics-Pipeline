# Importamos las librerías necesarias
import pandas as pd
import re
import sqlite3
import os
import logging

# --- CONFIGURACIÓN PROFESIONAL DE LOGGING ---
# En lugar de usar print(), usamos logging para registrar eventos en un archivo.
# Esto es estándar en proyectos serios.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("proceso_etl.log"), # Guarda los logs en un archivo
        logging.StreamHandler() # También muestra los logs en la consola
    ]
)

# --- DEFINICIÓN DE FUNCIONES MODULARES ---

def extraer_datos(carpeta_datos):
    """
    Busca todos los archivos CSV en una carpeta, los lee y los une en un solo DataFrame.
    """
    logging.info(f"Iniciando extracción de datos de la carpeta: {carpeta_datos}")
    archivos_csv = [f for f in os.listdir(carpeta_datos) if f.endswith('.csv')]
    if not archivos_csv:
        logging.warning("No se encontraron archivos CSV en la carpeta.")
        return None

    lista_df = []
    for archivo in archivos_csv:
        ruta_completa = os.path.join(carpeta_datos, archivo)
        try:
            df = pd.read_csv(ruta_completa)
            lista_df.append(df)
            logging.info(f"Archivo '{archivo}' leído correctamente.")
        except Exception as e:
            logging.error(f"Error al leer el archivo '{archivo}': {e}")
            continue
    
    if not lista_df:
        logging.error("No se pudo leer ningún archivo CSV con datos.")
        return None
        
    df_completo = pd.concat(lista_df, ignore_index=True)
    logging.info(f"Extracción completada. {len(df_completo)} filas consolidadas.")
    return df_completo

def transformar_datos(df):
    """
    Aplica las limpiezas y transformaciones necesarias al DataFrame.
    """
    logging.info("Iniciando la fase de transformación de datos...")
    if df is None:
        logging.error("El DataFrame de entrada es nulo. Saltando transformación.")
        return None

    # Función anidada para la limpieza, ya que solo se usa aquí
    def limpiar_y_convertir_a_float(valor):
        if pd.isna(valor):
            return None
        limpio = re.sub(r'[^\d.]', '', str(valor))
        if not limpio:
            return None
        try:
            return float(limpio)
        except ValueError:
            return None

    # Aplicamos la limpieza a las columnas objetivo
    df['Unit Selling Price (RMB/kg)'] = df['Unit Selling Price (RMB/kg)'].apply(limpiar_y_convertir_a_float)
    df['Quantity Sold (kilo)'] = df['Quantity Sold (kilo)'].apply(limpiar_y_convertir_a_float)
    logging.info("Columnas de precio y cantidad limpiadas y convertidas a numérico.")
    
    # Aquí podrías añadir más transformaciones en el futuro (ej. convertir fechas)
    
    return df

def cargar_datos(df, ruta_db, nombre_tabla):
    """
    Carga el DataFrame transformado a una base de datos SQLite.
    """
    logging.info(f"Iniciando la carga de datos a la tabla '{nombre_tabla}' en la base de datos '{ruta_db}'...")
    if df is None:
        logging.error("El DataFrame de entrada es nulo. No hay datos para cargar.")
        return

    try:
        conn = sqlite3.connect(ruta_db)
        df.to_sql(nombre_tabla, conn, if_exists='replace', index=False)
        logging.info("¡Carga completada! Los datos se han guardado en la base de datos.")
    except sqlite3.Error as e:
        logging.error(f"Error de SQLite al cargar los datos: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

# --- BLOQUE PRINCIPAL DE EJECUCIÓN ---
# Esta es una práctica estándar en Python para hacer los scripts reutilizables.
if __name__ == "__main__":
    
    # Definimos nuestras variables principales aquí
    CARPETA_DATOS = 'data'
    RUTA_DB = 'proyecto.db'
    NOMBRE_TABLA = 'ventas_limpias'
    
    # Orquestamos la ejecución del ETL
    df_crudo = extraer_datos(CARPETA_DATOS)
    df_transformado = transformar_datos(df_crudo)
    cargar_datos(df_transformado, RUTA_DB, NOMBRE_TABLA)