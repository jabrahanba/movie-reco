# ----------------------------------------
# Librerias y Frameworks:
# ----------------------------------------

import zipfile
import pandas as pd
from datetime import datetime, time
import ast
import json


# ----------------------------------------
# Funciones de extracción (E) 
# ----------------------------------------

def zip_to_dataframe(file_path):
    """
    Extrae un archivo ZIP y devuelve su contenido en forma de dataframe, se supone
    que solo hay un archivo en cada ZIP y que son tipo csv o xlsx.

    Args:
        file_path (str): Ruta del archivo ZIP.

    Returns:
        pandas.DataFrame: El contenido del archivo ZIP en forma de dataframe.

    Raises:
        ValueError: Si el archivo ZIP contiene más de un archivo o la extensión del archivo extraído no es compatible.

    Example:
        ruta_zip = "ruta/archivo.zip"
        dataframe = zip_to_dataframe(ruta_zip)
    """
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        file_list = zip_ref.namelist()
        if len(file_list) != 1:
            raise ValueError("El archivo ZIP debe contener un solo archivo.")

        file_extension = file_list[0].split('.')[-1]

        if file_extension == 'csv':
            with zip_ref.open(file_list[0]) as file:
                df = pd.read_csv(file)
        elif file_extension == 'xlsx':
            with zip_ref.open(file_list[0]) as file:
                df = pd.read_excel(file)
        else:
            raise ValueError("La extensión del archivo no es compatible.")

    return df

# ----------------------------------------
# Funciones de Transformación (T) 
# ----------------------------------------

def convert_to_str(value):
    """
    Convierte el valor proporcionado a una representación en formato de cadena de caracteres.

    Args:
        value: El valor que se desea convertir a cadena de caracteres.

    Returns:
        str: La representación en formato de cadena de caracteres del valor proporcionado.

    """
    if isinstance(value, (datetime, time)):
        return str(value)
    return value


def transform_columns(cell, name):
    """
    Transforma una celda que contiene una lista de diccionarios en un diccionario con un nombre de columna especificado.

    Args:
        cell: El valor de la celda que contiene la lista de diccionarios.
        name: El nombre de columna que se asignará al diccionario transformado.

    Returns:
        dict or None: El diccionario transformado con el nombre de columna especificado, o None si la celda es nula,
        una cadena que contiene '[]' o False.

    """
    if pd.isnull(cell) or cell == '[]' or cell is False:
        return None
    genres_list = ast.literal_eval(cell)
    sorted_genres = sorted(genres_list, key=lambda x: x['name'])
    transformed_cell = {name: sorted_genres}
    return transformed_cell

def convert_to_json(texto):
    """
    Convierte una cadena de texto en formato JSON.

    Args:
        texto (str): La cadena de texto a convertir.

    Returns:
        dict: Un objeto JSON resultado de la conversión.

    """
    estructura_datos = ast.literal_eval(texto)
    texto_json = json.dumps(estructura_datos)
    objeto_json = json.loads(texto_json)
    return objeto_json