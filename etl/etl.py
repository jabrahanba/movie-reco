### 1- Librerias y Frameworks
import pandas as pd
import ast
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import ELTfunctions as etl

load_dotenv("../config.env")

### 2- Extracción: Descompresión de archivos para crear los df.
movie = etl.zip_to_dataframe("movies_dataset.csv-20230613T164732Z-001.zip")
credit = etl.zip_to_dataframe("credits.csv-20230613T164729Z-001.zip")

### 3- Transformación:

### 3.1 movie:

#Eliminar duplicados exactos
movie = movie.drop_duplicates()

#Eliminar registros duplicados con el mismo ID, considerando que los IDs más grandes corresponden a los datos más recientes.
movie = movie.drop_duplicates(subset='id', keep='last') 

#Eliminar 3 registros que tienen campos incorrectos (parecen desfasados) y no tienen id:
movie = movie.drop(movie[pd.to_numeric(movie['belongs_to_collection'], errors='coerce').notnull()].index)

#Transformar release_date a datetime:
movie["release_date"] = pd.to_datetime(movie["release_date"])
movie["release_date"] = movie["release_date"].dt.strftime("%Y-%m-%d")

#Llenar campos vacios o incorrectos de peliculas cuyo nombre es una fecha,
# esta información se extrajo de la pagina de IMDB:
date_movies = {
    11304: ['09/11', '09/11'],
    30860: ['May 6th', 'May 6th'],
    21841: ['June 9', 'June 9'],
    51248: ['11/11/11', '11/11/11'],
    52681: ['September 30, 1955', 'September 30, 1955'],
    19430: ['08/15', '08/15', 'Shortly before the outbreak of World War II: Private Asch and gunner Vierbein belong to the same unit but could not be more contrary.'],
    332179: ['April 9th', 'April 9th'],
    79078: ['11/11/11', '11/11/11'],
    325803: ['October 1', 'October 1'],
    356294: ['25 April', '25 April']
}
for movie_id, values in date_movies.items():
    movie.loc[movie['id'] == movie_id, ['original_title', 'title']] = values[:2]  # Asignar original_title y title
    if len(values) == 3:
        movie.loc[movie['id'] == movie_id, 'overview'] = values[2]  # Asignar overview si está presente

#Transformar a string para no tener problemas con tipos de datos al cargar:
movie = movie.applymap(etl.convert_to_str)

#transformaciones para poder cargar a mongoDB.
#Diccionarios anidados:
movie['belongs_to_collection'] = movie['belongs_to_collection'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else {'data': None})
#lista de diccionarios:
columnas = ['genres','production_companies','production_countries', 'spoken_languages']
for col in columnas:
    movie[col] = movie[col].apply(lambda x: etl.transform_columns(x, col))

#Llenar los valores nulos de los campos revenue, budget con el número 0.
movie['revenue'] = movie['revenue'].fillna(0)
movie['budget'] = movie['budget'].fillna(0)

#Eliminar los registros con valores nulos para el campo release_date.
movie = movie.dropna(subset=['release_date'])

#Crear la columna release_year donde extraerán el año de release_date.
movie['release_year'] = movie['release_date'].apply(lambda x: x.split('-')[0])

#Crear la columna con el retorno de inversión,teniendo en cuenta que no sea NaN o Infinity
movie['budget'] = pd.to_numeric(movie['budget'], errors='coerce').fillna(0)
movie['return'] = movie['revenue'] / movie['budget'].replace(0, 1)

#Eliminar las columnas que no serán utilizadas
columnas_a_eliminar = ["video", "imdb_id", "adult", "original_title", "poster_path", "homepage"]
movie = movie.drop(columnas_a_eliminar, axis=1)


### 3.3 credit:

#Eliminar duplicados exactos
credit = credit.drop_duplicates()

#Eliminar registros duplicados por ID, considerando que 
#los diccionarios están ordenados de manera diferente pero 
#son idénticos en contenido.
credit = credit.drop_duplicates(subset='id', keep='last')

#convertir a json los diccionarios para poder cargarlos a mongoDB
credit['cast'] = credit['cast'].apply(etl.convert_to_json)
credit['crew'] = credit['crew'].apply(etl.convert_to_json)

### 4- Carga:

### 4.1 Merge:

#Combinar/unir ambos datasets:
movie['id'] = movie['id'].astype('int64')
df = movie.merge(credit, on='id', how='left', validate='one_to_one')
#Validar si el merge y la transformación son correctas:

### 4.2 Crear csv para usar en el modelo de ML:
df.to_csv("../model/df.csv", index=False)

### 4.3 Conectar con mongoDB:

#Variables de entorno:
user = os.getenv("MONGO_USERNAME")
password = os.getenv("MONGO_PASSWORD")
mongo_db = os.getenv("MONGO_DB")
collection_mongo = os.getenv("MONGO_COLLECTION") 

url = f"mongodb+srv://{user}:{password}@movies.sytutqr.mongodb.net/"

with MongoClient(url) as client:
    db = client[mongo_db]
    collection = db[collection_mongo]

    # Carga a MongoDB:
    datos_json = df.to_dict(orient='records')
    collection.insert_many(datos_json)