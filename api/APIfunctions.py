# ----------------------------------------
# Librerias y Framework
# ----------------------------------------

from pymongo import MongoClient
from dotenv import load_dotenv
import re
import os
import unidecode
import pandas as pd
import joblib

#-----------------------------------------
#manejo de las cifras // no lo admite render.com
#-----------------------------------------v
#import locale
#establecer la configuración regional de USA
#locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

# ----------------------------------------
# Variables de entorno
# ----------------------------------------

load_dotenv("../config.env")
user = os.getenv("MONGO_USERNAME")
password = os.getenv("MONGO_PASSWORD")
mongo_db = os.getenv("MONGO_DB")
collection_mongo = os.getenv("MONGO_COLLECTION")

# ----------------------------------------
# Conexión con la base de datos:
# ----------------------------------------

def connection():
    """
    Establece una conexión con una base de datos MongoDB utilizando variables globales
    y retorna la colección especificada.

    Returns:
        client: El cliente de MongoDB utilizado para la conexión.
        collection: Colección en la base de datos.

    """

    url = f"mongodb+srv://{user}:{password}@movies.sytutqr.mongodb.net/"
    client = MongoClient(url)
    db = client[mongo_db]
    collection = db[collection_mongo]

    return client, collection


# ----------------------------------------
# Diccionarios relevantes para las funciones
# ----------------------------------------

# Meses:
meses = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9, #algunas personas dicen así en Perú.
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12
}
# Días de la semana:
dias_semana = {
    'lunes': 2,
    'martes': 3,
    'miércoles': 4,
    'miercoles': 4,
    'jueves': 5,
    'viernes': 6,
    'sábado': 7,
    'sabado': 7, 
    'domingo': 1
}


# ----------------------------------------
# funciones de la API:
# ----------------------------------------


# Funcionabilidad número 1:
async def cantidad_filmaciones_mes(mes:str): 
    """Cuenta la cantidad de filmaciones en un mes específico.

    Args:
        mes (str): El nombre del mes en español y en formato de cadena.

    Returns:
        dict: Un diccionario que contiene la consulta para el mes especificado y la cantidad de filmaciones.

    Raises:
        ValueError: Si el nombre del mes ingresado no es válido.

    """

    client, collection = connection()
    nombre_mes = mes.lower()
    
    if nombre_mes not in meses: # Validar el nombre del mes ingresado
        raise ValueError("Nombre de mes inválido.")
    else:
        numero_mes = meses[nombre_mes] # Obtener el número del mes correspondiente
        mes_formateado = str(numero_mes).zfill(2)# Formatear el número del mes con ceros a la izquierda si es necesario

        # Consulta utilizando expresiones regulares
        query = {
            "release_date": {
                "$regex": fr"-{mes_formateado}-[0-9]{{2}}"
            }
        }
        count = collection.count_documents(query) # Contar los documentos que cumplen con la consulta
        client.close()
        return {f"consulta para {nombre_mes.capitalize()}": count}



# Funcionabilidad número 2:
async def cantidad_filmaciones_dia(dia:str):
    """
    Devuelve la cantidad de filmaciones lanzadas para un día específico de la semana.

    Args:
    - dia (str): El nombre del día de la semana para el cual se desea obtener la cantidad 
    de filmaciones.

    Returns:
        dict: Un diccionario que contiene la consulta para el día especificado y la cantidad 
        de filmaciones lanzadas.

    Raises:
    - ValueError: Si se proporciona un día inválido.

    """
    client, collection = connection()
    nombre_dia = dia.lower()

    if nombre_dia not in dias_semana:  # Validar el nombre del día ingresado
        raise ValueError("Nombre de día inválido.")
    else:
        numero_dia = dias_semana[nombre_dia]  # Obtener el número del día de la semana correspondiente
        # Crear la consulta utilizando la agregación
        query = [
            {
                '$match': {
                    '$expr': {
                        '$eq': [
                            {'$dayOfWeek': {'$dateFromString': {'dateString': '$release_date'}}},
                            numero_dia
                        ]
                    }
                }
            },
            {
                '$count': 'total'
            }
        ]

        result = collection.aggregate(query)  # Ejecutar la consulta
        count = next(result)['total']  # Obtener el resultado del conteo
        client.close()
        return {f"consulta para {nombre_dia.capitalize()}": count}
    


# Funcionabilidad número 3:
async def score_titulo(titulo: str):
    """
    Busca documentos en la colección según un título o fragmento de título proporcionado y devuelve información relacionada.

    Args:
        titulo (str): El título o fragmento del título a buscar.

    Returns:
        list: Una lista de diccionarios que contienen la información de los documentos encontrados. Cada diccionario tiene las siguientes claves:
            - "Título": El título de la filmación.
            - "Año de lanzamiento": El año de lanzamiento de la filmación.
            - "Score": La popularidad de la filmación.

    Raises:
        ValueError: Si no se encuentran resultados para el título especificado.

    """

    client, collection = connection()

    query = {"title": {"$regex": titulo, "$options": "i"}}

    results = collection.find(query)  # Ejecución de la consulta
    results_list = list(results)  # Convertir el cursor en una lista y obtener el número de documentos
    num_results = len(results_list)
    client.close()
    if num_results > 0:  # Verificación de los resultados
        list_movies = []
        for result in results_list:
            # Extracción de los campos
            titulo = result['title']
            release_year = result['release_year']
            popularity = result['popularity']
            popularity = float(popularity) #ESTE ES UN PARCHE, revisar tipos de datos.
            list_movies.append({"Título": titulo, "Año de lanzamiento": release_year, "Score": round(popularity, 2)})

        return list_movies
    else:
        raise ValueError("No se encontraron resultados para el título especificado")  


# Funcionabilidad número 4:
async def votos_titulo(titulo: str):
    """
    Busca todas las filmaciones con el título especificado y devuelve un diccionario
    con información sobre el título, la cantidad de valoraciones y el promedio de valoraciones.

    Args:
        titulo (str): El título a buscar en la base de datos.

    Returns:
        dict or list: Un diccionario con los siguientes campos:
            - 'Título': El título de la filmación encontrada.
            - 'Cantidad de valoraciones': La cantidad de valoraciones del título.
            - 'Promedio de valoraciones': El promedio de valoraciones del título.
        En caso de haber más de una película encontrada, se retorna una lista de diccionarios.
        Si hay películas con menos de 2000 valoraciones, se devuelve una lista adicional con la siguiente estructura:
            - 'Título': El título de la filmación encontrada.
            - 'message': Un mensaje indicando que debe tener al menos 2000 valoraciones.

    Raises:
        ValueError: Si no se encuentran resultados para el título especificado en la base de datos.

    """

    client, collection = connection()
    query = {"title": {"$regex": titulo, "$options": "i"}}

    results = collection.find(query)
    results_list = list(results)  # Convertir el cursor en una lista y obtener el número de documentos
    num_results = len(results_list)
    client.close()

    if num_results > 0:  # Verificación de los resultados
        greater_dosk = []
        less_dosk = []
        for result in results_list:
            titulo = result['title']  # Extracción de los campos
            vote_count = result['vote_count']
            vote_average = result['vote_average']
            if vote_count >= 2000:
                greater_dosk.append({"Título": titulo, "Cantidad de valoraciones": vote_count,
                                        "Promedio de valoraciones": round(vote_average, 2)})
            else:
                less_dosk.append({"Título": titulo, "message": "Debe tener al menos 2000 valoraciones"})

        if greater_dosk:  # validaciones para devolver las listas
            if less_dosk:
                return greater_dosk, less_dosk
            else:
                return greater_dosk
        elif less_dosk:
            return less_dosk
        else:
            return []
    else:
        raise ValueError("No se encontraron resultados para el título especificado.")
        


# Funcionabilidad número 5:
async def get_actor(name_actor: str):
    """
    Obtiene información sobre un actor específico.

    Args:
        name_actor (str): El nombre del actor.

    Returns:
        dict: Un diccionario que contiene información sobre el actor, incluyendo su nombre, la cantidad de películas en las que ha actuado,
            el retorno total acumulado y el promedio de retorno por películas en las que ha tenido retorno.
            - Actor: El nombre del actor.
            - Cantidad de películas en las que ha actuado: La cantidad de películas en las que el actor ha participado.
            - Retorno total (éxito)": El retorno total acumulado de las películas en las que el actor ha participado.
            - Promedio por películas con retorno: El promedio de retorno por las películas en las que el actor ha tenido retorno.

    Raises:
        ValueError: Si no se encuentran resultados para la persona especificada.

    """

    client, collection = connection()
    #query = {"cast.name": name_actor}
    name_actor_normalized = unidecode.unidecode(name_actor)  # Eliminar acentos del nombre del actor
    query = {"cast.name": {"$regex": f"^{name_actor_normalized}$", "$options": "i"}}  # Utiliza una expresión regular para buscar el nombre del actor sin importar mayúsculas, minúsculas ni acentos

    results = collection.find(query)
    num_results = collection.count_documents(query)
    if num_results > 0:  # Verificación de los resultados
        revenue_acum = 0
        budget_acum = 0
        return_acum = 0
        return_greater_zero = 0
        for result in results:
            return_ = result['return'] # Extracción de los campos
            budget = result['budget'] 
            revenue = result['revenue']
            budget_acum += budget
            revenue_acum += revenue
            if return_ > 0:
                return_acum += return_
                return_greater_zero += 1
        client.close()
        # El retorno del actor fue calculado con el acumulado de los revenue y budget.
        # Sacar un promedio de porcentajes/indicadores/kpi genera BIAS si estamos utilizandos presupuestos de peliculas diferentes
        # por ejemplo, si en una pelicula se ganó 100 pero se gasto gastó 200 el retorno es 0.5
        # y en una pelicula en donde se ganó 100000 pero se gastó 200000 el retorno también es 0.5, diferentes magnitudes
        # por eso se calcula usando la suma de los revenue y los budget, para premiar o castigar el éxito de forma justa.
        # El promedio (solicitado) si se hizo calculando el promedio de los porcentajes, pues es lo requerido.
        return {
            "Actor": name_actor_normalized.capitalize(),
            "Cantidad de peliculas en las que ha actuado": num_results,
            "Retorno total (éxito)": round((revenue_acum/budget_acum), 2),
            "Promedio de retorno por peliculas con retorno": round((return_acum)/ return_greater_zero, 2)}
    else:
        client.close()
        raise ValueError("No se encontraron resultados para el actor/actriz especificado.")
        


'''Está función la creé de más. No estará en el deploy
# Funcionabilidad número 5.2: teniendo en cuenta: La definición no deberá considerar directores
async def get_actor2(name_actor: str):
    """
    Obtiene información sobre un actor específico.

    Args:
        name_actor (str): El nombre del actor.

    Returns:
        dict: Un diccionario que contiene información sobre el actor, incluyendo su nombre, la cantidad de películas en las que ha actuado,
            el retorno total acumulado y el promedio de retorno por películas en las que ha tenido retorno.
            - Actor: El nombre del actor.
            - Cantidad de películas en las que ha actuado: La cantidad de películas en las que el actor ha participado, excluyendo las películas en las que también ha sido director.
            - Retorno total (éxito)": El retorno total acumulado de las películas en las que el actor ha participado.
            - Promedio por películas con retorno: El promedio de retorno por las películas en las que el actor ha tenido retorno.

    Raises:
        ValueError: Si no se encuentran resultados para la persona especificada.

    Note:
        No se devuelven las películas en las que el actor haya sido el director también.
    """

    client, collection = connection()
    name_actor_normalized = unidecode.unidecode(name_actor)  # Eliminar acentos del nombre del actor
    query = {
        "$and": [
            {"cast": {"$elemMatch": {"name": {"$regex": f"^{name_actor_normalized}$", "$options": "i"}}}},  # Utiliza una expresión regular para buscar el nombre del actor sin importar mayúsculas, minúsculas ni acentos
            {
                "$nor": [
                    {
                        "$and": [
                            {"crew": {"$elemMatch": {"job": "Director", "name": {"$regex": f"^{name_actor_normalized}$", "$options": "i"}}}},
                            {"cast": {"$elemMatch": {"name": {"$regex": f"^{name_actor_normalized}$", "$options": "i"}}}},
                        ]
                    }
                ]
            },
        ]
    }
    results = collection.find(query)
    num_results = collection.count_documents(query)
    if num_results > 0:  # Verificación de los resultados
        revenue_acum = 0
        budget_acum = 0
        return_acum = 0
        return_greater_zero = 0
        for result in results:
            return_ = result['return'] # Extracción de los campos
            budget = result['budget'] 
            revenue = result['revenue']
            budget_acum += budget
            revenue_acum += revenue
            if return_ > 0:
                return_acum += return_
                return_greater_zero += 1
        client.close()
        # El retorno del actor fue calculado con el acumulado de los revenue y budget.
        # Sacar un promedio de porcentajes/indicadores/kpi genera BIAS si estamos utilizandos presupuestos de peliculas diferentes
        # por ejemplo, si en una pelicula se ganó 100 pero se gasto gastó 200 el retorno es 0.5
        # y en una pelicula en donde se ganó 100000 pero se gastó 200000 el retorno también es 0.5, diferentes magnitudes
        # por eso se calcula usando la suma de los revenue y los budget, para premiar o castigar el éxito de forma justa.
        # El promedio (solicitado) si se hizo calculando el promedio de los porcentajes, pues es lo requerido.
        return {
            "Actor": name_actor_normalized.capitalize(),
            "Cantidad de peliculas en las que ha actuado": num_results,
            "Retorno total (éxito)": round((revenue_acum/budget_acum), 2),
            "Promedio de retorno por peliculas con retorno": round((return_acum)/ return_greater_zero, 2)}
    else:
        client.close()
        raise ValueError("No se encontraron resultados para el actor/actriz especificado.")'''
        


# Funcionabilidad número 6:
async def get_director(director: str):
    """
    Busca las películas asociadas a un director específico en una base de datos y devuelve información sobre ellas.

    Args:
        director (str): El nombre del director a buscar.

    Returns:
        dict: Un diccionario que contiene la información del director y las películas asociadas. El diccionario tiene las siguientes claves:
            - 'Director': El nombre del director.
            - 'Retorno total del director': El retorno total de todas las películas del director 
               (sumatoria del revenue de todas las películas / sumatoria del budget de todas las películas).
            - 'Películas': Una lista de diccionarios, donde cada diccionario representa una película y tiene las siguientes claves:
                - 'Título': El título de la película.
                - 'Año de lanzamiento': El año de lanzamiento de la película.
                - 'Retorno de la película': El retorno de la película.
                - 'Costo de la película': El costo de la película.
                - 'Ganancia de la película': La ganancia de la película.

    Raises:
        ValueError: Si no se encontraron resultados para el director especificado.
    """
    client, collection = connection()
    #query = {"crew": {"$elemMatch": {"job": "Director", "name": director}}}
    director_normalized = unidecode.unidecode(director)  # Eliminar acentos del nombre del director
    query = {"crew": {"$elemMatch": {"job": "Director", "name": {"$regex": f"^{director_normalized}$", "$options": "i"}}}}  # Utiliza una expresión regular para buscar el nombre del director sin importar mayúsculas, minúsculas ni acentos

    results = collection.find(query)
    num_results = collection.count_documents(query)
    if num_results > 0:  # Verificación de los resultados
        films = []
        revenue_acum = 0
        budget_acum = 0
        for result in results:
            return_ = result['return']  # Este es para mostrar el de cada pelicula
            titulo = result['title']
            anio = result['release_year']
            budget = result['budget']
            revenue = result['revenue']
            budget_acum += budget
            revenue_acum += revenue
            films.append({
                "Título": titulo,
                "Año de lanzamiento": anio,
                "Retorno de la película": round(return_, 2),
                "Costo de la película": budget,
                #"Costo de la película": locale.currency(budget, grouping=True),
                "Ganancia de la película": revenue
                #"Ganancia de la película": locale.currency(revenue, grouping=True),
            })
        client.close()
        # El retorno del director fue calculado con el acumulado de los revenue y budget.
        # Sacar un promedio de porcentajes/indicadores/kpi genera BIAS si estamos utilizandos presupuestos de peliculas diferentes
        # por ejemplo, si en una pelicula se ganó 100 pero se gasto gastó 200 el retorno es 0.5
        # y en una pelicula en donde se ganó 100000 pero se gastó 200000 el retorno también es 0.5, diferentes magnitudes
        # por eso se calcula usando la suma de los revenue y los budget, para premiar o castigar el éxito de forma justa.
        return {
            "Director": director_normalized.capitalize(),
            "Retorno total del director (éxito)": round((revenue_acum/budget_acum), 2),
            "Películas": films
        }
    else:
        client.close()
        raise ValueError("No se encontraron resultados para el director especificado.")
    

# Funcionabilidad número 7:
async def recomendacion(title:str):
    """
    Recomienda películas similares en base a un título dado.
    Args:
        title (str): Título de la película.
    Returns:
        dict: Diccionario con las películas recomendadas.
    """
    #Data
    pelis = pd.DataFrame
    pelis = pd.read_csv('../model/df_resize.csv', index_col=False) 
    df = pd.read_csv('../model/df_model.csv', index_col=False) 
    trained_model_filename = '../model/trained_model.joblib' 
    vectorizer_filename = '../model/vectorizer.joblib'
    column = column = 'stemming_unique'

    #Obtener index:
    pelis['title_lower'] = pelis['title'].str.lower()
    title = title.lower()
    title_index = pelis[pelis['title_lower'] == title].index.tolist()
    
    if not title_index:
        raise ValueError("La película no se encuentra en la base de datos.")
    else:
        movie_id = title_index[0]  # Devolver el primer índice encontrado

    # Cargar el modelo entrenado desde el archivo
    knn = joblib.load(trained_model_filename)
        # Cargar el vectorizador
    vectorizer = joblib.load(vectorizer_filename)

    # Obtener las características de la película de interés
    df[column] = df[column].apply(lambda x: x.replace("[", "").replace("]", "").replace("'", "").replace(",", " "))
    pelicula_interes = df.iloc[movie_id][column]
    pelicula_interes_vec = vectorizer.transform([pelicula_interes])

    # Calcular distancias utilizando el modelo cargado
    distances, indices = knn.kneighbors(pelicula_interes_vec)
    indices_similares = indices[0][1:]  # Excluir la película de interés
    peliculas_similares = df.iloc[indices_similares]
    indices = peliculas_similares.index

    # Obtener los títulos de las películas recomendadas
    recommended_movies = pelis.loc[indices, 'title']
    movie_dict = {}
    for i, movie in enumerate(recommended_movies, 1):
        movie_dict[str(i)] = movie

    return {'peliculas recomendadas': movie_dict}