# movie-reco

![Diagrama de Arquitectura](src/arquitectura.jpeg)

## Objetivo General
El objetivo general de este proyecto es desarrollar un Producto Mínimo Viable (MVP) que satisfaga consultas a través de una API de FastAPI desplegada en Render.

## Diagrama de Arquitectura
El diagrama de arquitectura del proyecto se muestra en la imagen anterior. A continuación, se explica el proceso paso a paso para lograrlo:

1. **Extracción**: Se realiza la descompresión de archivos para crear los dataframes. Se utiliza el método `zip_to_dataframe` de la librería ETL para descomprimir los archivos "movies_dataset.csv-20230613T164732Z-001.zip" y "credits.csv-20230613T164729Z-001.zip" y crear los dataframes `movie` y `credit`.

2. **Transformación**:

   - **movie**: Se realizan varias transformaciones en el dataframe `movie`, incluyendo:
     - Eliminación de duplicados exactos.
     - Eliminación de registros duplicados con el mismo ID, manteniendo los más recientes.
     - Eliminación de registros con campos incorrectos y sin ID.
     - Eliminación de registros con valores nulos para el campo "release_date".
     - Eliminación de columnas que no serán utilizadas.
     - Cambio de tipo de datos numéricos.
     - Llenado de campos vacíos o incorrectos en películas cuyo nombre es una fecha.
     - Transformación del formato de "release_date" a datetime.
     - Creación de la columna "release_year" para extraer el año de "release_date".
     - Llenado de valores faltantes en los campos "budget" y "revenue" con cero.
     - Asignación de cero en "budget" a películas con un presupuesto inconsistente respecto a los ingresos.
     - Creación de la columna "return" para calcular el retorno de inversión.
     - Inserción de valores faltantes en el campo "overview".
     - Eliminación de la columna "imdb_id".
     - Transformaciones adicionales para preparar los datos para cargar en MongoDB.

   - **credit**: Se realizan transformaciones similares en el dataframe `credit`, incluyendo la eliminación de duplicados y la conversión de tipos de datos.

3. **Carga**:

   - **Merge**: Se combina el dataframe `movie` con el dataframe `credit` mediante la columna "id" utilizando un join tipo "left" y validación de "one_to_one". Se crea un nuevo dataframe `df` que contiene la información relevante para la carga.

   - **Crear csv para el modelo de ML**: Se crea un archivo CSV llamado "df.csv" que se utilizará para alimentar el modelo de Machine Learning. Se guarda en la carpeta "../model/".

   - **Convertir a zip**: Se comprime el archivo CSV en un archivo ZIP llamado "df.zip" utilizando el método `create_zip_file` de la librería ETL. El archivo comprimido se guarda en la carpeta "../model/".

   - **Conectar con MongoDB**: Se establece la conexión con la base de datos de MongoDB utilizando las variables de entorno para el nombre de usuario, contraseña, base de datos y colección. Se carga el dataframe `df` en la colección especificada.

## Modelo

El modelo utilizado en este proyecto se basa en el archivo "df.zip" generado en el proceso de carga. Se realizaron tareas de limpieza, procesamiento y entrenamiento de los datos, las cuales se explican en detalle en el archivo [ETL_y_Modelo.md](ETL_y_Modelo.md).

Para obtener más información sobre el modelo y cómo se utilizó, consulta el archivo mencionado anteriormente.

## API

La API utilizada en este proyecto utiliza tanto la base de datos en MongoDB como el modelo de Machine Learning para proporcionar respuestas a 7 consultas relacionadas con el dataset.

Para obtener más información sobre cómo utilizar la API, consulta el archivo [API.md](API.md).
