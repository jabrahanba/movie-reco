# Movie Data ETL Script

Este script realiza las siguientes operaciones para extraer, transformar y cargar datos relacionados con películas:

1. Extracción de datos:
   - Se importan las librerías y frameworks necesarios.
   - Se carga la configuración desde un archivo de entorno.
   - Se descomprimen archivos para obtener los datos en formato DataFrame.

2. Transformación de datos:
   - Se realizan varias transformaciones en el DataFrame "movie":
     - Eliminación de duplicados exactos.
     - Eliminación de registros duplicados por ID, conservando los más recientes.
     - Transformación de formatos y corrección de campos.
     - Conversión de columnas a tipo string.
     - Transformaciones específicas para preparar los datos para MongoDB.
     - Eliminación de columnas no utilizadas.
   - Se realizan transformaciones en el DataFrame "credit":
     - Eliminación de duplicados exactos.
     - Eliminación de registros duplicados por ID.
     - Conversión de columnas a formato JSON.

3. Carga de datos:
   - Se realiza la combinación de los DataFrames "movie" y "credit" utilizando el ID como clave de unión.
   - Se verifica la integridad del proceso de combinación.
   - Se crea un archivo CSV con los datos combinados para su uso en un modelo de machine learning.
   - Se establece la conexión con MongoDB utilizando variables de entorno.
   - Se crea una colección en la base de datos.
   - Se insertan los datos transformados en MongoDB.
   - Se cierra la conexión con MongoDB.

Este script ofrece un flujo completo de ETL para limpiar, transformar y cargar los datos de películas en un formato adecuado para su análisis y uso posterior.