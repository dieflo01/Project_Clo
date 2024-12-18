# Proyecto Cloud y Big Data: Análisis del top de Spotify desde 2017

## Indice
1. Descripción del problema
2. Necesidad de Big Data y Cloud
3. Descripción de los datos
4. Descripción del proyecto
5. Diseño del software
6. Uso
7. Conclusiones

## 1. Descripción del problema
En el mundo actual, donde la demanda y oferta de música crecen a un ritmo acelerado, los datos juegan un papel crucial para comprender tendencias, preferencias y comportamientos en la industria musical. Queremos analizar las diferentes estadísticas relacionadas con el Top 200 de Spotify desde 2017 hasta 2021.

Dado que estas listas se actualizan cada 2-3 días, nos enfrentamos a un conjunto de datos extenso y complejo, que contiene información detallada sobre las canciones más populares y virales a lo largo de 4 años. Organizar estos datos de manera estructurada es esencial para compararlos y extraer análisis claros y significativos.

## 2. Necesidad de Big Data y Cloud
El análisis del Top 200 de Spotify requiere soluciones de Big Data y Cloud Computing debido al volumen y complejidad de los datos. Con más de 27 GB, este proyecto necesita infraestructura capaz de procesar grandes volúmenes de información de manera eficiente. Además, la diversidad de las variables y el crecimiento potencial del dataset hacen imprescindible utilizar herramientas escalables y flexibles.

Google Cloud ofrece almacenamiento dinámico, capacidad de procesamiento elástico y acceso global, lo que permite realizar análisis rápidos, colaborativos y generar visualizaciones claras desde cualquier ubicación. Estas tecnologías no solo garantizan eficiencia en el manejo de datos, sino que también optimizan costos y aseguran que el proyecto esté preparado para crecer en el futuro.

## 3. Descripción de los datos
Es una base de datos de unos 27GB que hemos encontrado en Kaggle. Esta base de datos cuenta con 29 columnas con diferentes datos, estas columnas son:
Id, Title, Rank, Date, Artist, Url, Region, Chart, Trend, Streams, Track_Id, Album, Popularity, Duration, Explicit, Release_Date, Available_markets, AF_Danceability, AF_Energy, AF_Key, AF_Loudness, AF_Mode, AF_Speechiness, AF_Acousticness, AF_Instrumentalness, AF_Liveness, AF_Valence, AF_Tempo, AF_Time_signature.

Donde además de los datos evidentes de la canción como su nombre, el artista, etc. Encontramos también el número de reproducciones, en que paises ha subido al top, o datos más subjetivos como los que van precedidos por AF, como por ejemplo como de bailable es, si es musica en directo, etc.

## 4. Descripción del proyecto
La aplicación se centra en el análisis de tendencias musicales a partir de datos históricos del Top 200 de Spotify. Su objetivo es procesar, organizar y analizar un gran volumen de datos para identificar patrones, características comunes de las canciones exitosas y variaciones en popularidad entre regiones y categorías.
El modelo de programación se basa en el uso de DataFrames. Este enfoque facilita el procesamiento y la manipulación de grandes volúmenes de información mediante operaciones similares a SQL, como filtros, agregaciones y uniones.

## 5. Diseño del software
El lenguaje usado es python, y para el procesamiento de datos usamos spark.

## 6. Uso
El dataset merged_data.csv se encuentra en el bucket del proyecto en la carpeta project.
Para el procesamiento de los datos se usa el script process.py que tiene todas las operaciones necesarias para hacer el análisis deseado.

## 7. Conclusiones
Hemos conseguido hacer un breve análisis de las canciones que han tenido más éxito en Spotify y por tanto entre la gente que disfruta de escuchar música, hemos podido ver las diferentes tendencias y como es principalmente el mismo tipo de música el que acaba triunfando. Pero aparte, también nos ha servido para lograr comprender mejor el funcionamiento del big data y cloud así como de los dataframes que ha sido lo que hemos usado principalmente.

## Autores
Diego Flores Simón,
Álvaro Gómez Tejedor
