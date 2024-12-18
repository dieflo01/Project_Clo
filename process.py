from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, countDistinct, sum as _sum, split, explode, trim, regexp_replace, desc, when, avg, collect_list, concat_ws, slice
import sys

# Crear una SparkSession
spark = SparkSession.builder.appName("PruebaPySpark").getOrCreate()

# Cargar el archivo como un DataFrame
input_path = sys.argv[1]
output_path_a = sys.argv[2]
output_path_c = sys.argv[3]
output_path_y = sys.argv[4]

data = spark.read.csv(input_path, header=True, sep=',')

# Procesar el DataFrame
resultArtists = (
    data
    .filter(col("chart") == "top200")  # Filtrar filas con valores válidos en 'mass'
    .withColumn("artist_list", split(col("artist"), ",")) # Separar los artistas en una lista
    .withColumn("artist", explode(col("artist_list"))) # Explode para crear una fila por cada artista
    .withColumn("artist", trim(col("artist"))) # Eliminar espacios adicionales en los nombres de los artistas
    .withColumn("artist", regexp_replace(col("artist"), r'\\|\"', ''))  # Limpiar los nombres de los artistas (remover comillas y barras invertidas)
    .groupBy("artist").agg(
        countDistinct("title").alias("unique_songs"),  # Número de canciones únicas
        _sum("streams").alias("total_streams"),        # Total de streams
        countDistinct(when(col("rank") == 1, col("title"))).alias("songs_rank_1"), # Número de canciones que llegaron al rank 1
        countDistinct("album").alias("number_albums"),
        countDistinct("region").alias("number_countries"),
        avg("AF_Danceability").alias("avg_danceability"),  # Promedio de AF_Danceability, cuanto de bailable es
        avg("AF_Speechiness").alias("avg_speechiness"),  # Promedio de AF_Speechiness, palabras habladas 
        avg("AF_Acousticness").alias("avg_acousticness"),  # Promedio de AF_Acousticness, si la cancion está hecho con instrumentos o con ordenador. 1 solo instrumentos
        avg("AF_Valence").alias("avg_valence") # 1 alegre, 0 melancolica 
    )
)

resultCountries = (
    data
    .filter(col("chart") == "top200")  # Filtrar filas con valores válidos en 'mass'
    .groupBy("region").agg(
        countDistinct("title").alias("unique_songs"),  # Número de canciones únicas
        _sum("streams").alias("total_streams"),        # Total de streams
        countDistinct(when(col("rank") == 1, col("title"))).alias("songs_rank_1"), # Número de canciones que llegaron al rank 1
        countDistinct("album").alias("number_albums"),
        avg("AF_Danceability").alias("avg_danceability"),  # Promedio de AF_Danceability, cuanto de bailable es
        avg("AF_Speechiness").alias("avg_speechiness"),  # Promedio de AF_Speechiness, palabras habladas 
        avg("AF_Acousticness").alias("avg_acousticness"),  # Promedio de AF_Acousticness, si la cancion está hecho con instrumentos o con ordenador. 1 solo instrumentos
        avg("AF_Valence").alias("avg_valence") # 1 alegre, 0 melancolica 
    )
)

# Filtrar solo las filas relevantes y ordenar por streams dentro de cada año
rankedSongs = (
    data
    .filter(col("chart") == "top200")  # Filtrar solo las filas del top200
    .withColumn("year", year(col("date")))  # Extraer el año
    .groupBy("year", "title")  # Agrupar por año y canción
    .agg(_sum("streams").alias("total_streams"))  # Sumar los streams por canción en cada año
    .orderBy("year", desc("total_streams"))  # Ordenar por año y luego por streams en orden descendente
)

# Seleccionar solo las top 10 canciones por año
top10SongsPerYear = (
    rankedSongs
    .groupBy("year")  # Agrupar nuevamente por año
    .agg(
        collect_list("title").alias("all_songs"),  # Recolectar las canciones del año en una lista
        collect_list("total_streams").alias("all_streams")  # Recolectar los streams en una lista para depuración
    )
    .withColumn("top_songs", slice(col("all_songs"), 1, 5))  # Seleccionar solo las primeras 10 canciones
)

# Convertir la lista de las canciones top 10 en una cadena de texto
top10SongsPerYear = top10SongsPerYear.withColumn("top_songs", concat_ws(", ", col("top_songs")))

# Agregar las métricas adicionales
resultYears = (
    data
    .filter(col("chart") == "top200")  # Filtrar filas con valores válidos en 'chart'
    .withColumn("year", year(col("date")))
    .groupBy("year")
    .agg(
        countDistinct("title").alias("unique_songs"),  # Número de canciones únicas
        _sum("streams").alias("total_streams"),        # Total de streams
        countDistinct(when(col("rank") == 1, col("title"))).alias("songs_rank_1"),  # Número de canciones en rank 1
        avg("AF_Danceability").alias("avg_danceability"),  # Promedio de AF_Danceability
        avg("AF_Speechiness").alias("avg_speechiness"),  # Promedio de AF_Speechiness
        avg("AF_Acousticness").alias("avg_acousticness"),  # Promedio de AF_Acousticness
        avg("AF_Valence").alias("avg_valence")  # Promedio de AF_Valence
    )
)

# Unir las canciones top 10 al resultado final
resultYears = resultYears.join(top10SongsPerYear.select("year", "top_songs"), on="year", how="left")

# Ordena por artista
resultArtists = resultArtists.filter(col("total_streams") >= 1000000000).orderBy(desc("total_streams"))
resultCountries = resultCountries.orderBy(desc("total_streams"))
resultYears = resultYears.orderBy("year")

# Guardar el resultado
resultArtists.write.mode("overwrite").csv(output_path_a, header=True)
resultCountries.write.mode("overwrite").csv(output_path_c, header=True)
resultYears.write.mode("overwrite").csv(output_path_y, header=True)
