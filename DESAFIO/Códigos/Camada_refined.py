from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, dense_rank, col, lit, when, collect_list, size
from pyspark.sql.window import Window

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("Filmes").getOrCreate()

# Caminho para a camada Trusted
arquivo_parquet_trusted = "s3://NOME DO SEU CAMINHO"

# Carrega os dados da camada Trusted
df = spark.read.parquet(arquivo_parquet_trusted)

# Explode os gêneros
dataframe_genero_explodido = df.withColumn("genero", explode("generos"))

# Adiciona IDs aos gêneros
dataframe_generos_com_id = dataframe_genero_explodido.withColumn("id_genero", dense_rank().over(Window.orderBy("genero")))

# Cria a dimensão de gênero/subgênero
dim_genero = dataframe_generos_com_id.select("id_genero", col("genero").alias("subgenero")).distinct()

# Cria a dimensão de filme
fato_filmes = df.select("id_filme", "titulo", "data_lancamento", "media_de_votos", "popularidade", "receita", "tempo_duracao(minutos)", "total_votos").distinct()

# Realiza o join entre o dataframe original e a dimensão de gênero para obter os IDs correspondentes
dim_genero = dataframe_generos_com_id.join(
    dim_genero,
    (dataframe_generos_com_id['genero'] == dim_genero['subgenero']) & (dataframe_generos_com_id['id_genero'] == dim_genero['id_genero']),
    'left_outer'
).select(
    dataframe_generos_com_id['id_filme'],
    "subgenero",
    "data_lancamento",
    "media_de_votos",
    "popularidade",
    "receita",
    "tempo_duracao(minutos)",
    "total_votos",
    "titulo"
)

# Adiciona coluna indicando se é um subgênero (excluindo "Crime")
dim_genero = dim_genero.withColumn("subgenero", when(col("subgenero") != "Crime", col("subgenero")).otherwise(None))

# Agrupa por filme e coleta os subgêneros
dim_subgenero = dim_genero.groupBy("id_filme").agg(
    collect_list("subgenero").alias("subgeneros")
)

# Extraindo até 4 subgêneros distintos para cada filme
for i in range(1, 5):
    dim_subgenero = dim_subgenero.withColumn("Subgenero_" + str(i), when(size("subgeneros") >= i, col("subgeneros")[i - 1]).otherwise(None))

# Inclui os dados dos subgêneros não relacionados a "Crime"
subgeneros_nao_crime = dataframe_genero_explodido.filter(col("genero") != "Crime")
subgeneros_nao_crime = subgeneros_nao_crime.select(
    "id_filme",
    col("genero").alias("subgenero"),  # Renomeia a coluna para subgenero
    "titulo",
    "data_lancamento",
    "media_de_votos",
    "popularidade",
    "receita",
    "tempo_duracao(minutos)",
    "total_votos"
)

# Caminho para a camada Refined
caminho_refined = "s3://SUA SAÍDA"

# Salva as tabelas também em formato CSV
dim_genero.write.csv(caminho_refined + "dim_genero_CSV", header=True, mode="overwrite")
fato_filmes.write.csv(caminho_refined + "fato_filmes_CSV", header=True, mode="overwrite")
dim_subgenero.write.csv(caminho_refined + "dim_subgenero_CSV", header=True, mode="overwrite")
subgeneros_nao_crime.write.csv(caminho_refined + "subgeneros_nao_crime_CSV", header=True, mode="overwrite")

# Exibe os dados
dim_subgenero.show()
dim_genero.show()
fato_filmes.show()
subgeneros_nao_crime.show(truncate=False)





























------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, dense_rank, col, lit, when, collect_list, size
from pyspark.sql.window import Window

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("Filmes").getOrCreate()

# Caminho para a camada Trusted
arquivo_parquet_trusted = "s3://data-lake-kesley/trusted-zone/FilmesJSON/parquet/part-00000-0e83417c-cf59-4ba0-992f-7cb69e31c994-c000.snappy.parquet"

# Carrega os dados da camada Trusted
df = spark.read.parquet(arquivo_parquet_trusted)

# Explode os gêneros
dataframe_genero_explodido = df.withColumn("genero", explode("generos"))

# Adiciona IDs aos gêneros
dataframe_generos_com_id = dataframe_genero_explodido.withColumn("id_genero", dense_rank().over(Window.orderBy("genero")))

# Cria a dimensão de gênero/subgênero
dim_genero = dataframe_generos_com_id.select("id_genero", col("genero").alias("subgenero")).distinct()

# Cria a dimensão de filme
fato_filmes = df.select("id_filme", "titulo", "data_lancamento", "media_de_votos", "popularidade", "receita", "tempo_duracao(minutos)", "total_votos").distinct()

# Realiza o join entre o dataframe original e a dimensão de gênero para obter os IDs correspondentes
dim_genero = dataframe_generos_com_id.join(
    dim_genero,
    (dataframe_generos_com_id['genero'] == dim_genero['subgenero']) & (dataframe_generos_com_id['id_genero'] == dim_genero['id_genero']),
    'left_outer'
).select(
    dataframe_generos_com_id['id_filme'],
    "subgenero",
    "data_lancamento",
    "media_de_votos",
    "popularidade",
    "receita",
    "tempo_duracao(minutos)",
    "total_votos",
    "titulo"
)

# Adiciona coluna indicando se é um subgênero (excluindo "Crime")
dim_genero = dim_genero.withColumn("subgenero", when(col("subgenero") != "Crime", col("subgenero")).otherwise(None))

# Agrupa por filme e coleta os subgêneros
dim_subgenero = dim_genero.groupBy("id_filme").agg(
    collect_list("subgenero").alias("subgeneros")
)

# Extraindo até 4 subgêneros distintos para cada filme
for i in range(1, 5):
    dim_subgenero = dim_subgenero.withColumn("Subgenero_" + str(i), when(size("subgeneros") >= i, col("subgeneros")[i - 1]).otherwise(None))

# Inclui os dados dos subgêneros não relacionados a "Crime"
subgeneros_nao_crime = dataframe_genero_explodido.filter(col("genero") != "Crime")
subgeneros_nao_crime = subgeneros_nao_crime.select(
    "id_filme",
    col("genero").alias("subgenero"),  # Renomeia a coluna para subgenero
    "titulo",
    "data_lancamento",
    "media_de_votos",
    "popularidade",
    "receita",
    "tempo_duracao(minutos)",
    "total_votos"
)

# Caminho para a camada Refined
caminho_refined = "s3://data-lake-kesley/refined-zone/string/teste/2"

# Salva as dimensões e o fato no formato Parquet na camada Refined
dim_genero.write.parquet(caminho_refined + "dim_genero", mode="overwrite")
fato_filmes.write.parquet(caminho_refined + "fato_filmes", mode="overwrite")
dim_subgenero.write.parquet(caminho_refined + "dim_subgenero", mode="overwrite")
subgeneros_nao_crime.write.parquet(caminho_refined + "subgeneros_nao_crime", mode="overwrite")

# Salva as tabelas também em formato CSV
dim_genero.write.csv(caminho_refined + "dim_genero_CSV", header=True, mode="overwrite")
fato_filmes.write.csv(caminho_refined + "fato_filmes_CSV", header=True, mode="overwrite")
dim_subgenero.write.csv(caminho_refined + "dim_subgenero_CSV", header=True, mode="overwrite")
subgeneros_nao_crime.write.csv(caminho_refined + "subgeneros_nao_crime_CSV", header=True, mode="overwrite")

# Exibe os dados
dim_subgenero.show()
dim_genero.show()
fato_filmes.show()
subgeneros_nao_crime.show(truncate=False)































--------------------------------------------------------------------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, dense_rank, col, lit, when, collect_list, size
from pyspark.sql.window import Window

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("Filmes").getOrCreate()

# Carrega os dados da camada Trusted
arquivo_parquet_trusted = "s3://data-lake-kesley/trusted-zone/FilmesJSON/parquet/part-00000-0e83417c-cf59-4ba0-992f-7cb69e31c994-c000.snappy.parquet"
df = spark.read.parquet(arquivo_parquet_trusted)

# Explode os gêneros
dataframe_genero_explodido = df.withColumn("genero", explode("generos"))

# Adiciona IDs aos gêneros
dataframe_generos_com_id = dataframe_genero_explodido.withColumn("id_genero", dense_rank().over(Window.orderBy("genero")))

# Cria a dimensão de gênero/subgênero
dim_genero = dataframe_generos_com_id.select("id_genero", col("genero").alias("subgenero")).distinct()

# Cria a dimensão de filme
fato_filmes = df.select("id_filme", "titulo", "data_lancamento", "media_de_votos", "popularidade", "receita", "tempo_duracao(minutos)", "total_votos").distinct()

# Realiza o join entre o dataframe original e a dimensão de gênero para obter os IDs correspondentes
dim_genero = dataframe_generos_com_id.join(
    dim_genero,
    (dataframe_generos_com_id['genero'] == dim_genero['subgenero']) & (dataframe_generos_com_id['id_genero'] == dim_genero['id_genero']),
    'left_outer'
).select(
    dataframe_generos_com_id['id_filme'],
    "subgenero",
    "data_lancamento",
    "media_de_votos",
    "popularidade",
    "receita",
    "tempo_duracao(minutos)",
    "total_votos",
    "titulo"
)

# Adiciona coluna indicando se é um subgênero (excluindo "Crime")
dim_genero = dim_genero.withColumn("subgenero", when(col("subgenero") != "Crime", col("subgenero")).otherwise(None))

# Agrupa por filme e coleta os subgêneros
dim_subgenero = dim_genero.groupBy("id_filme").agg(
    collect_list("subgenero").alias("subgeneros")
)

# Extraindo até 4 subgêneros distintos para cada filme
for i in range(1, 5):
    dim_subgenero = dim_subgenero.withColumn("Subgenero_" + str(i), when(size("subgeneros") >= i, col("subgeneros")[i - 1]).otherwise(None))

# Seleciona as colunas desejadas
dim_subgenero = dim_subgenero.select("id_filme", "Subgenero_1", "Subgenero_2", "Subgenero_3", "Subgenero_4")

# Caminho para a camada Refined
caminho_refined = "s3://data-lake-kesley/refined-zone/string/teste/2"

# Salva as dimensões e o fato no formato Parquet na camada Refined
dim_genero.write.parquet(caminho_refined + "dim_genero", mode="overwrite")
fato_filmes.write.parquet(caminho_refined + "fato_filmes", mode="overwrite")
dim_subgenero.write.parquet(caminho_refined + "dim_genero", mode="overwrite")

# Salva as tabelas também em formato CSV
dim_genero.write.csv(caminho_refined + "dim_genero_CSV", header=True, mode="overwrite")
fato_filmes.write.csv(caminho_refined + "fato_filmes_CSV", header=True, mode="overwrite")
dim_subgenero.write.csv(caminho_refined + "dim_genero_CSV", header=True, mode="overwrite")

# Exibe os dados
dim_subgenero.show()
dim_genero.show()
fato_filmes.show()




