from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, dense_rank
from pyspark.sql.window import Window

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("Filmes").getOrCreate()

# Caminho para a camada Trusted
arquivo_parquet_trusted = "s3://data-lake-kesley/trusted-zone/FilmesJSON/parquet/part-00000-0e83417c-cf59-4ba0-992f-7cb69e31c994-c000.snappy.parquet"

# Carrega os dados da camada Trusted
df = spark.read.parquet(arquivo_parquet_trusted)

# Explode os gêneros
dataframe_genero_explodido = df.withColumn("genero", explode("generos"))

# Cria a janela de ordenação
window = Window.orderBy("genero")

# Adiciona IDs aos gêneros
dataframe_generos_com_id = dataframe_genero_explodido.withColumn("id_genero", dense_rank().over(window))

# Cria a dimensão de gênero/subgênero
dim_genero_subgenero = dataframe_generos_com_id.select("id_genero", "genero").distinct()

# Cria a dimensão de filme
dim_filme = df.select("id_filme", "data_lancamento", "media_de_votos", "popularidade", "receita", "tempo_duracao(minutos)", "total_votos").distinct()

# Realiza o join entre o dataframe original e a dimensão de gênero para obter os IDs correspondentes
fato_filmes_crime = dataframe_generos_com_id.join(
    dim_genero_subgenero.withColumnRenamed("id_genero", "id_genero_dim"), 
    dataframe_generos_com_id['genero'] == dim_genero_subgenero['genero'], 'left_outer'
).select(
    dataframe_generos_com_id['id_filme'],
    "id_genero_dim",
    "data_lancamento",
    "media_de_votos",
    "popularidade",
    "receita",
    "tempo_duracao(minutos)",
    "total_votos"
).distinct()

# Caminho para a camada Refined
caminho_refined = "s3://data-lake-kesley/refined-zone/"

# Salva as dimensões e o fato no formato Parquet na camada Refined
dim_genero_subgenero.write.parquet(caminho_refined + "Dim_Genero_Subgenero", mode="overwrite")
dim_filme.write.parquet(caminho_refined + "Dim_Filme", mode="overwrite")
fato_filmes_crime.write.parquet(caminho_refined + "Fato_Filmes_Crime", mode="overwrite")

# Salva as tabelas também em formato CSV
dim_genero_subgenero.write.csv(caminho_refined + "Dim_Genero_Subgenero_CSV", header=True, mode="overwrite")
dim_filme.write.csv(caminho_refined + "Dim_Filme_CSV", header=True, mode="overwrite")
fato_filmes_crime.write.csv(caminho_refined + "Fato_Filmes_Crime_CSV", header=True, mode="overwrite")

# Exibe os dados
fato_filmes_crime.show()
dim_genero_subgenero.show()
dim_filme.show()