from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("Filmes").getOrCreate()

# Lista dos anos para os quais você tem arquivos JSON
anos = list(range(2000, 2024))

# Caminho de origem dos arquivos JSON (substitua pelo seu caminho real)
caminho_origem_json = "s3://data-lake-kesley/Raw/tmdb/json/Topfilmes/2000-2023/2024-01-07/"

# Lista para armazenar os DataFrames lidos de cada ano
dataframes_por_ano = []

# Leitura dos arquivos JSON e armazenamento em uma lista de DataFrames
for ano in anos:
    nome_arquivo = f"filmes({ano})_crime_aclamados(1).json"
    df = spark.read.option("multiline", "true").json(f"{caminho_origem_json}{nome_arquivo}")
    dataframes_por_ano.append(df)

# Concatenação de todos os DataFrames em um único DataFrame
df_final = dataframes_por_ano[0]
for df in dataframes_por_ano[1:]:
    df_final = df_final.union(df)

# Remoção de linhas com valores nulos em 'id_imdb', 'receita', 'total_votos' e 'media_de_votos'
colunas_para_verificar_nulos = ['id_imdb', 'receita', 'total_votos', 'media_de_votos']
df_sem_nulos = df_final.filter(
    (col("id_imdb") != "null") & (col("receita").isNotNull()) & (col("total_votos") != 0) & (col("media_de_votos").isNotNull())
)

# Caminho de destino no Amazon S3
caminho_destino = "s3://data-lake-kesley/trusted-zone/FilmesJSON/parquet/"

# Escreve o DataFrame em formato Parquet no caminho de destino
df_sem_nulos.coalesce(1).write.parquet(caminho_destino, mode="overwrite")

# Exibe os resultados
print("Total de registros após a filtragem:", df_sem_nulos.count())
df_sem_nulos.show()