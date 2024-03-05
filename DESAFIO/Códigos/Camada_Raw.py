# Variáveis para armazenar informações sensíveis
api_key = "sua_chave_de_api_aqui"
nome_documento = "filmes_criminosos.json"

# Código para obtenção de dados da API
api_url = f"http://api.example.com/top_filmes_crime?api_key={api_key}&document={nome_documento}"
dados_api = obter_dados_da_api(api_url)



# Código para obter os dados da API
# Substitua 'sua_chave_de_api' pela sua chave real da API
# Certifique-se de ter instalado a biblioteca 'requests' antes de executar este código

import requests
import json
from pyspark.sql import SparkSession

# Sua chave de API TMDB
api_key = 'sua_chave_de_api'

# Endpoint da API TMDB para os top filmes de crime (exemplo)
endpoint = f'https://api.themoviedb.org/3/discover/movie?api_key={api_key}&with_genres=80&sort_by=popularity.desc'

# Obtendo dados da API
response = requests.get(endpoint)
data = response.json()

# Transformando os dados em um DataFrame Spark
spark = SparkSession.builder.appName("Filmes").getOrCreate()
df_api = spark.createDataFrame([json.dumps(data)])

# Exibindo os dados obtidos
df_api.show(truncate=False)









from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Variáveis para armazenar informações sensíveis
api_key = "sua_chave_de_api_aqui"
nome_documento = "filmes_criminosos.json"

# Código para obtenção de dados da API
api_url = f"http://api.example.com/top_filmes_crime?api_key={api_key}&document={nome_documento}"
dados_api = obter_dados_da_api(api_url)


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



