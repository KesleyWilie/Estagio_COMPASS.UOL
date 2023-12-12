from pyspark.sql import SparkSession
import requests
import re

# Inicialização da sessão Spark
spark = SparkSession.builder.appName("Contagem de Palavras").getOrCreate()

# URL do arquivo README.md no meu repositório do GitHub
url = "https://raw.githubusercontent.com/KesleyWilie/Respositorio/main/Sprint7/EXERCÍCIOS/EXERCÍCIO%202/README.md?token=GHSAT0AAAAAACLMIIUV3EKG4BNXEXHYK5BAZLUAGVQ"

# Baixar conteúdo do arquivo usando requests diretamente no contêiner
response = requests.get(url)

# Verificação  se o download foi bem-sucedido
if response.status_code == 200:
    # Salvar o conteúdo do arquivo no diretório /home/jovyan/work/ dentro do contêiner
    with open("/home/jovyan/work/README.md", "w") as file:
        file.write(response.text)

    # Função para dividir as palavras
    def split_words(line):
        # Use expressão regular para extrair palavras
        return re.findall(r'\b\w+\b', line)

    # Divida as linhas em palavras usando a função
    words = [word for line in response.text.splitlines() for word in split_words(line)]

    # Convertendo as palavras em RDD
    text_rdd = spark.sparkContext.parallelize(words)

    # Contar as ocorrências de cada palavra
    word_counts = text_rdd.countByValue()

    # Exibindo a contagem de palavras
    for word, count in word_counts.items():
        print(f"{word}: {count}")
else:
    print(f"Erro ao baixar o arquivo. Código de status: {response.status_code}")

# Encerrando a sessão Spark
spark.stop()