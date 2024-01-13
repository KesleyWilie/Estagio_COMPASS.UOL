## Camada Refined

A camada Refined neste projeto representa a etapa de refinamento dos dados, onde os dados brutos da camada Trusted são processados, transformados e armazenados de maneira otimizada para análises mais avançadas. Aqui estão os principais elementos da camada Refined:

### Processo de Refinamento com Apache Spark

O Apache Spark é utilizado para processar os dados da camada Trusted, transformando-os conforme as necessidades do modelo dimensional definido. Durante esse processo, os dados são persistidos no formato Parquet para otimizar o desempenho e possibilitar consultas mais eficientes.

### Estrutura de Armazenamento

Os dados refinados são armazenados na camada Refined, organizados de acordo com o modelo dimensional estabelecido. Cada tabela é estruturada de forma a suportar análises específicas, oferecendo eficiência na recuperação de informações.

### Particionamento e AWS Glue Data Catalog

Quando necessário, os dados são particionados para melhorar a eficiência nas consultas. Além disso, as tabelas geradas durante o processo de refinamento são registradas no AWS Glue Data Catalog. Isso permite que as tabelas sejam facilmente acessadas e consultadas em serviços como o Amazon Athena, simplificando a análise de dados.

### Exemplo de Código de Refinamento

Aqui está um exemplo de código que demonstra o uso do Apache Spark para o refinamento dos dados e persistência no formato Parquet:

```python
# Código Spark para refinamento dos dados
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RefinementJob").getOrCreate()

# Carregar dados da camada Trusted
df_trusted = spark.read.parquet("s3://data-lake-kesley/trusted-zone/FilmesJSON/parquet/")

# Lógica de transformação e processamento dos dados
# ...

# Persistir dados refinados no formato Parquet
df_refined.write.parquet("s3://data-lake-kesley/refined-zone/Filmes/")

# Registrar tabelas no AWS Glue Data Catalog
spark.catalog.createTable("refined_filmes", "s3://data-lake-kesley/refined-zone/Filmes/")
