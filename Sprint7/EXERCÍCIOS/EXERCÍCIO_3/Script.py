import sys
from pyspark.sql.functions import col
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])

# Inicializo as configurações do Spark e do Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Defino os caminhos dos arquivos de entrada e saída
source_file = args['S3_INPUT_PATH']
target_path = args['S3_TARGET_PATH']

# Carrego os dados do CSV
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    "s3",
    {"paths": [source_file]},
    "csv",
    {"withHeader": True, "separator": ","},
)

# Ordene os dados pelo ano de forma decrescente
dynamic_frame.toDF().sort(col('ano').desc())

# Imprimo o schema do dataframe gerado
dynamic_frame.printSchema()

# Altero a caixa dos valores da coluna nome para MAIÚSCULO
dynamic_frame = dynamic_frame.apply_mapping([
    ("nome", "string", "NOME", "string"),
    ("outras_colunas", "string", "outras_colunas", "string"),
    ("ano", "int", "ano", "int"),
    ("sexo", "string", "sexo", "string"),
    ("total", "int", "total", "int")  # Adicione esta linha para incluir a coluna 'total'
])

# Imprimo a contagem de linhas presentes no dataframe
print("Contagem de linhas:", dynamic_frame.count())

# Imprimo a contagem de nomes, agrupando os dados pelas colunas ano e sexo
count_by_ano_sexo = dynamic_frame.toDF().groupBy('ano', 'sexo').agg({'total': 'sum'}).sort('ano', ascending=False)
count_by_ano_sexo.show(10, truncate=False)

#df_count = dynamic_frame.fromDF(df.toDF().groupBy("ano", "sexo").count().orderBy("ano", ascending=False),glueContext,)
#df_count.toDF().show()

# Apresento qual foi o nome feminino com mais registros e em que ano ocorreu
max_female_name = dynamic_frame.toDF().filter(col('sexo') == 'F').orderBy('total', ascending=False).first()
print("Nome feminino com mais registros:", max_female_name["NOME"], "em", max_female_name["ano"])

# Apresento qual foi o nome masculino com mais registros e em que ano ocorreu
max_male_name = dynamic_frame.toDF().filter(col('sexo') == 'M').orderBy('total', ascending=False).first()
print("Nome masculino com mais registros:", max_male_name["NOME"], "em", max_male_name["ano"])

# Escrevo o conteúdo do dataframe com os valores de nome em maiúsculo no S3
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={"path": f"s3://kesley/lab-glue/frequencia_registro_nomes_eua/", "partitionKeys": ["sexo", "ano"]},
    format="json",
    transformation_ctx="output"
)

# Finalizo o job
job.commit()