import pyarrow.parquet as pq

# Caminho do arquivo Parquet
file_path = 'SPRINT 9\exe3\part-00000-0e83417c-cf59-4ba0-992f-7cb69e31c994-c000.snappy.parquet'

# Lê o arquivo Parquet
table = pq.read_table(file_path)

# Converte a tabela para um DataFrame do Pandas
df = table.to_pandas()

# Exibindo os primeiros registros do DataFrame
print(df.count())
print(df.columns)
print(df)
