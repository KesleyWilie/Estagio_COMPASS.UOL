# Ler o arquivo actors.csv e retornar os dados como uma lista de linhas
def ler_arquivo(arquivo):
    with open(arquivo, 'r') as file:
        return file.readlines()

# Converter um valor float em int, arredondando-o corretamente
def converter_para_int(valor):
    try:
        return int(float(valor))
    except ValueError:
        return 0  # Retorna 0 em caso de erro

# Converter um valor em float, ou retornar 0 se não for possível
def converter_para_float(valor):
    try:
        return float(valor)
    except ValueError:
        return 0.0 

# Escrever resultados em um arquivo .txt
def escrever_resultado_em_arquivo(arquivo_saida, resultado):
    with open(arquivo_saida, 'w') as file:
        file.write(resultado)

# Leitura de dados
dados_csv = ler_arquivo('actors.csv')
#tratamendo de dados para o caso do Robert Downey Jr.

dados_csv[5] = dados_csv[5].replace('"','').replace(',','',1)
# Tratamento de dados para casos gerais como o do Robert Downey Jr.
# Irá remover as aspas e substituir apenas a primeira vírgula dentro das aspas duplas

'''for i in range(len(dados_csv)):
    if '"' in dados_csv[i]:
        partes = dados_csv[i].split('"')
        partes[1] = partes[1].replace(',', '')
        linha_corrigida = '"'.join(partes) 
        dados_csv[i] = linha_corrigida.replace('"', '')
'''

# 1: Encontrar o ator/atriz com o maior número de filmes
maior_numero_filmes = 0
ator_com_maior_numero_filmes = ''

for linha in dados_csv[1:]:
    colunas = linha.strip().split(',')
    num_de_filmes = converter_para_int(colunas[2])
    if num_de_filmes > maior_numero_filmes:
        maior_numero_filmes = num_de_filmes
        ator_com_maior_numero_filmes = colunas[0]

resultado_etapa_1 = f"1 - {ator_com_maior_numero_filmes} possui o maior número de filmes: {maior_numero_filmes}"

# 2: Calcular a média de receita de bilheteria bruta dos principais filmes (coluna Gross)
total_receita_bruta = 0
contador_filmes = 0

for linha in dados_csv[1:]:
    colunas = linha.strip().split(',')
    receita_bruta = converter_para_float(colunas[5])
    total_receita_bruta += receita_bruta
    contador_filmes += 1

media_receita = total_receita_bruta / contador_filmes

resultado_etapa_2 = f"2 - Média de receita de bilheteria bruta dos principais filmes: {media_receita:.2f} milhões de dólares"

# 3: Encontrar o ator/atriz com a maior média de receita de bilheteria bruta por filme
maior_media_receita_por_filme = 0
ator_com_maior_media_receita_por_filme = ''

for linha in dados_csv[1:]:
    colunas = linha.strip().split(',')
    media_receita_por_filme = converter_para_float(colunas[3])
    if media_receita_por_filme > maior_media_receita_por_filme:
        maior_media_receita_por_filme = media_receita_por_filme
        ator_com_maior_media_receita_por_filme = colunas[0]

resultado_etapa_3 = f"3 - {ator_com_maior_media_receita_por_filme} possui a maior média de receita de bilheteria bruta por filme: {maior_media_receita_por_filme:.2f} milhões de dólares por filme"

# 4: Contagem de aparições dos filmes de maior bilheteria (coluna #1 Movie)
filmes = {}

for linha in dados_csv[1:]:
    colunas = linha.strip().split(',')
    filme = colunas[4]
    if filme in filmes:
        filmes[filme] += 1
    else:
        filmes[filme] = 1

filmes_ordenados = sorted(filmes.items(), key=lambda x: (-x[1], x[0]))

resultado_etapa_4 = "4 - Filmes de maior bilheteria no dataset:\n"

for filme, quantidade in filmes_ordenados:
    resultado_etapa_4 += f"{filme} - Aparece {quantidade} vez(es) no dataset\n"

# 5: Lista de atores ordenada pela receita bruta de bilheteria de seus filmes (coluna Total Gross)
atores = {}

for linha in dados_csv[1:]:
    colunas = linha.strip().split(',')
    ator = colunas[0]
    receita_bruta = converter_para_float(colunas[1])
    if ator in atores:
        atores[ator] += receita_bruta
    else:
        atores[ator] = receita_bruta

atores_ordenados = sorted(atores.items(), key=lambda x: (-x[1], x[0]))

resultado_etapa_5 = "5 - Atores ordenados pela receita bruta de bilheteria de seus filmes:\n"

for ator, receita_bruta in atores_ordenados:
    resultado_etapa_5 += f"{ator} - {receita_bruta:.2f} milhões de dólares\n"

# Escrever os resultados em arquivos de texto
escrever_resultado_em_arquivo('etapa-1.txt', resultado_etapa_1)
escrever_resultado_em_arquivo('etapa-2.txt', resultado_etapa_2)
escrever_resultado_em_arquivo('etapa-3.txt', resultado_etapa_3)
escrever_resultado_em_arquivo('etapa-4.txt', resultado_etapa_4)
escrever_resultado_em_arquivo('etapa-5.txt', resultado_etapa_5)

# Imprimir os resultados no console
print(resultado_etapa_1)
print(resultado_etapa_2)
print(resultado_etapa_3)
print(resultado_etapa_4)
print(resultado_etapa_5)