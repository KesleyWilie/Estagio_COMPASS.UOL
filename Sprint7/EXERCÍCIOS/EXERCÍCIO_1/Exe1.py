import pandas as pd

# Carregar o arquivo CSV
df = pd.read_csv('actors.csv')

# Identificar o ator/atriz com o maior número de filmes
actor_max_movies = df.loc[df['Number of Movies'].idxmax()]['Actor']
num_max_movies = df['Number of Movies'].max()
print(f"1. Ator/atriza com maior número de filmes: {actor_max_movies} ({num_max_movies} filmes)\n")

# Calcular a média da coluna de número de filmes
average_movies = df['Number of Movies'].mean()
print(f"2. Média de filmes por ator/atriz: {average_movies:.2f}\n")

# Identificar o ator/atriz com a maior média por filme
df['Average per Movie'] = df['Total Gross'] / df['Number of Movies']
actor_max_average = df.loc[df['Average per Movie'].idxmax()]['Actor']
max_average = df['Average per Movie'].max()
print(f"3. Ator/atriza com maior média por filme: {actor_max_average} ({max_average:.2f} por filme)\n")

# Identificar o(s) filme(s) mais frequente(s) e sua respectiva frequência
top_movies = df[df['Number of Movies'] == df['Number of Movies'].max()]['#1 Movie'].tolist()
top_movies_frequency = df['#1 Movie'].value_counts().max()
print(f"4. Filme(s) mais frequente(s): {', '.join(top_movies)} ({top_movies_frequency} vezes)\n")