## Modelo Dimensional

O modelo dimensional adotado neste projeto visa proporcionar uma estrutura eficiente para análise de dados relacionados a filmes. Ele é composto por três principais entidades:

### Dimensão de Filmes (Dim_Filme)

- `id_filme`: Chave Primária.
- `data_lancamento`: Data de lançamento do filme.
- `media_de_votos`: Média de votos atribuídos ao filme.
- `popularidade`: Popularidade do filme.
- `receita`: Receita gerada pelo filme.
- `tempo_duracao(minutos)`: Tempo de duração do filme em minutos.
- `total_votos`: Total de votos recebidos pelo filme.

Essa dimensão permite uma análise aprofundada dos atributos específicos de cada filme, fornecendo uma visão detalhada do desempenho individual.

### Dimensão de Subgêneros (Dim_Subgenero)

- `id_genero`: Chave Primária.
- `genero`: Nome do subgênero.

Esta dimensão representa os diferentes subgêneros associados aos filmes, proporcionando uma maneira de categorizar e analisar filmes com base em suas características específicas de gênero.

### Tabela de Relacionamento entre Filmes e Subgêneros (Rel_Filmes_Subgeneros)

- `id_filme`: Chave Estrangeira para `Dim_Filme`.
- `id_genero`: Chave Estrangeira para `Dim_Subgenero`.

Esta tabela de relacionamento estabelece a conexão entre os filmes e seus subgêneros correspondentes, permitindo análises que exploram a distribuição de filmes em diferentes categorias de subgêneros.

O uso de um modelo dimensional facilita consultas analíticas complexas, proporcionando uma estrutura organizada e eficiente para explorar informações sobre filmes, subgêneros e seu desempenho ao longo do tempo.
