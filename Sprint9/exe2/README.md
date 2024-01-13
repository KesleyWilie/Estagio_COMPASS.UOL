# Modelagem Dimensional - Concessionária

Este projeto envolve a criação de um modelo dimensional para uma concessionária com base em um modelo relacional normalizado. A modelagem dimensional é uma técnica que visa simplificar e otimizar a análise de dados.

## Estrutura do Banco de Dados

O banco de dados é composto por tabelas relacionais normalizadas, incluindo:

- Tabela Vendedor
- Tabela Cliente
- Tabela Carro
- Tabela Combustível
- Tabela Locação

## Criação de Tabelas Fato e Dimensão

### Tabela de Fatos (LocacaoFato)

- `idLocacao` (Chave Primária)
- `idCarro` (Chave Estrangeira)
- `idCliente` (Chave Estrangeira)
- `idVendedor` (Chave Estrangeira)
- `dataLocacao`
- `horaLocacao`
- `qtdDiaria`
- `vlrDiaria`
- `dataEntrega`
- `horaEntrega`

### Tabela Dimensional (DesempenhoVendasDimensao)

- `idCarro` (Chave Primária)
- `modeloCarro`
- `marcaCarro`
- `idCliente` (Chave Estrangeira)
- `nomeCliente`
- `estadoCliente`
- `idVendedor` (Chave Estrangeira)
- `nomeVendedor`

## Criação de Views

Foram criadas duas views para facilitar a análise dimensional:

- **fatoLocacao**: Contém detalhes sobre cada locação, com informações de carro, cliente e vendedor.

- **dimDesempenhoVendas**: Resumo do faturamento total por carro, cliente e vendedor.

![imagem](exe2/imagem-exe2.jpeg)
![imagem](exe2/imagem-exe2-resultado.jpeg)
