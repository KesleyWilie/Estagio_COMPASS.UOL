-- Criação da tabela Vendedor
CREATE TABLE IF NOT EXISTS Vendedor (
    idVendedor INT PRIMARY KEY,
    nomeVendedor VARCHAR(15),
    sexoVendedor SMALLINT,
    estadoVendedor VARCHAR(40)
);

-- Criação da tabela Cliente
CREATE TABLE IF NOT EXISTS Cliente (
    idCliente INT PRIMARY KEY,
    nomeCliente VARCHAR(100),
    cidadeCliente VARCHAR(100),
    estadoCliente VARCHAR(40),
    paisCliente VARCHAR(40)
);

-- Criação da tabela Carro
CREATE TABLE IF NOT EXISTS Carro (
    idCarro INT PRIMARY KEY,
    modeloCarro VARCHAR(80),
    marcaCarro VARCHAR(80),
    anoCarro INT,
    kmCarro INT,
    idCombustivel INT, 
    classiCarro VARCHAR(50),
    FOREIGN KEY (idCombustivel) REFERENCES Combustivel(idCombustivel)
);

-- Criação da tabela Combustivel
CREATE TABLE IF NOT EXISTS Combustivel (
    idCombustivel INT PRIMARY KEY,
    tipoCombustivel VARCHAR(20)
);

-- Criação da tabela Locacao
CREATE TABLE IF NOT EXISTS Locacao (
    idLocacao INT PRIMARY KEY,
    idCliente INT,
    idCarro INT,
    dataLocacao DATETIME,
    horaLocacao TIME,
    qtdDiaria INT,
    vlrDiaria DECIMAL(18,2),
    dataEntrega DATE,
    horaEntrega TIME,
    idVendedor INT,
    FOREIGN KEY (idCliente) REFERENCES Cliente(idCliente),
    FOREIGN KEY (idCarro) REFERENCES Carro(idCarro),
    FOREIGN KEY (idVendedor) REFERENCES Vendedor(idVendedor)
);

-- Inserir dados na tabela Vendedor (evitar violação de chave primária)
INSERT OR IGNORE INTO Vendedor (idVendedor, nomeVendedor, sexoVendedor, estadoVendedor)
SELECT DISTINCT idVendedor, nomeVendedor, sexoVendedor, estadoVendedor
FROM tb_locacao;

-- Inserir dados na tabela Cliente (evitar violação de chave primária)
INSERT OR IGNORE INTO Cliente (idCliente, nomeCliente, cidadeCliente, estadoCliente, paisCliente)
SELECT DISTINCT idCliente, nomeCliente, cidadeCliente, estadoCliente, paisCliente
FROM tb_locacao;

-- Inserir dados na tabela Carro (evitar violação de chave primária)
INSERT OR IGNORE INTO Carro (idCarro, modeloCarro, marcaCarro, anoCarro, kmCarro, idCombustivel, classiCarro)
SELECT DISTINCT idCarro, modeloCarro, marcaCarro, anoCarro, kmCarro, idCombustivel, classiCarro
FROM tb_locacao;

-- Inserir dados na tabela Combustivel (evitar violação de chave primária e adicionar tipoCombustivel)
INSERT OR IGNORE INTO Combustivel (idCombustivel, tipoCombustivel)
SELECT DISTINCT idCombustivel, tipoCombustivel
FROM tb_locacao;


-- Inserir dados na tabela Locacao
INSERT OR IGNORE INTO Locacao (idLocacao, idCliente, idCarro, dataLocacao, horaLocacao, qtdDiaria, vlrDiaria, dataEntrega, horaEntrega, idVendedor)
SELECT idLocacao, idCliente, idCarro, dataLocacao, horaLocacao, qtdDiaria, vlrDiaria, dataEntrega, horaEntrega, idVendedor
FROM tb_locacao;

-- Visualizar dados nas tabelas
SELECT * FROM Locacao;
SELECT * FROM Vendedor;
SELECT * FROM Cliente;
SELECT * FROM Carro;
SELECT * FROM Combustivel;
SELECT * FROM tb_locacao;
