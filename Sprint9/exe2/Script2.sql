-- Criar a view para a tabela de fatos (Locacao)
CREATE VIEW fatoLocacao AS
SELECT
    l.idLocacao,
    l.idCarro,
    l.qtdDiaria,
    l.vlrDiaria,
    l.dataLocacao,
    l.dataEntrega,
    c.modeloCarro,
    c.marcaCarro,
    v.nomeVendedor,
    cl.nomeCliente,
    cl.estadoCliente
FROM Locacao l
JOIN Carro c ON l.idCarro = c.idCarro
JOIN Vendedor v ON l.idVendedor = v.idVendedor
JOIN Cliente cl ON l.idCliente = cl.idCliente;

-- Criar a view para a tabela dimensional
CREATE VIEW dimDesempenhoVendas AS
SELECT
    idCarro,
    modeloCarro,
    marcaCarro,
    SUM(vlrDiaria) AS faturamentoTotal
FROM fatoLocacao
GROUP BY
    idCarro,
    modeloCarro,
    marcaCarro
ORDER BY faturamentoTotal DESC;


-- LISTAR CARROS POR FATURAMENTO
SELECT *
FROM dimDesempenhoVendas;