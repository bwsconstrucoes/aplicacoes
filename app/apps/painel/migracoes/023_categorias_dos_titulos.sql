-- 023 — As categorias de cada título, quando ele tem mais de uma
--
-- 09/10/2026, o dono: títulos de empréstimo divididos entre DUAS categorias —
-- uma parte é a devolução do principal (fluxo), a outra é o juro (DRE) — "não
-- estão aparecendo nada, nem a porção de juros nem a do principal".
--
-- O painel guardava UMA categoria por título: com mais de uma, ficava com a de
-- maior valor e jogava o título inteiro nela. A parte dos juros nunca chegava
-- ao DRE. Esta tabela guarda a lista inteira, como o OMIE manda, para o fato
-- dividir o título entre as categorias na proporção dele.
--
-- Só título com DUAS ou mais categorias tem linhas aqui; o resto continua com a
-- categoria única da tabela `titulos`. O código tolera a tabela ausente.

CREATE TABLE IF NOT EXISTS painel.titulo_categorias (
    codigo_lancamento_omie  BIGINT  NOT NULL,
    seq                     INTEGER NOT NULL,
    codigo_categoria        TEXT    NOT NULL,
    percentual              NUMERIC,
    valor                   NUMERIC,
    PRIMARY KEY (codigo_lancamento_omie, seq)
);
