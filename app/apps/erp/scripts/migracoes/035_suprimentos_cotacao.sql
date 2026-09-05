-- 035 — Suprimentos, fase 3: cotação, mapa e banco de preços
--
-- O mapa é o coração do processo: fornecedores em coluna, insumos em linha,
-- preço na célula. Aqui ele deixa de ser planilha e vira tabela — o que tira
-- de uma vez o limite de 50 insumos x 10 fornecedores que a planilha impunha.
--
-- O BANCO DE PREÇOS (precos_historico) nasce junto e não por acaso: todo preço
-- que entra num mapa e todo preço que vira pedido ficam guardados com a data,
-- o fornecedor e a origem. É o que responde "este preço está bom?" na hora de
-- fechar, e é o que permite herdar preço de uma cotação anterior.

DO $$ BEGIN
    CREATE TYPE status_cotacao AS ENUM ('ABERTA', 'FECHADA', 'CANCELADA');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE modo_entrega AS ENUM ('ENTREGA', 'COLETA');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE origem_preco AS ENUM ('DIGITADO', 'IA', 'HERDADO');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE tipo_preco AS ENUM ('COTADO', 'COMPRADO');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS cotacoes (
    id           BIGSERIAL PRIMARY KEY,
    numero       TEXT NOT NULL UNIQUE,
    titulo       TEXT NOT NULL,
    status       status_cotacao NOT NULL DEFAULT 'ABERTA',
    observacoes  TEXT,
    criado_por   BIGINT NOT NULL REFERENCES usuarios(id),
    criado_em    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Os itens cotados. Um item da solicitação entra em UMA cotação aberta por
-- vez: cotar o mesmo item duas vezes ao mesmo tempo é pedir dois preços para
-- a mesma coisa e não saber qual vale.
CREATE TABLE IF NOT EXISTS cotacao_itens (
    id                  BIGSERIAL PRIMARY KEY,
    cotacao_id          BIGINT NOT NULL REFERENCES cotacoes(id) ON DELETE CASCADE,
    suprimento_item_id  BIGINT NOT NULL REFERENCES suprimento_itens(id),
    numero              INT    NOT NULL,
    UNIQUE (cotacao_id, suprimento_item_id),
    UNIQUE (cotacao_id, numero)
);

-- A coluna do fornecedor no mapa: forma de pagamento, entrega, frete e
-- desconto. Tudo isso muda o preço final e por isso vive junto do preço.
CREATE TABLE IF NOT EXISTS cotacao_fornecedores (
    id                    BIGSERIAL PRIMARY KEY,
    cotacao_id            BIGINT NOT NULL REFERENCES cotacoes(id) ON DELETE CASCADE,
    fornecedor_id         BIGINT NOT NULL REFERENCES fornecedores(id),
    contato_id            BIGINT REFERENCES fornecedor_contatos(id),
    condicao_pagamento_id BIGINT REFERENCES condicoes_pagamento(id),
    entrega               modo_entrega,
    frete                 NUMERIC(14,2) NOT NULL DEFAULT 0,
    desconto              NUMERIC(14,2) NOT NULL DEFAULT 0,
    acrescimo_percentual  NUMERIC(6,3)  NOT NULL DEFAULT 0,
    respondido_em         TIMESTAMPTZ,
    respondido_por        TEXT,
    anexo_id              BIGINT REFERENCES anexos(id),
    ordem                 INT NOT NULL DEFAULT 0,
    UNIQUE (cotacao_id, fornecedor_id),
    CONSTRAINT ck_cotacao_forn_frete CHECK (frete >= 0 AND desconto >= 0)
);

CREATE TABLE IF NOT EXISTS cotacao_precos (
    id                     BIGSERIAL PRIMARY KEY,
    cotacao_fornecedor_id  BIGINT NOT NULL REFERENCES cotacao_fornecedores(id)
                                  ON DELETE CASCADE,
    cotacao_item_id        BIGINT NOT NULL REFERENCES cotacao_itens(id)
                                  ON DELETE CASCADE,
    preco_unitario         NUMERIC(14,4) NOT NULL,
    observacao             TEXT,
    origem                 origem_preco NOT NULL DEFAULT 'DIGITADO',
    herdado_de_cotacao_id  BIGINT REFERENCES cotacoes(id),
    registrado_em          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (cotacao_fornecedor_id, cotacao_item_id),
    CONSTRAINT ck_preco_positivo CHECK (preco_unitario > 0)
);

-- ---------------------------------------------------------------------------
-- BANCO DE PREÇOS
--
-- Preço COTADO é o que o fornecedor ofereceu; COMPRADO é o que a empresa
-- aceitou pagar. Guardar os dois separados importa: o comprado vale mais na
-- hora de julgar se um preço novo está bom.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS precos_historico (
    id                    BIGSERIAL PRIMARY KEY,
    insumo_id             BIGINT NOT NULL REFERENCES insumos(id),
    especificacao         TEXT,
    unidade               TEXT REFERENCES unidades_compra(codigo),
    preco_unitario        NUMERIC(14,4) NOT NULL,
    quantidade            NUMERIC(14,3),
    fornecedor_id         BIGINT REFERENCES fornecedores(id),
    obra_id               BIGINT REFERENCES obras(id),
    condicao_pagamento_id BIGINT REFERENCES condicoes_pagamento(id),
    tipo                  tipo_preco NOT NULL,
    cotacao_id            BIGINT REFERENCES cotacoes(id),
    data                  DATE NOT NULL DEFAULT CURRENT_DATE,
    criado_em             TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_preco_hist_positivo CHECK (preco_unitario > 0)
);

CREATE INDEX IF NOT EXISTS ix_precos_hist_insumo_data
    ON precos_historico (insumo_id, data DESC);
CREATE INDEX IF NOT EXISTS ix_precos_hist_fornecedor
    ON precos_historico (fornecedor_id, data DESC);

CREATE SEQUENCE IF NOT EXISTS seq_cotacao START 1;
