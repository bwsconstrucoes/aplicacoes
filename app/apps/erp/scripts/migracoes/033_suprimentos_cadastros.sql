-- 033 — Suprimentos, fase 1: os cadastros que o resto do módulo usa
--
-- Nada aqui é tela ainda: é a base sobre a qual solicitação, cotação, mapa e
-- pedido vão se apoiar. A especificação está em app/apps/erp/SUPRIMENTOS.md.

-- ---------------------------------------------------------------------------
-- Unidades de compra. Vieram da planilha em uso, com o nome por extenso porque
-- "VR" não diz nada para quem está aprendendo o sistema.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS unidades_compra (
    codigo    TEXT PRIMARY KEY,
    descricao TEXT    NOT NULL,
    ordem     INT     NOT NULL DEFAULT 0,
    ativo     BOOLEAN NOT NULL DEFAULT TRUE
);

INSERT INTO unidades_compra (codigo, descricao, ordem) VALUES
    ('UN',  'Unidade',        1),
    ('M',   'Metro',          2),
    ('M2',  'Metro quadrado', 3),
    ('M3',  'Metro cúbico',   4),
    ('KG',  'Quilo',          5),
    ('T',   'Tonelada',       6),
    ('L',   'Litro',          7),
    ('SC',  'Saco',           8),
    ('VR',  'Vara',           9),
    ('LT',  'Lata',          10),
    ('BD',  'Balde',         11),
    ('GL',  'Galão',         12),
    ('PCT', 'Pacote',        13),
    ('CX',  'Caixa',         14),
    ('PL',  'Pallet',        15),
    ('CAR', 'Carrada',       16)
ON CONFLICT (codigo) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Condições de pagamento como REGRA, não como lista.
--
-- A planilha tinha 121 arranjos escritos à mão ("30/60/90 dias", "30% + 28/56
-- dias", "6x parcelas"). Todos cabem em duas informações: quanto entra na hora
-- (em %) e em quantos dias vencem as demais parcelas. Guardando assim, o
-- sistema gera as parcelas do título sozinho, e um arranjo novo é uma linha —
-- não código.
--
--   À vista .............. entrada_percentual = 100, dias = {}
--   30 dias .............. entrada 0, dias = {30}
--   30/60/90 ............. entrada 0, dias = {30,60,90}
--   30% + 28/56 .......... entrada 30, dias = {28,56}
--   6x parcelas .......... entrada 0, dias = {30,60,90,120,150,180}
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS condicoes_pagamento (
    id                 BIGSERIAL PRIMARY KEY,
    nome               TEXT    NOT NULL UNIQUE,
    entrada_percentual NUMERIC(5,2) NOT NULL DEFAULT 0,
    dias               INT[]   NOT NULL DEFAULT '{}',
    ordem              INT     NOT NULL DEFAULT 0,
    ativo              BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_condicao_entrada CHECK (entrada_percentual >= 0
                                      AND entrada_percentual <= 100),
    -- Sem entrada e sem prazo não é condição nenhuma: seria um título sem
    -- vencimento. Fecha aqui, e não no meio de um lançamento.
    CONSTRAINT ck_condicao_tem_algo CHECK (entrada_percentual > 0
                                       OR array_length(dias, 1) >= 1)
);

INSERT INTO condicoes_pagamento (nome, entrada_percentual, dias, ordem) VALUES
    ('À vista',           100, '{}',            1),
    ('7 dias',              0, '{7}',          10),
    ('14 dias',             0, '{14}',         11),
    ('21 dias',             0, '{21}',         12),
    ('28 dias',             0, '{28}',         13),
    ('30 dias',             0, '{30}',         14),
    ('45 dias',             0, '{45}',         15),
    ('7/14 dias',           0, '{7,14}',       20),
    ('7/14/21 dias',        0, '{7,14,21}',    21),
    ('15/30 dias',          0, '{15,30}',      22),
    ('28/42 dias',          0, '{28,42}',      23),
    ('28/42/56 dias',       0, '{28,42,56}',   24),
    ('30/60 dias',          0, '{30,60}',      25),
    ('30/60/90 dias',       0, '{30,60,90}',   26),
    ('30/60/90/120 dias',   0, '{30,60,90,120}', 27),
    ('30% + 30 dias',      30, '{30}',         40),
    ('30% + 30/60 dias',   30, '{30,60}',      41),
    ('50% + 30 dias',      50, '{30}',         42),
    ('50% + 30/60/90 dias',50, '{30,60,90}',   43),
    ('2x parcelas',         0, '{30,60}',      60),
    ('3x parcelas',         0, '{30,60,90}',   61),
    ('4x parcelas',         0, '{30,60,90,120}', 62),
    ('6x parcelas',         0, '{30,60,90,120,150,180}', 63)
ON CONFLICT (nome) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Fornecedor: o que muda a forma de comprar
--
-- Região e canal são LISTAS porque na planilha já são ("CE, RMF"; e-mail e
-- WhatsApp ao mesmo tempo). Guardar como texto único obrigaria a inventar
-- separador e a fazer LIKE na hora de filtrar.
-- ---------------------------------------------------------------------------
DO $$ BEGIN
    CREATE TYPE fornecedor_porte AS ENUM (
        'FABRICA', 'REP_FABRICA', 'DISTRIBUIDOR', 'LOCAL', 'HOMECENTER');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

ALTER TABLE fornecedores ADD COLUMN IF NOT EXISTS porte fornecedor_porte;
ALTER TABLE fornecedores ADD COLUMN IF NOT EXISTS regioes_atuacao TEXT[]
    NOT NULL DEFAULT '{}';
ALTER TABLE fornecedores ADD COLUMN IF NOT EXISTS canais_cotacao TEXT[]
    NOT NULL DEFAULT '{EMAIL}';

CREATE INDEX IF NOT EXISTS ix_fornecedores_regioes
    ON fornecedores USING GIN (regioes_atuacao);

-- O que cada fornecedor vende. Sem isso, cotar cimento manda e-mail para quem
-- vende cabo elétrico, e o fornecedor para de responder.
CREATE TABLE IF NOT EXISTS fornecedor_categorias (
    fornecedor_id       BIGINT NOT NULL REFERENCES fornecedores(id) ON DELETE CASCADE,
    categoria_insumo_id BIGINT NOT NULL REFERENCES insumo_categorias(id) ON DELETE CASCADE,
    PRIMARY KEY (fornecedor_id, categoria_insumo_id)
);

-- O COTADOR: a pessoa que responde pelo fornecedor. São vários, com função
-- diferente, e o mapa precisa registrar qual deles mandou cada proposta.
CREATE TABLE IF NOT EXISTS fornecedor_contatos (
    id             BIGSERIAL PRIMARY KEY,
    fornecedor_id  BIGINT NOT NULL REFERENCES fornecedores(id) ON DELETE CASCADE,
    nome           TEXT   NOT NULL,
    funcao         TEXT,
    email          TEXT,
    telefone       TEXT,
    recebe_cotacao BOOLEAN NOT NULL DEFAULT TRUE,
    ativo          BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Contato que não tem e-mail nem telefone não serve para disparar cotação,
    -- que é a única razão de ele existir.
    CONSTRAINT ck_contato_tem_canal CHECK (email IS NOT NULL OR telefone IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS ix_fornecedor_contatos_fornecedor
    ON fornecedor_contatos (fornecedor_id);

-- ---------------------------------------------------------------------------
-- Solicitação de cadastro de insumo
--
-- Cadastro aberto a todos produz duplicidade e nomenclatura inconsistente, e
-- aí os relatórios param de significar coisa alguma. Quem precisa PEDE; quem
-- responde por suprimentos decide o nome, a categoria e a conta, e cadastra.
-- ---------------------------------------------------------------------------
DO $$ BEGIN
    CREATE TYPE status_solicitacao_insumo AS ENUM ('PENDENTE', 'CADASTRADO', 'RECUSADO');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS insumo_solicitacoes (
    id             BIGSERIAL PRIMARY KEY,
    descricao      TEXT   NOT NULL,
    justificativa  TEXT,
    unidade        TEXT REFERENCES unidades_compra(codigo),
    solicitante_id BIGINT NOT NULL REFERENCES usuarios(id),
    obra_id        BIGINT REFERENCES obras(id),
    status         status_solicitacao_insumo NOT NULL DEFAULT 'PENDENTE',
    insumo_id      BIGINT REFERENCES insumos(id),
    motivo         TEXT,
    decidido_por   BIGINT REFERENCES usuarios(id),
    decidido_em    TIMESTAMPTZ,
    avisado_em     TIMESTAMPTZ,
    criado_em      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Cadastrada é cadastrada: tem de apontar para o insumo que nasceu dela.
    CONSTRAINT ck_solicitacao_insumo_cadastrada
        CHECK (status <> 'CADASTRADO' OR insumo_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS ix_insumo_solicitacoes_pendentes
    ON insumo_solicitacoes (status, criado_em) WHERE status = 'PENDENTE';
