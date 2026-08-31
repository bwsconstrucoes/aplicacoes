-- ============================================================================
-- Migração 022 — insumos e gestão de locações
--
-- O descontrole de hoje: alguém loca, o equipamento entra na obra e ninguém
-- acompanha. A cobrança chega por e-mail para compras ou financeiro, que não
-- sabem do que se trata. Equipamento migra de obra e se perde.
--
-- Aqui a locação vira CONTRATO com itens, cada item ligado a um insumo e a uma
-- obra. O contrato gera a PREVISÃO das parcelas conforme a periodicidade, e o
-- título a pagar nasce dessa previsão — então o financeiro sabe o que é a
-- cobrança que chegou. Devolução parcial reduz o valor das próximas parcelas;
-- remanejo troca a obra sem perder o histórico.
-- ============================================================================

CREATE TABLE IF NOT EXISTS insumo_categorias (
    id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    codigo    TEXT NOT NULL UNIQUE,
    nome      TEXT NOT NULL,
    ativo     BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS insumos (
    id                    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    codigo                TEXT NOT NULL UNIQUE,
    descricao             TEXT NOT NULL,
    categoria_insumo_id   BIGINT REFERENCES insumo_categorias(id),
    categoria_id          BIGINT REFERENCES categorias(id),   -- conta do plano
    unidade               TEXT,
    locavel               BOOLEAN NOT NULL DEFAULT FALSE,
    valor_referencia_compra   NUMERIC(14,2),
    valor_referencia_locacao  NUMERIC(14,2),   -- por mês
    ativo                 BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em             TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_insumos_locavel ON insumos (locavel) WHERE locavel IS TRUE;

CREATE TABLE IF NOT EXISTS contratos_locacao (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    numero          TEXT        NOT NULL UNIQUE,
    fornecedor_id   BIGINT      NOT NULL REFERENCES fornecedores(id),
    obra_id         BIGINT      NOT NULL REFERENCES obras(id),
    categoria_id    BIGINT      REFERENCES categorias(id),
    numero_externo  TEXT,
    periodicidade   TEXT        NOT NULL DEFAULT 'MENSAL',  -- DIARIA|SEMANAL|QUINZENAL|MENSAL
    dia_vencimento  SMALLINT,
    data_inicio     DATE        NOT NULL,
    data_fim_prevista DATE,
    data_encerramento DATE,
    status          TEXT        NOT NULL DEFAULT 'ATIVO',   -- ATIVO|ENCERRADO|CANCELADO
    responsavel_id  BIGINT      REFERENCES usuarios(id),
    observacoes     TEXT,
    criado_por      BIGINT      REFERENCES usuarios(id),
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_locacao_status ON contratos_locacao (status, obra_id);

CREATE TABLE IF NOT EXISTS locacao_itens (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    contrato_id     BIGINT      NOT NULL REFERENCES contratos_locacao(id) ON DELETE CASCADE,
    insumo_id       BIGINT      REFERENCES insumos(id),
    descricao       TEXT        NOT NULL,
    quantidade      NUMERIC(14,4) NOT NULL,
    quantidade_devolvida NUMERIC(14,4) NOT NULL DEFAULT 0,
    valor_unitario  NUMERIC(14,4) NOT NULL,
    obra_id         BIGINT      REFERENCES obras(id),
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS locacao_movimentos (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    contrato_id     BIGINT      NOT NULL REFERENCES contratos_locacao(id) ON DELETE CASCADE,
    item_id         BIGINT      REFERENCES locacao_itens(id),
    tipo            TEXT        NOT NULL,   -- DEVOLUCAO | REMANEJO | ACRESCIMO
    quantidade      NUMERIC(14,4),
    obra_origem_id  BIGINT      REFERENCES obras(id),
    obra_destino_id BIGINT      REFERENCES obras(id),
    data_movimento  DATE        NOT NULL,
    documento       TEXT,
    observacao      TEXT,
    usuario_id      BIGINT      REFERENCES usuarios(id),
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS locacao_parcelas (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    contrato_id     BIGINT      NOT NULL REFERENCES contratos_locacao(id) ON DELETE CASCADE,
    competencia     DATE        NOT NULL,
    vencimento      DATE        NOT NULL,
    valor_previsto  NUMERIC(14,2) NOT NULL,
    titulo_id       BIGINT      REFERENCES titulos(id),
    status          TEXT        NOT NULL DEFAULT 'PREVISTA',  -- PREVISTA|LANCADA|CANCELADA
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_locacao_parcela UNIQUE (contrato_id, competencia)
);
CREATE INDEX IF NOT EXISTS idx_locacao_parcelas ON locacao_parcelas (status, vencimento);

ALTER TABLE titulos ADD COLUMN IF NOT EXISTS locacao_parcela_id BIGINT
    REFERENCES locacao_parcelas(id);
