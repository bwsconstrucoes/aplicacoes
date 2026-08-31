-- ============================================================================
-- Migração 020 — contratos de empreita com medição e bloqueio de período
--
-- O problema real: a obra controla empreita em planilha. Lança "fulano fez
-- tantos metros, pagar tanto" e pede o pagamento. Se lançar duas vezes a mesma
-- medição, ninguém percebe. Aqui a empreita vira um CONTRATO com saldo, e cada
-- medição consome esse saldo — medir mais do que foi contratado é impossível,
-- e medir duas vezes o mesmo período é apontado.
--
-- O bloqueio de período impede alterar o passado já conciliado; o diretor
-- destrava uma janela quando for necessário.
-- ============================================================================

CREATE TABLE IF NOT EXISTS contratos_servico (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    numero          TEXT        NOT NULL UNIQUE,
    obra_id         BIGINT      NOT NULL REFERENCES obras(id),
    fornecedor_id   BIGINT      NOT NULL REFERENCES fornecedores(id),
    categoria_id    BIGINT      REFERENCES categorias(id),
    objeto          TEXT        NOT NULL,
    modo            TEXT        NOT NULL DEFAULT 'MEDICAO',  -- MEDICAO | PARCELAS
    unidade         TEXT,                       -- m², m³, vb, un…
    quantidade      NUMERIC(14,4),
    preco_unitario  NUMERIC(14,4),
    valor_total     NUMERIC(14,2) NOT NULL,
    valor_aditivos  NUMERIC(14,2) NOT NULL DEFAULT 0,
    parcelas_previstas SMALLINT,
    data_inicio     DATE,
    data_fim        DATE,
    status          TEXT        NOT NULL DEFAULT 'RASCUNHO',
        -- RASCUNHO | AGUARDANDO_AVAL | VIGENTE | CONCLUIDO | CANCELADO
    exige_foto      BOOLEAN     NOT NULL DEFAULT TRUE,
    observacoes     TEXT,
    criado_por      BIGINT      REFERENCES usuarios(id),
    aprovado_por    BIGINT      REFERENCES usuarios(id),
    aprovado_em     TIMESTAMPTZ,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_contratos_obra ON contratos_servico (obra_id, status);

CREATE TABLE IF NOT EXISTS contrato_medicoes (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    contrato_id     BIGINT      NOT NULL REFERENCES contratos_servico(id) ON DELETE CASCADE,
    numero          SMALLINT    NOT NULL,
    periodo_inicio  DATE,
    periodo_fim     DATE,
    quantidade      NUMERIC(14,4),
    percentual      NUMERIC(7,4),
    valor_medido    NUMERIC(14,2) NOT NULL,
    valor_adiantamento_abatido NUMERIC(14,2) NOT NULL DEFAULT 0,
    valor_liquido   NUMERIC(14,2) NOT NULL,
    observacao      TEXT,
    status          TEXT        NOT NULL DEFAULT 'MEDIDA',
        -- MEDIDA | AUTORIZADA | FATURADA | CANCELADA
    titulo_id       BIGINT      REFERENCES titulos(id),
    medido_por      BIGINT      REFERENCES usuarios(id),
    autorizado_por  BIGINT      REFERENCES usuarios(id),
    autorizado_em   TIMESTAMPTZ,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_medicao UNIQUE (contrato_id, numero)
);

CREATE INDEX IF NOT EXISTS idx_medicoes_contrato ON contrato_medicoes (contrato_id, status);

-- adiantamentos concedidos no contrato, a abater nas medições
ALTER TABLE titulos
    ADD COLUMN IF NOT EXISTS contrato_servico_id BIGINT REFERENCES contratos_servico(id),
    ADD COLUMN IF NOT EXISTS medicao_id          BIGINT REFERENCES contrato_medicoes(id),
    ADD COLUMN IF NOT EXISTS adiantamento_contrato BOOLEAN NOT NULL DEFAULT FALSE;

-- bloqueio de período: passado conciliado não se altera sem destravar
CREATE TABLE IF NOT EXISTS periodos_bloqueados (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ate_data      DATE        NOT NULL,
    liberado_ate  DATE,
    liberado_por  BIGINT      REFERENCES usuarios(id),
    liberado_em   TIMESTAMPTZ,
    liberado_motivo TEXT,
    liberado_expira TIMESTAMPTZ,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT now()
);
