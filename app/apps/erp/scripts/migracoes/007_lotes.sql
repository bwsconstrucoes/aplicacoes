-- ============================================================================
-- Migração 007 — LOTES de pagamento
-- Organiza as parcelas por prioridade e por quem solicitou, como o "Lote" da
-- Análise de SPs — mas como tabela de verdade, com histórico, e não como
-- coluna de texto na planilha.
-- ============================================================================
CREATE TABLE IF NOT EXISTS lotes (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nome            TEXT        NOT NULL,
    descricao       TEXT,
    prioridade      SMALLINT    NOT NULL DEFAULT 3,   -- 1 urgente … 5 baixa
    status          TEXT        NOT NULL DEFAULT 'ABERTO',  -- ABERTO|ENVIADO|PAGO|CANCELADO
    conta_bancaria_id BIGINT    REFERENCES contas_bancarias(id),
    data_prevista   DATE,
    criado_por      BIGINT      REFERENCES usuarios(id),
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    fechado_em      TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS lote_itens (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    lote_id     BIGINT NOT NULL REFERENCES lotes(id) ON DELETE CASCADE,
    parcela_id  BIGINT NOT NULL REFERENCES parcelas(id),
    ordem       INTEGER NOT NULL DEFAULT 0,
    observacao  TEXT,
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_lote_parcela UNIQUE (lote_id, parcela_id)
);

CREATE INDEX IF NOT EXISTS idx_lote_itens_parcela ON lote_itens (parcela_id);
CREATE INDEX IF NOT EXISTS idx_lotes_status ON lotes (status, prioridade);
