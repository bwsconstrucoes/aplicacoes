-- ============================================================================
-- Migração 023 — empreita com VÁRIOS serviços (planilha de orçamento)
-- Um contrato de empreita raramente tem um serviço só: tem alvenaria, reboco,
-- contrapiso, cada um com sua unidade, quantidade e preço. A medição passa a
-- ser por ITEM, como na engenharia: mede-se quanto avançou de cada serviço.
-- ============================================================================
CREATE TABLE IF NOT EXISTS contrato_servico_itens (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    contrato_id     BIGINT      NOT NULL REFERENCES contratos_servico(id) ON DELETE CASCADE,
    ordem           INTEGER     NOT NULL DEFAULT 0,
    descricao       TEXT        NOT NULL,
    unidade         TEXT,
    quantidade      NUMERIC(14,4) NOT NULL,
    preco_unitario  NUMERIC(14,4) NOT NULL,
    quantidade_aditivada NUMERIC(14,4) NOT NULL DEFAULT 0,
    insumo_id       BIGINT      REFERENCES insumos(id),
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_contrato_itens ON contrato_servico_itens (contrato_id, ordem);

CREATE TABLE IF NOT EXISTS medicao_itens (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    medicao_id      BIGINT      NOT NULL REFERENCES contrato_medicoes(id) ON DELETE CASCADE,
    contrato_item_id BIGINT     NOT NULL REFERENCES contrato_servico_itens(id),
    quantidade      NUMERIC(14,4) NOT NULL,
    valor           NUMERIC(14,2) NOT NULL,
    observacao      TEXT
);
CREATE INDEX IF NOT EXISTS idx_medicao_itens ON medicao_itens (medicao_id);
