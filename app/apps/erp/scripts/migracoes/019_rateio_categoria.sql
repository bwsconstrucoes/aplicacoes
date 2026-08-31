-- ============================================================================
-- Migração 019 — rateio por CATEGORIA além da obra
-- Uma mesma nota pode ter material e serviço, ou material de duas naturezas.
-- O rateio passa a aceitar categoria própria por linha; quando não informada,
-- vale a conta do título (comportamento de hoje, preservado).
-- ============================================================================
ALTER TABLE rateios
    ADD COLUMN IF NOT EXISTS categoria_id BIGINT REFERENCES categorias(id),
    ADD COLUMN IF NOT EXISTS descricao    TEXT;

CREATE INDEX IF NOT EXISTS idx_rateios_categoria ON rateios (categoria_id);
