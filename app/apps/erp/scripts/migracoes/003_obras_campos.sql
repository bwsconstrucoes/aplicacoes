-- Migração 003 — campos ricos da obra (origem: C.Diários / pipe Centro de Custo)
ALTER TABLE obras
    ADD COLUMN IF NOT EXISTS objeto          TEXT,
    ADD COLUMN IF NOT EXISTS cliente         TEXT,
    ADD COLUMN IF NOT EXISTS cnpj_cliente    TEXT,
    ADD COLUMN IF NOT EXISTS contrato        TEXT,
    ADD COLUMN IF NOT EXISTS valor_contrato  NUMERIC(14,2),
    ADD COLUMN IF NOT EXISTS aliquota_iss    NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS tributacao      TEXT,
    ADD COLUMN IF NOT EXISTS data_inicio     DATE,
    ADD COLUMN IF NOT EXISTS data_termino    DATE,
    ADD COLUMN IF NOT EXISTS orgao_resumido  TEXT,
    ADD COLUMN IF NOT EXISTS ref_pipefy      TEXT;
