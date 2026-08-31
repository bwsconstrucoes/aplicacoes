-- ============================================================================
-- Migração 015 — gestão de obras (não é mais um cadastro simples)
-- A obra ganha FASE (o acompanhamento que hoje vive no pipe Centro de Custo),
-- histórico de mudança de fase e os campos de garantia/documentação que
-- faltavam. Os documentos usam a tabela de anexos, já no banco.
-- ============================================================================
ALTER TABLE obras
    ADD COLUMN IF NOT EXISTS fase                TEXT NOT NULL DEFAULT 'CRIACAO',
    ADD COLUMN IF NOT EXISTS fase_desde          DATE,
    ADD COLUMN IF NOT EXISTS seguro_garantia     TEXT,
    ADD COLUMN IF NOT EXISTS seguro_vigencia_fim DATE,
    ADD COLUMN IF NOT EXISTS caucao_pct          NUMERIC(6,2),
    ADD COLUMN IF NOT EXISTS retencao_contratual_pct NUMERIC(6,2),
    ADD COLUMN IF NOT EXISTS crea_obra           TEXT,
    ADD COLUMN IF NOT EXISTS cei_obra            TEXT,
    ADD COLUMN IF NOT EXISTS observacoes         TEXT,
    ADD COLUMN IF NOT EXISTS data_conclusao      DATE,
    ADD COLUMN IF NOT EXISTS data_recebimento_provisorio DATE,
    ADD COLUMN IF NOT EXISTS data_recebimento_definitivo DATE;

CREATE TABLE IF NOT EXISTS obra_fases (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    obra_id     BIGINT NOT NULL REFERENCES obras(id) ON DELETE CASCADE,
    fase        TEXT   NOT NULL,
    observacao  TEXT,
    usuario_id  BIGINT REFERENCES usuarios(id),
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_obra_fases ON obra_fases (obra_id, criado_em DESC);
CREATE INDEX IF NOT EXISTS idx_obras_fase ON obras (fase);
