-- ============================================================================
-- Migração 013 — tabelas do aval (continuação da 012)
-- ============================================================================

CREATE TABLE IF NOT EXISTS titulo_avais (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    titulo_id       BIGINT      NOT NULL REFERENCES titulos(id),
    usuario_id      BIGINT      NOT NULL REFERENCES usuarios(id),
    papel           TEXT        NOT NULL,      -- SUPERVISOR_OBRA|GESTOR_OBRA|DIRETOR_FINANCEIRO|ADMIN
    decisao         TEXT        NOT NULL,      -- CONFIRMADO | RECUSADO
    motivo          TEXT,
    assinatura      TEXT        NOT NULL,      -- hash do que foi assinado
    resumo_assinado JSONB       NOT NULL,      -- estado do título no momento do aval
    ip              TEXT,
    dispositivo     TEXT,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_avais_titulo ON titulo_avais (titulo_id, decisao);

ALTER TABLE titulos
    ADD COLUMN IF NOT EXISTS exige_aval   BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS avalizado_em TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS avalizado_por BIGINT REFERENCES usuarios(id);

CREATE INDEX IF NOT EXISTS idx_titulos_aguardando_aval ON titulos (status)
    WHERE status = 'AGUARDANDO_AVAL';
