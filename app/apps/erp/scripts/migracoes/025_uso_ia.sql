-- ============================================================================
-- Migração 025 — consumo de IA
-- Cada chamada à OpenAI devolve quantos tokens foram usados. Guardando isso,
-- dá para saber quanto custa a leitura de documentos por mês, por módulo e
-- por pessoa — e decidir com número, não com palpite.
-- ============================================================================
CREATE TABLE IF NOT EXISTS ia_uso (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    modelo          TEXT        NOT NULL,
    operacao        TEXT        NOT NULL,   -- leitura_documento, sugestao_categoria…
    tokens_entrada  INTEGER     NOT NULL DEFAULT 0,
    tokens_saida    INTEGER     NOT NULL DEFAULT 0,
    custo_usd       NUMERIC(12,6) NOT NULL DEFAULT 0,
    duracao_ms      INTEGER,
    sucesso         BOOLEAN     NOT NULL DEFAULT TRUE,
    erro            TEXT,
    usuario_id      BIGINT      REFERENCES usuarios(id),
    referencia      TEXT,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ia_uso_data ON ia_uso (criado_em DESC);
CREATE INDEX IF NOT EXISTS idx_ia_uso_operacao ON ia_uso (operacao, criado_em DESC);
