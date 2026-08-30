-- ============================================================================
-- Migração 006 — DE-PARA do plano antigo (Omie/Pipefy) para o plano novo
--
-- Os cards do Pipefy trazem o "Tipo de Despesa" com a nomenclatura antiga.
-- Esta tabela traduz para a conta nova na importação, sem exigir que ninguém
-- reclassifique card por card. Também guarda as traduções que a BWS ajustar
-- na tela, e o que ficou sem correspondência para ser resolvido.
-- ============================================================================
CREATE TABLE IF NOT EXISTS categoria_depara (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    origem_texto    TEXT        NOT NULL,          -- como vem do Pipefy/Omie
    origem_chave    TEXT        NOT NULL UNIQUE,   -- normalizado (busca)
    origem_codigo   TEXT,                          -- código Omie, quando houver
    categoria_id    BIGINT      REFERENCES categorias(id),
    confirmado      BOOLEAN     NOT NULL DEFAULT FALSE,
    origem_registro TEXT        NOT NULL DEFAULT 'PADRAO',  -- PADRAO | USUARIO | AUTO
    ocorrencias     INTEGER     NOT NULL DEFAULT 0,
    ultima_vez      TIMESTAMPTZ,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_depara_pendente ON categoria_depara (categoria_id)
    WHERE categoria_id IS NULL;
