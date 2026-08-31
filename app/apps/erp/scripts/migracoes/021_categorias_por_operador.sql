-- ============================================================================
-- Migração 021 — contas liberadas por operador + rastro da nota
-- O administrativo de obra não deve enxergar o plano inteiro: lista vazia
-- significa "todas" (ninguém fica travado por falta de cadastro).
-- A chave de acesso e o CNO da nota ficam guardados para consulta e para
-- cruzar com o cadastro das obras.
-- ============================================================================
CREATE TABLE IF NOT EXISTS usuario_categorias (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usuario_id   BIGINT NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    categoria_id BIGINT NOT NULL REFERENCES categorias(id),
    criado_em    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_usuario_categoria UNIQUE (usuario_id, categoria_id)
);
CREATE INDEX IF NOT EXISTS idx_usuario_categorias ON usuario_categorias (usuario_id);

ALTER TABLE titulos
    ADD COLUMN IF NOT EXISTS chave_acesso_nfe TEXT,
    ADD COLUMN IF NOT EXISTS cno_documento    TEXT;
CREATE INDEX IF NOT EXISTS idx_titulos_chave ON titulos (chave_acesso_nfe)
    WHERE chave_acesso_nfe IS NOT NULL;
