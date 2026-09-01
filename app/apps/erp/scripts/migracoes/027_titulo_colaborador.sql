-- ============================================================================
-- Migração 027 — todo pagamento de pessoa atrelado ao colaborador
-- A DC já vincula por item, mas título avulso (guia de FGTS, rescisão, exame,
-- vale) ficava solto — e aí o histórico da pessoa fica incompleto justamente
-- nos valores maiores. Agora o título pode apontar o colaborador, e há uma
-- visão única do que se pagou a cada um, venha de onde vier.
-- ============================================================================
ALTER TABLE titulos
    ADD COLUMN IF NOT EXISTS colaborador_id BIGINT REFERENCES colaboradores(id);
CREATE INDEX IF NOT EXISTS idx_titulos_colaborador ON titulos (colaborador_id)
    WHERE colaborador_id IS NOT NULL;

-- guias e verbas de pessoal referentes a vários colaboradores (ex.: FGTS do mês)
CREATE TABLE IF NOT EXISTS titulo_colaboradores (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    titulo_id       BIGINT NOT NULL REFERENCES titulos(id) ON DELETE CASCADE,
    colaborador_id  BIGINT NOT NULL REFERENCES colaboradores(id),
    valor           NUMERIC(14,2),
    observacao      TEXT,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_titulo_colaborador UNIQUE (titulo_id, colaborador_id)
);
CREATE INDEX IF NOT EXISTS idx_tc_colaborador ON titulo_colaboradores (colaborador_id);
