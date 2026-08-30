-- ============================================================================
-- Migração 004 — plano financeiro em 3 níveis + dedutibilidade no título
--
-- (a) CATEGORIAS ganham grupo/subgrupo, descrição de uso e "substituída por"
--     (permite aposentar uma conta e mandar os lançamentos para outra sem
--     tocar em título nenhum);
-- (b) DEDUTIBILIDADE sai da categoria e vira estado do TÍTULO, decidido depois
--     pelo financeiro ou pela IA a partir do documento;
-- (c) FORMA DE LIQUIDAÇÃO do tributo (retido na nota × guia × compensado)
--     vira atributo do lançamento — não gera conta separada.
-- ============================================================================

ALTER TABLE categorias
    ADD COLUMN IF NOT EXISTS grupo_codigo    TEXT,
    ADD COLUMN IF NOT EXISTS grupo_nome      TEXT,
    ADD COLUMN IF NOT EXISTS subgrupo_codigo TEXT,
    ADD COLUMN IF NOT EXISTS subgrupo_nome   TEXT,
    ADD COLUMN IF NOT EXISTS descricao_uso   TEXT,
    ADD COLUMN IF NOT EXISTS substituida_por_id BIGINT REFERENCES categorias(id),
    ADD COLUMN IF NOT EXISTS ordem           INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_categorias_grupo ON categorias (grupo_codigo, ordem);

-- dedutibilidade: estado do título
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'status_dedutibilidade') THEN
        CREATE TYPE status_dedutibilidade AS ENUM
            ('PENDENTE', 'DEDUTIVEL', 'INDEDUTIVEL', 'PARCIAL');
    END IF;
END$$;

ALTER TABLE titulos
    ADD COLUMN IF NOT EXISTS dedutibilidade status_dedutibilidade NOT NULL DEFAULT 'PENDENTE',
    ADD COLUMN IF NOT EXISTS dedutibilidade_valor    NUMERIC(14,2),
    ADD COLUMN IF NOT EXISTS dedutibilidade_motivo   TEXT,
    ADD COLUMN IF NOT EXISTS dedutibilidade_por      BIGINT REFERENCES usuarios(id),
    ADD COLUMN IF NOT EXISTS dedutibilidade_em       TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS dedutibilidade_origem   TEXT;   -- HUMANO | IA | REGRA

CREATE INDEX IF NOT EXISTS idx_titulos_dedutibilidade ON titulos (dedutibilidade)
    WHERE dedutibilidade = 'PENDENTE';

-- forma de liquidação de tributo no lançamento (retido na nota, guia, compensado)
ALTER TABLE titulos
    ADD COLUMN IF NOT EXISTS forma_liquidacao TEXT;

-- migra o antigo booleano para o novo estado, sem perder o que já foi marcado
UPDATE titulos SET dedutibilidade = 'INDEDUTIVEL'
 WHERE dedutivel IS FALSE AND dedutibilidade = 'PENDENTE';
