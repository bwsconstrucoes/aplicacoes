-- ===========================================================================
-- 013 — A CONFERÊNCIA DO ROSTO QUE SE ADAPTA À PESSOA
--
-- Pedido do dono, 17/10/2026: "quanto melhor a pessoa, menos se confere; quanto
-- pior a pessoa, mais se confere (…) a gente começa conferindo tudo e, à medida
-- que for compreendido que determinadas pessoas não têm a necessidade de tanta
-- conferência, a gente vai reduzindo. Mas se nas conferências aleatórias aquela
-- pessoa começar a aparecer de novo, a gente volta ela para insistir."
--
-- A batida que NÃO foi sorteada para conferir ganha a sua linha, com resultado
-- DISPENSADA e custo zero: assim ela não volta a cada rodada, e fica registrado
-- por que não foi conferida (o nível da pessoa e a chance de sorteio naquela hora).
-- A batida que ficou fora da regra (obra, dia ou horário excluído) também.
-- ===========================================================================

ALTER TABLE ponto.conferencias_rosto DROP CONSTRAINT IF EXISTS conferencias_rosto_resultado_check;
ALTER TABLE ponto.conferencias_rosto ADD CONSTRAINT conferencias_rosto_resultado_check
    CHECK (resultado IN ('MESMA_PESSOA', 'OUTRA_PESSOA', 'SEM_ROSTO', 'SEM_CADASTRAL', 'VIROU_CADASTRAL',
                         'ERRO', 'DISPENSADA'));

-- O nível da pessoa quando a batida passou pela conferência, e a chance (%) de
-- ser sorteada naquele nível.
ALTER TABLE ponto.conferencias_rosto ADD COLUMN IF NOT EXISTS nivel TEXT;
ALTER TABLE ponto.conferencias_rosto ADD COLUMN IF NOT EXISTS chance SMALLINT;
