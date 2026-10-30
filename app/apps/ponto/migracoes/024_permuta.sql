-- ===========================================================================
-- 024 — A PERMUTA DE PLANTÃO DOS VIGIAS
--
-- O dono, 31/10/2026: "preciso resolver a questão das permutas dos vigias. O ponto continua algumas vezes sem
-- compreender. Então acho que preciso lançar um tipo de ajuste chamado de permuta, que valerá somente pra vigias
-- (…) e ainda gostaria que o DP visualizasse isso e pudesse aplicar."
--
-- A permuta é uma COMPENSAÇÃO com subtipo PERMUTA — a folga é a noite que a pessoa deixou, o dia trabalhado é a do
-- colega que ela cobriu — e com o COLEGA da troca, aqui. Aprovada, o sistema lança o outro lado (a compensação do
-- colega, espelhada) na mesma hora. Uma coluna, sem dado nenhum mexido.
-- ===========================================================================

ALTER TABLE ponto.ocorrencias ADD COLUMN IF NOT EXISTS colega_id BIGINT REFERENCES public.colaboradores (id);
