-- 059 — A FOLHA DO 13º SALÁRIO (25/11/2026).
--
-- O dono: "vou lançar a folha do 13º. Pagamos o adiantamento dele em novembro e o
-- restante em dezembro. Ela é igual à folha da contabilidade (…) gostaria de
-- dividir cada funcionário de acordo com o ponto do ano."
--
-- A folha importada é UMA por competência e TIPO, e o tipo só aceitava
-- 'quinzena' e 'fim_de_mes'. O 13º entra como dois tipos novos — 'decimo_1' (1ª
-- parcela, o adiantamento) e 'decimo_2' (2ª parcela) — para NÃO substituir a
-- quinzena nem o fim de mês do mesmo mês.
--
-- A trava era uma CHECK escrita na própria coluna (029), que o Postgres chama de
-- "<tabela>_<coluna>_check". Sai e volta com a lista nova.
ALTER TABLE analisesps.folha DROP CONSTRAINT IF EXISTS folha_tipo_check;
ALTER TABLE analisesps.folha ADD CONSTRAINT folha_tipo_check
    CHECK (tipo IN ('quinzena', 'fim_de_mes', 'decimo_1', 'decimo_2'));
