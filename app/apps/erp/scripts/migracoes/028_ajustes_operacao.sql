-- ============================================================================
-- Migração 028 — ajustes vindos do uso real
-- 1. A obra passa a ter conta corrente de pagamento: é por ela que se filtra e
--    se decide de onde sai o dinheiro.
-- 2. O item da DC ganha categoria do plano: sem isso a despesa com colaborador
--    não entra na análise de custo por conta.
-- ============================================================================
ALTER TABLE obras
    ADD COLUMN IF NOT EXISTS conta_bancaria_id BIGINT REFERENCES contas_bancarias(id);

ALTER TABLE despesa_colaborador_itens
    ADD COLUMN IF NOT EXISTS categoria_id BIGINT REFERENCES categorias(id);
