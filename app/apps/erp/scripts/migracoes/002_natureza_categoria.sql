-- Migração 002 — natureza da categoria no plano financeiro:
--   RESULTADO = compõe DRE gerencial (custos/despesas/receitas)
--   FLUXO     = movimentação que não é resultado (transferências, empréstimos,
--               aportes, aplicações, pagamento de principal)
ALTER TABLE categorias
    ADD COLUMN IF NOT EXISTS natureza TEXT NOT NULL DEFAULT 'RESULTADO'
    CHECK (natureza IN ('RESULTADO', 'FLUXO'));
