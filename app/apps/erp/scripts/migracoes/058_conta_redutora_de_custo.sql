-- ============================================================================
-- 058 — Conta redutora de custo
--
-- Devolução de material, estorno de despesa e reembolso de custas NÃO são
-- receita: são devolução de dinheiro que já saiu. Enquanto ficaram no grupo 1
-- (Receitas), a obra aparecia com receita a mais e custo inalterado, e a
-- margem saía errada dos dois lados.
--
-- Elas passam para o grupo 3 (Custos de obra) marcadas como REDUTORAS: entram
-- no relatório com sinal negativo, abatendo o custo. A despesa original
-- continua registrada — R$ 10.000 de compra e R$ 500 de devolução mostram
-- R$ 9.500 de custo, com as duas linhas visíveis no histórico. Nada de
-- estornar o lançamento original.
--
-- A coluna nasce FALSE para todas as contas existentes: só o plano padrão
-- marca quais são redutoras.
-- ============================================================================

ALTER TABLE categorias
    ADD COLUMN IF NOT EXISTS redutora BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN categorias.redutora IS
    'Conta que ABATE o custo em vez de somar. Entra no relatorio com sinal '
    'negativo. Ex.: devolucao de material a fornecedor.';
