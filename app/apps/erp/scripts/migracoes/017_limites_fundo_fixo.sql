-- ============================================================================
-- Migração 017 — limites de fundo fixo por PESSOA
-- O teto não é do sistema: é de quem gasta. Um administrativo de obra pequena
-- e um gestor têm alçadas diferentes. Sem limite definido, o operador não
-- é impedido de prestar contas — apenas fica sem parâmetro de comparação, e o
-- sistema avisa que falta cadastrar.
-- ============================================================================
ALTER TABLE usuarios
    ADD COLUMN IF NOT EXISTS ff_teto_item      NUMERIC(14,2),
        -- valor máximo de UMA despesa avulsa
    ADD COLUMN IF NOT EXISTS ff_teto_prestacao NUMERIC(14,2),
        -- valor máximo do total de uma prestação
    ADD COLUMN IF NOT EXISTS ff_saldo_adiantamento NUMERIC(14,2) NOT NULL DEFAULT 0,
        -- quanto a empresa adiantou e ainda não foi prestado
    ADD COLUMN IF NOT EXISTS ff_autorizado     BOOLEAN NOT NULL DEFAULT FALSE;
        -- se a pessoa pode movimentar fundo fixo
