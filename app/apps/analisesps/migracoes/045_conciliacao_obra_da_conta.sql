-- ===========================================================================
-- 045 — A OBRA DOS MOVIMENTOS DE CONTA CORRENTE MORA NA CONTA
--
-- O dono, 02/10/2026, sobre o lançamento de movimentos da conciliação:
--
--   *"no cadastro do Tipo pede Obra (opcional), ocorre que não podemos
--   colocar aqui pois como temos várias contas e o TIPO OMIE é meio genérico
--   vai dar erro de apropriação. Quero o seguinte, definir isso no cadastro
--   da Conta Corrente. Definir lá pra qual obra vão as tarifas."*
--
-- Mesmo raciocínio do fornecedor (migração 025): o tipo ("Tarifa PIX") vale
-- para todas as contas; a obra de cada movimento de conta corrente (tarifa,
-- rentabilidade, os demais tipos) depende da CONTA. O dono corrigiu no mesmo
-- dia: "na verdade é pros movimentos de conta corrente", não só tarifas. A coluna
-- do tipo (`conciliacao_tipo.cod_departamento`) fica no banco, sem uso.
--
-- O código não lê nada daqui antes de este arquivo rodar (`tem_coluna`).
-- ===========================================================================
ALTER TABLE analisesps.conciliacao_conta
    ADD COLUMN IF NOT EXISTS omie_departamento TEXT NOT NULL DEFAULT '';
