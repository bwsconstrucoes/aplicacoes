-- ============================================================================
-- Migração 031 — restrições que impedem "dois títulos para uma coisa só"
--
-- A auditoria de consistência (AUDITORIA_TRANSACIONAL.md) mostrou que uma
-- falha no meio de uma operação NÃO deixa o banco pela metade — mas duas
-- pessoas na mesma operação no mesmo segundo podiam gerar dois títulos para a
-- mesma despesa de colaborador ou para a mesma medição, e conciliar a mesma
-- linha do extrato com dois pagamentos.
--
-- O código ganhou trava de linha (FOR UPDATE) nessas operações; estas
-- restrições são a segunda linha de defesa, no próprio banco: mesmo que um
-- caminho novo esqueça a trava, a duplicidade não entra.
--
-- Todas parciais (só valem onde há valor), então não mexem no que já existe
-- e não impedem NULL.
-- ============================================================================

-- uma despesa de colaborador gera no máximo UM título
CREATE UNIQUE INDEX IF NOT EXISTS uq_despesa_colaborador_titulo
    ON despesas_colaborador (titulo_id) WHERE titulo_id IS NOT NULL;

-- uma medição de empreita gera no máximo UM título
CREATE UNIQUE INDEX IF NOT EXISTS uq_medicao_titulo
    ON contrato_medicoes (titulo_id) WHERE titulo_id IS NOT NULL;

-- uma linha do extrato comprova no máximo UM pagamento (enquanto a
-- conciliação estiver de pé; desfeita, a linha volta a ficar livre)
CREATE UNIQUE INDEX IF NOT EXISTS uq_conciliacao_extrato_vigente
    ON conciliacoes (extrato_id) WHERE desfeita_em IS NULL;
