-- ===========================================================================
-- 055 — O PONTO DO MÊS E AS PLANILHAS DE APOIO NÃO SEGURAM AS SPs
--
-- O dono, 31/10/2026: "agora preciso que seja atualizado somente SPsBD e ele dá
-- essa mensagem: Já existe uma atualização em andamento (buscando notas na
-- Receita — … CTE, lote 1, desde 16:12). Espere ela terminar."
--
-- A busca na Receita (dentro das planilhas de apoio), a carga do ponto do mês e
-- o faturamento corriam na pista GERAL, a mesma das SPs: enquanto um deles
-- rodava (minutos), a SPsBD não se atualizava. Agora são cinco pistas, cada uma
-- com no máximo UMA execução viva:
--   pessoa   ponto_pessoa, ponto_lancar
--   cadastro colaboradores
--   ponto    ponto, ponto_diario
--   apoios   apoios, notas_receita, notas_ciencia, fiscal, fiscal_ia,
--            faturamento, faturamento_antigas, faturamento_omie,
--            faturamento_completar
--   geral    o resto — as SPs (sincronizar, carga_inicial, fila, comprovantes)
-- ⚠️ Tem de concordar com `tarefas.pista_do`.
--
-- O índice novo é mais fino que o da 054 (toda combinação que ele aceitava este
-- também aceita), então criá-lo nunca esbarra em execução aberta.
-- ===========================================================================
DROP INDEX IF EXISTS analisesps.ux_execucao_viva_por_pista3;

CREATE UNIQUE INDEX IF NOT EXISTS ux_execucao_viva_por_pista5
    ON analisesps.execucoes ((
        CASE WHEN tipo IN ('ponto_pessoa', 'ponto_lancar') THEN 'pessoa'
             WHEN tipo = 'colaboradores' THEN 'cadastro'
             WHEN tipo IN ('ponto', 'ponto_diario') THEN 'ponto'
             WHEN tipo IN ('apoios', 'notas_receita', 'notas_ciencia', 'fiscal',
                           'fiscal_ia', 'faturamento', 'faturamento_antigas',
                           'faturamento_omie', 'faturamento_completar') THEN 'apoios'
             ELSE 'geral' END))
    WHERE fim IS NULL;
