-- ===========================================================================
-- 056 — A BAIXA DOS COMPROVANTES NÃO SEGURA AS SPs (e vice-versa)
--
-- O dono, 11/11/2026: "Já existe uma atualização em andamento (dando baixa nos
-- comprovantes — Bradesco_11112026_081401.PDF: leva 2 de 2, desde 08:31). Espere
-- ela terminar. (…) esse comprovante tão sempre demorando muito."
--
-- A baixa (o robô do BaixaBradesco, leva por leva: leitura, Omie, planilha,
-- Pipefy) corria na pista GERAL, a mesma da sincronização das SPs e da fila de
-- envio para a planilha: enquanto um rodava, o outro esperava. Ela não escreve na
-- tabela das SPs — só na planilha, pelo robô, que já roda pelo Make a qualquer
-- hora, junto com a sincronização. Agora tem pista própria. Seis pistas, cada uma
-- com no máximo UMA execução viva:
--   pessoa       ponto_pessoa, ponto_lancar
--   cadastro     colaboradores
--   ponto        ponto, ponto_diario
--   apoios       apoios, notas_receita, notas_ciencia, fiscal, fiscal_ia,
--                faturamento, faturamento_antigas, faturamento_omie,
--                faturamento_completar
--   comprovantes comprovantes
--   geral        o resto — as SPs (sincronizar, carga_inicial, fila)
-- ⚠️ Tem de concordar com `tarefas.pista_do`.
--
-- O índice novo é mais fino que o da 055 (toda combinação que ele aceitava este
-- também aceita), então criá-lo nunca esbarra em execução aberta.
-- ===========================================================================
DROP INDEX IF EXISTS analisesps.ux_execucao_viva_por_pista5;

CREATE UNIQUE INDEX IF NOT EXISTS ux_execucao_viva_por_pista6
    ON analisesps.execucoes ((
        CASE WHEN tipo IN ('ponto_pessoa', 'ponto_lancar') THEN 'pessoa'
             WHEN tipo = 'colaboradores' THEN 'cadastro'
             WHEN tipo IN ('ponto', 'ponto_diario') THEN 'ponto'
             WHEN tipo IN ('apoios', 'notas_receita', 'notas_ciencia', 'fiscal',
                           'fiscal_ia', 'faturamento', 'faturamento_antigas',
                           'faturamento_omie', 'faturamento_completar') THEN 'apoios'
             WHEN tipo = 'comprovantes' THEN 'comprovantes'
             ELSE 'geral' END))
    WHERE fim IS NULL;
