-- ===========================================================================
-- 054 — O CADASTRO DE COLABORADORES NÃO ESPERA A SINCRONIZAÇÃO DAS SPs
--
-- O dono, 16/10/2026: "estou tentando atualizar o cadastro dos colaboradores
-- (…) Já existe uma atualização em andamento (trazendo as SPs alteradas) (…)
-- mas o que acho é que elas deveriam ser independentes, não?"
--
-- São mesmo: uma lê a planilha de colaboradores e grava o cadastro; a outra lê
-- a SPsBD e grava as SPs. A 041 já separava a pista DA PESSOA (ponto_pessoa,
-- ponto_lancar) da GERAL; agora são três — a do CADASTRO (colaboradores) sai da
-- geral. Cada pista continua com no máximo UMA execução viva.
--
-- O índice novo é mais fino que o anterior (toda combinação que ele aceitava
-- este também aceita), então criá-lo nunca esbarra em execução aberta.
-- ===========================================================================
DROP INDEX IF EXISTS analisesps.ux_execucao_viva_por_pista;

CREATE UNIQUE INDEX IF NOT EXISTS ux_execucao_viva_por_pista3
    ON analisesps.execucoes ((
        CASE WHEN tipo IN ('ponto_pessoa', 'ponto_lancar') THEN 'pessoa'
             WHEN tipo = 'colaboradores' THEN 'cadastro'
             ELSE 'geral' END))
    WHERE fim IS NULL;
