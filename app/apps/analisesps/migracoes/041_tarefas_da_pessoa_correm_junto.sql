-- ===========================================================================
-- 041 — O PONTO DE UMA PESSOA NÃO ESPERA A CARGA DO MÊS
--
-- O dono, 01/10/2026: "tentei atualizar um ponto, mas deu isso: já existe uma
-- atualização em andamento (trazendo o ponto). Uma coisa não deveria ter nada
-- a ver com a outra. Ou seja, tento lançar e nada acontece."
--
-- A 004 deixava UMA execução viva no sistema inteiro. Trazer o ponto de uma
-- pessoa e lançar batidas para ela são trabalhos de segundos, sobre UMA pessoa,
-- e ficavam presos atrás da carga do mês (que leva dezenas de minutos e roda de
-- hora em hora). Agora são duas pistas: a GERAL (sincronização, carga do ponto,
-- fiscal…) e a DA PESSOA (ponto_pessoa, ponto_lancar). Cada pista continua com
-- no máximo UMA viva — duas cargas do mês ao mesmo tempo continuam impossíveis.
--
-- O índice é sobre "esta execução é da pista da pessoa?": verdadeiro numa pista,
-- falso na outra — no máximo uma viva de cada.
-- ===========================================================================
DROP INDEX IF EXISTS analisesps.ux_execucao_viva;

CREATE UNIQUE INDEX IF NOT EXISTS ux_execucao_viva_por_pista
    ON analisesps.execucoes ((tipo IN ('ponto_pessoa', 'ponto_lancar')))
    WHERE fim IS NULL;
