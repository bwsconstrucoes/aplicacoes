-- ===========================================================================
-- 037 — A CARGA DO PONTO SABE SE TERMINOU, E A QUE CAIU RETOMA DE ONDE PAROU
--
-- Pergunta do dono em 29/09/2026: "O que acontece se o ponto der problema pra
-- baixar no meio do caminho?" — e, em seguida: "Precisa que caso a carga pare
-- que possa ser retomada de onde parou e que sejamos avisados."
--
-- Até aqui a carga anterior do mês era apagada ANTES de a nova começar, e uma
-- carga que caísse no meio deixava o mês com um pedaço — que a folha usava
-- como se fosse o mês inteiro. E recomeçava da página 1 na tentativa seguinte.
--
-- `terminada_em` diz se a carga chegou ao fim (NULL = em andamento, ou caiu).
-- Com ela:
--   - a carga nova nasce ao lado da antiga e só a substitui quando termina;
--   - `paginas_lidas` passa a ser o ANDAMENTO (páginas gravadas por inteiro,
--     cada uma na sua transação), e é dele que a retomada parte;
--   - o índice único por competência vale só para cargas terminadas: pode
--     haver uma em andamento junto com a que vale.
--
-- ⚠️ ANTES DESTA MIGRAÇÃO O CÓDIGO CONTINUA COMO ERA (apaga antes, não
-- retoma), com um aviso na carga — ver `ponto._substituicao_segura`.
-- ===========================================================================
ALTER TABLE analisesps.ponto_carga
    ADD COLUMN IF NOT EXISTS terminada_em TIMESTAMPTZ;

-- Tudo que existe hoje chegou ao fim (a versão anterior só gravava a linha
-- terminada). Sem isto, toda carga antiga viraria "em andamento" e sumiria.
UPDATE analisesps.ponto_carga
   SET terminada_em = carregado_em
 WHERE terminada_em IS NULL;

DROP INDEX IF EXISTS analisesps.ix_analisesps_ponto_carga_competencia;

CREATE UNIQUE INDEX IF NOT EXISTS ix_analisesps_ponto_carga_competencia
    ON analisesps.ponto_carga (ano, mes)
 WHERE terminada_em IS NOT NULL;
