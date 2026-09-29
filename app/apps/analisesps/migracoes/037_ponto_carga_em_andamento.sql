-- ===========================================================================
-- 037 — A CARGA DO PONTO EM ANDAMENTO CONVIVE COM A CARGA TERMINADA DO MÊS
--
-- Pergunta do dono em 29/09/2026: "O que acontece se o ponto der problema pra
-- baixar no meio do caminho?" A resposta era ruim: a carga anterior do mês era
-- apagada ANTES de a nova começar, e uma carga que caísse no meio (rede,
-- Mobponto fora do ar, o serviço reiniciando numa publicação) deixava o mês
-- com um pedaço — e a folha usava o pedaço como se fosse o mês inteiro.
--
-- Agora a carga nova nasce com `paginas_lidas = 0` ("em andamento") ao lado da
-- antiga, e só quando termina a antiga é apagada, na mesma transação. Para
-- isso o índice único por competência passa a valer só para cargas TERMINADAS:
-- pode haver uma em andamento (ou interrompida) junto com a que vale.
--
-- ⚠️ ANTES DESTA MIGRAÇÃO O CÓDIGO CONTINUA COMO ERA (apaga antes), com um
-- aviso na carga — ver `ponto._substituicao_segura`. Depois do botão, a troca
-- é atômica.
-- ===========================================================================
DROP INDEX IF EXISTS analisesps.ix_analisesps_ponto_carga_competencia;

CREATE UNIQUE INDEX IF NOT EXISTS ix_analisesps_ponto_carga_competencia
    ON analisesps.ponto_carga (ano, mes)
 WHERE paginas_lidas > 0;
