-- ===========================================================================
-- 011 — "FOI PAGO?" PASSA A TER UMA RESPOSTA SO
--
-- Ate aqui a mesma pergunta tinha DUAS respostas no sistema:
--
--   a CARGA (sync/fato.py) considerava quitado:
--       o status dizer pago/recebido/conciliado  OU  a baixa no OMIE dizer
--       liquidado (cLiquidado = 'S')
--
--   as TELAS (coluna `pago`, migracao 009) olhavam so a primeira metade:
--       situacao ~* '(pago|recebido|conciliado)'
--
-- Resultado: titulo baixado no OMIE cujo status ficou com outra palavra —
-- "ATRASADO", por exemplo — era dado por pago pela carga e nao era contado por
-- tela nenhuma. Nem no DRE, nem na Visao Geral, nem no fluxo de caixa, nem no
-- bloco de aportes. Na base do dono, em 20/09/2026, isso escondia
-- R$ 96.750,00 em 2 titulos.
--
-- A CORRECAO NAO E REPETIR A REGRA DA CARGA AQUI. Regra repetida e regra que
-- diverge de novo na proxima mudanca. A carga ja GRAVA a decisao que tomou, na
-- coluna `situacao_vencimento` ('Quitado' quando quitado). Entao a coluna
-- `pago` passa a ser essa decisao, e nada mais — uma so pessoa decide, as
-- telas obedecem.
--
-- ISTO MUDA NUMERO NA TELA, de proposito: o que estava escondido aparece. O
-- DRE e a Visao Geral sobem o valor que antes sumia.
--
-- Nao precisa refazer os numeros depois: `situacao_vencimento` ja esta gravada
-- em todas as linhas, e o Postgres preenche a coluna nova no instante em que
-- ela nasce. Trocar uma coluna gerada exige derrubar e recriar — os indices vem
-- junto, pelo mesmo motivo.
-- ===========================================================================

ALTER TABLE painel.fato DROP COLUMN IF EXISTS pago;

ALTER TABLE painel.fato
    ADD COLUMN pago BOOLEAN
    GENERATED ALWAYS AS (situacao_vencimento = 'Quitado') STORED;

CREATE INDEX IF NOT EXISTS ix_fato_pago      ON painel.fato(pago);
CREATE INDEX IF NOT EXISTS ix_fato_pago_ano  ON painel.fato(pago, ano);
CREATE INDEX IF NOT EXISTS ix_fato_pago_data ON painel.fato(pago, data);
