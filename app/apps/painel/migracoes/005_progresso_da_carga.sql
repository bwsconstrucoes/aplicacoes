-- ===========================================================================
-- 005 — Andamento da atualização, gravado no banco
--
-- Até aqui o andamento vivia só na memória do processo. Duas consequências
-- ruins, as duas sentidas na prática:
--
--   1. Uma carga que levava horas dizia apenas "baixando", sem nunca dizer
--      quanto já andou. Não dava para distinguir "está indo" de "travou".
--   2. Se o serviço reiniciasse no meio — um envio de código faz isso —, a
--      execução sumia da tela. A linha ficava aqui sem `fim`, e a tela, que só
--      olhava execuções terminadas, mostrava a falha ANTERIOR como se fosse a
--      atual. O dono ficava lendo um erro velho achando que era novo.
--
-- Com o andamento no banco, a tela conta a verdade mesmo depois de recarregar,
-- de outro aparelho, ou depois de o servidor ter reiniciado.
-- ===========================================================================
ALTER TABLE painel.execucoes ADD COLUMN IF NOT EXISTS etapa      TEXT;
ALTER TABLE painel.execucoes ADD COLUMN IF NOT EXISTS progresso  TEXT;
-- batimento: enquanto a execução está viva, ela carimba a hora aqui. Se parar
-- de carimbar, é porque morreu — e a tela consegue dizer isso.
ALTER TABLE painel.execucoes ADD COLUMN IF NOT EXISTS visto_em   TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS ix_exec_abertas ON painel.execucoes(fim)
    WHERE fim IS NULL;
