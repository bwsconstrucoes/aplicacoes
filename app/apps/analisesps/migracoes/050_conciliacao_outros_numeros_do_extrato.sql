-- ===========================================================================
-- 050 — CONCILIAÇÃO: OS OUTROS JEITOS DE O EXTRATO ESCREVER A CONTA
--
-- O dono, 06/10/2026: *"com frequência extratos de conta que já haviam sido
-- detectados, ele não detecta a conta novamente"*.
--
-- A conta guardava UM jeito de o OFX escrevê-la (`ofx_bankid`/`ofx_acctid`, o
-- do primeiro extrato) e não o sobrescreve — de propósito, para um arquivo
-- trocado não estragar um cadastro certo. Mas o mesmo banco escreve a mesma
-- conta de mais de um jeito (com a agência na frente, com outro dígito…), e o
-- segundo jeito nunca era guardado: a pergunta voltava a cada extrato.
--
-- `ofx_outros`: os outros pares "banco:conta" (só dígitos, sem zeros à
-- esquerda) já confirmados para esta conta, separados por vírgula.
--
-- O código não lê nada daqui antes de este arquivo rodar (`tem_coluna`).
-- ===========================================================================
ALTER TABLE analisesps.conciliacao_conta
    ADD COLUMN IF NOT EXISTS ofx_outros TEXT NOT NULL DEFAULT '';
