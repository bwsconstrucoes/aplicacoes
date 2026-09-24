-- ===========================================================================
-- O SALDO INICIAL DA CONTA — 24/09/2026
--
-- Pedido do dono depois de ver a tela com dado de verdade:
--
--   *"Eu estou vendo aqui o saldo, por exemplo, não está batendo de uma
--   determinada conta. Então acho que merece ser colocado o saldo inicial, e
--   uma data, para poder bater o saldo. Porque quando a gente cria uma conta
--   em sistema, normalmente voce bota saldo no dia tal, e aí a tendencia e
--   ele vai corrigindo."*
--
-- ⚠️ POR QUE O SALDO NAO BATIA, e nao era defeito: o extrato importado comeca
-- num dia qualquer — o dia em que a planilha dele comecou. Tudo o que a conta
-- movimentou ANTES disso nao existe aqui. Somando so o que existe, o saldo
-- da uma diferenca exatamente do tamanho do que veio antes.
--
-- O saldo inicial e a resposta: um numero e uma data que dizem "neste dia a
-- conta tinha isto", e a soma passa a partir dali. E o mesmo que qualquer
-- sistema de conciliacao faz ao cadastrar uma conta.
-- ===========================================================================
ALTER TABLE analisesps.conciliacao_conta
    ADD COLUMN IF NOT EXISTS saldo_inicial NUMERIC(14, 2) NOT NULL DEFAULT 0;

ALTER TABLE analisesps.conciliacao_conta
    ADD COLUMN IF NOT EXISTS saldo_inicial_em DATE;
