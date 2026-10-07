-- 021 — Os lançamentos de conta corrente entram no painel, com a apropriação
--
-- 07/10/2026, o dono: "na conta Sicredi tem um lançamento com apropriação em
-- duas obras e ele não está sendo exibido nem contabilizado no painel. No OMIE
-- existe baixa, conciliação, favorecido, mas o painel não tá lendo isso."
--
-- O painel lia esses movimentos (os SEM TÍTULO: lançados direto na conta
-- corrente) e os guardava à parte, fora de toda conta — e jogava fora a
-- apropriação. A coluna nova guarda o movimento INTEIRO como o OMIE mandou
-- (departamentos, categorias, favorecido, número do lançamento), para o fato
-- poder ratear nas obras e para dar para conferir o que o OMIE manda de fato.
--
-- O código tolera a coluna ainda não existir: grava sem ela até o botão
-- "Aplicar atualizações do banco" ser apertado.

ALTER TABLE painel.movimentos_sem_titulo ADD COLUMN IF NOT EXISTS bruto TEXT;
