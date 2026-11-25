-- O extrato conferível (24/11/2026).
--
-- O dono do painel: "a gente pegar o extrato da conta corrente, inclusive eu
-- poder comparar com o extrato [do banco] aberto, o extrato de um lado e a
-- conta do outro. E conseguir visualizar o saldo do dia". E, sobre de onde vem
-- o saldo: "vai precisar ser informado o saldo inicial, para poder bater."
--
-- O painel sabe o que ANDOU em cada conta, não quanto havia nela. Então o
-- saldo de UM dia é informado por quem está com o extrato do banco na mão, e
-- o de todos os outros dias sai dele, somando ou tirando os movimentos.
--
-- Uma linha por conta: o saldo AO FIM do dia informado (é como o banco imprime
-- o "saldo do dia"). Informar de novo troca o anterior.
CREATE TABLE IF NOT EXISTS painel.extrato_saldo_informado (
    conta          TEXT PRIMARY KEY,
    dia            DATE NOT NULL,
    saldo          NUMERIC(16, 2) NOT NULL,
    informado_por  TEXT,
    informado_em   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Os dias que alguém já conferiu contra o extrato do banco, conta a conta —
-- para saber onde a conferência parou. Desmarcar apaga a linha.
CREATE TABLE IF NOT EXISTS painel.extrato_dia_conferido (
    conta          TEXT NOT NULL,
    dia            DATE NOT NULL,
    conferido_por  TEXT,
    conferido_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (conta, dia)
);
