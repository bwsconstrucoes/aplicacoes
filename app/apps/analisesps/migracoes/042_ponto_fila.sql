-- ===========================================================================
-- 042 — A FILA DO PONTO POR PESSOA
--
-- O dono, 01/10/2026: "se eu clicar pra ajeitar o ponto de uma pessoa, posso
-- sair do analítico dela e ir resolver de outro enquanto o sistema trabalha?" —
-- podia sair, mas não pedir a próxima: era uma pessoa por vez, e a segunda era
-- recusada. "Sim, faça uma fila."
--
-- Cada pedido ("atualizar o ponto desta pessoa" ou "lançar batidas") vira uma
-- linha aqui. Um único trabalhador (a tarefa `ponto_pessoa`, na pista da pessoa)
-- resolve a fila em ordem, um pedido por vez, e escreve em cada linha o
-- andamento e o resultado — que a tela mostra mesmo depois de a pessoa sair e
-- voltar.
--
-- situacao: esperando → rodando → feito | falhou
-- ===========================================================================
CREATE TABLE IF NOT EXISTS analisesps.ponto_fila (
    id          SERIAL PRIMARY KEY,
    tipo        VARCHAR(10)   NOT NULL,          -- 'pessoa' | 'lancar'
    ano         INTEGER       NOT NULL,
    mes         INTEGER       NOT NULL,
    cpf         VARCHAR(11)   NOT NULL,
    nome        VARCHAR(160)  NOT NULL DEFAULT '',
    pedido      TEXT          NOT NULL DEFAULT '{}',
    situacao    VARCHAR(12)   NOT NULL DEFAULT 'esperando',
    progresso   VARCHAR(300)  NOT NULL DEFAULT '',
    mensagem    VARCHAR(2000) NOT NULL DEFAULT '',
    pedido_por  VARCHAR(120)  NOT NULL DEFAULT '',
    criado_em   TIMESTAMP     NOT NULL DEFAULT NOW(),
    inicio      TIMESTAMP,
    fim         TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_ponto_fila_situacao
    ON analisesps.ponto_fila (situacao, id);
CREATE INDEX IF NOT EXISTS ix_ponto_fila_cpf
    ON analisesps.ponto_fila (cpf, id);
