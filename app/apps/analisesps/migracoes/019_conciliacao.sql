-- ===========================================================================
-- CONCILIACAO BANCARIA — o controle paralelo que hoje vive numa planilha
--
-- Pedido do dono em 24/09/2026. Ele mantem, ha anos, uma planilha "Controle
-- de Conciliacao" com uma aba por conta bancaria: cola o extrato, marca o que
-- ja bateu com o OMIE, e anota pendencia. A conciliacao "de verdade" continua
-- no OMIE; esta e a visao dele, que ele considera mais legivel — e o lugar
-- onde cabe a ANOTACAO, que o OMIE nao tem.
--
-- ⚠️ TRES TABELAS, E A RAZAO DE CADA UMA:
--
-- `conciliacao_conta`    — as contas bancarias. Existe para o OFX saber
--                          sozinho de qual conta ele e (banco + numero vem
--                          dentro do arquivo).
-- `conciliacao_extrato`  — as linhas do extrato. E o coracao.
-- `conciliacao_arquivo`  — cada OFX que entrou: quando, quem, que periodo
--                          cobria, quantas linhas eram novas. Sem isso,
--                          "esse extrato ja foi importado?" nao tem resposta.
-- ===========================================================================
CREATE SCHEMA IF NOT EXISTS analisesps;

CREATE TABLE IF NOT EXISTS analisesps.conciliacao_conta (
    id              SERIAL PRIMARY KEY,
    nome            TEXT NOT NULL,              -- como o dono chama: "BD 7011"
    banco           TEXT NOT NULL DEFAULT '',   -- "Bradesco", "Banco do Brasil"
    agencia         TEXT NOT NULL DEFAULT '',
    numero          TEXT NOT NULL DEFAULT '',   -- a conta, so digitos
    -- ⚠️ O QUE FAZ O OFX SE RECONHECER SOZINHO. O arquivo traz <BANKID> (o
    -- codigo do banco na camara de compensacao) e <ACCTID> (a conta). Guardar
    -- os dois aqui e o que evita perguntar "de qual conta e este arquivo?" a
    -- cada importacao — e evita o erro de responder errado.
    ofx_bankid      TEXT NOT NULL DEFAULT '',
    ofx_acctid      TEXT NOT NULL DEFAULT '',
    -- A aba da planilha que deu origem a esta conta. Guardada para a
    -- importacao saber onde parou, e para explicar de onde veio cada linha.
    aba_planilha    TEXT NOT NULL DEFAULT '',
    ativa           BOOLEAN NOT NULL DEFAULT TRUE,
    ordem           INTEGER NOT NULL DEFAULT 0,
    observacao      TEXT NOT NULL DEFAULT '',
    criada_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Duas contas com o mesmo par (banco, numero) fariam o OFX escolher no "sorteio".
CREATE UNIQUE INDEX IF NOT EXISTS ux_conc_conta_ofx
    ON analisesps.conciliacao_conta (ofx_bankid, ofx_acctid)
    WHERE ofx_bankid <> '' AND ofx_acctid <> '';

CREATE UNIQUE INDEX IF NOT EXISTS ux_conc_conta_nome
    ON analisesps.conciliacao_conta (lower(nome));


CREATE TABLE IF NOT EXISTS analisesps.conciliacao_arquivo (
    id              SERIAL PRIMARY KEY,
    conta_id        INTEGER NOT NULL REFERENCES analisesps.conciliacao_conta(id),
    nome_arquivo    TEXT NOT NULL DEFAULT '',
    -- O periodo que o arquivo cobre, lido de dentro dele (DTSTART/DTEND) ou,
    -- na falta, da menor e da maior data das transacoes. E o que responde
    -- "este periodo ja foi importado?".
    periodo_ini     DATE,
    periodo_fim     DATE,
    linhas_lidas    INTEGER NOT NULL DEFAULT 0,
    linhas_novas    INTEGER NOT NULL DEFAULT 0,
    linhas_repetidas INTEGER NOT NULL DEFAULT 0,
    -- A impressao digital do conteudo. O MESMO arquivo reenviado e
    -- reconhecido na hora, sem precisar reprocessar linha por linha.
    impressao       TEXT NOT NULL DEFAULT '',
    saldo_ofx       NUMERIC(14, 2),             -- <LEDGERBAL>, quando vem
    saldo_ofx_em    DATE,
    importado_em    TIMESTAMPTZ NOT NULL DEFAULT now(),
    importado_por   TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS ix_conc_arquivo_conta
    ON analisesps.conciliacao_arquivo (conta_id, periodo_ini);


CREATE TABLE IF NOT EXISTS analisesps.conciliacao_extrato (
    id              BIGSERIAL PRIMARY KEY,
    conta_id        INTEGER NOT NULL REFERENCES analisesps.conciliacao_conta(id),
    data            DATE NOT NULL,
    descricao       TEXT NOT NULL DEFAULT '',
    documento       TEXT NOT NULL DEFAULT '',
    -- ⚠️ UM VALOR SO, COM SINAL: negativo e saida. A planilha tem duas colunas
    -- (credito e debito) e isso obriga toda soma a lembrar de somar as duas e
    -- subtrair uma — que e exatamente o tipo de conta que sai errada uma vez
    -- e ninguem percebe. A tela mostra em duas colunas; o banco guarda uma.
    valor           NUMERIC(14, 2) NOT NULL DEFAULT 0,

    conciliado      BOOLEAN NOT NULL DEFAULT FALSE,
    conciliado_em   TIMESTAMPTZ,
    conciliado_por  TEXT NOT NULL DEFAULT '',
    observacao      TEXT NOT NULL DEFAULT '',

    -- De onde esta linha veio: 'ofx', 'planilha' ou 'mao'.
    origem          TEXT NOT NULL DEFAULT 'ofx',
    arquivo_id      INTEGER REFERENCES analisesps.conciliacao_arquivo(id),

    -- ⚠️ A IDENTIDADE DA LINHA, e o que torna reimportar o mesmo extrato
    -- inofensivo. Vem do FITID do banco quando existe; sem ele, de
    -- data+valor+historico+documento mais a ORDEM da repeticao dentro do
    -- arquivo — porque dois PIX iguais no mesmo dia sao dois lancamentos, e
    -- trata-los como um faz o extrato divergir do banco em silencio.
    -- (A regra e a mesma do `erp/core/pagamentos/ofx.py`, reusado aqui.)
    impressao       TEXT NOT NULL,
    fitid           TEXT NOT NULL DEFAULT '',

    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    alterado_em     TIMESTAMPTZ,
    alterado_por    TEXT NOT NULL DEFAULT ''
);

-- A trava que sustenta tudo: a mesma linha nao entra duas vezes na mesma conta.
CREATE UNIQUE INDEX IF NOT EXISTS ux_conc_extrato_impressao
    ON analisesps.conciliacao_extrato (conta_id, impressao);

-- A tela abre sempre por conta e por data; e tambem a ordem do saldo corrido.
CREATE INDEX IF NOT EXISTS ix_conc_extrato_conta_data
    ON analisesps.conciliacao_extrato (conta_id, data, id);

-- "O que falta conciliar?" e a pergunta que a tela existe para responder.
CREATE INDEX IF NOT EXISTS ix_conc_extrato_pendente
    ON analisesps.conciliacao_extrato (conta_id, conciliado, data);
