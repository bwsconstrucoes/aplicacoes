-- ---------------------------------------------------------------------------
-- APORTES E DEVOLUCOES NO OMIE — 20/09/2026
--
-- Pedido do dono, por escrito: lancar aporte e devolucao de aporte DENTRO do
-- OMIE sem ele ter de montar dois lancamentos a mao, um em cada conta,
-- lembrando qual nome usar de cada lado.
--
-- ⚠️ TRES TABELAS, E CADA UMA EXISTE POR UM MOTIVO DIFERENTE.
--
-- 1. `aporte_conta` e `aporte_categoria` sao DE-PARA CONFERIVEL.
--
--    O OMIE nao aceita o NOME da categoria: exige o codigo dela (tipo
--    "2.01.05"). O dono mexe no plano financeiro dele. Codigo chumbado no
--    programa vira, no dia em que ele mexer, LANCAMENTO ERRADO EM SILENCIO —
--    o pior defeito possivel aqui, porque ninguem tem como perceber olhando.
--
--    Entao o codigo e descoberto pela DESCRICAO, no espelho do OMIE
--    (`painel.cat`), e o resultado fica gravado aqui para o dono conferir e
--    corrigir. Descricao que nao aparece, ou que aparece DUAS vezes, para a
--    tela e pede a decisao dele — nunca escolhe uma por conta propria.
--
--    O mesmo vale para as contas correntes: "7011" e "22069" sao como o dono
--    chama as contas; o codigo que o OMIE usa e outro, e quem aponta a
--    correspondencia e ele, na tela.
--
-- 2. `aporte_lancamento` e o REGISTRO DO QUE FOI GRAVADO NO OMIE.
--
--    Isto nao e log de conforto. Uma escrita no OMIE altera o cadastro da
--    empresa e desfazer e trabalho manual, titulo a titulo. Sem registro de
--    QUAL titulo nasceu de QUAL lancamento daqui, nao ha como desfazer nem
--    como auditar.
--
--    ⚠️ `grupo` e o que AMARRA OS DOIS LADOS de uma operacao interna. Um
--    aporte da BWS e dois titulos: a saida na matriz e a entrada na parceria.
--    Se o segundo falhar depois do primeiro ter entrado, fica meio aporte no
--    OMIE — pior do que nao ter lancado nada, porque nenhum relatorio fecha e
--    ninguem percebe. O `grupo` e o que permite achar o par e o orfao.
-- ---------------------------------------------------------------------------

-- O de-para das contas: qual conta do OMIE e a "matriz" e qual e a "parceria".
-- O papel e a chave porque e ele que a regra de negocio conhece; a conta do
-- OMIE pode mudar, o papel nao.
CREATE TABLE IF NOT EXISTS analisesps.aporte_conta (
    papel          TEXT PRIMARY KEY,           -- 'matriz' | 'parceria'
    codigo_conta   BIGINT,                     -- id_conta_corrente do OMIE
    descricao      TEXT NOT NULL DEFAULT '',   -- como o OMIE chama a conta
    definido_em    TIMESTAMPTZ NOT NULL DEFAULT now(),
    definido_por   TEXT NOT NULL DEFAULT ''
);

-- O de-para das categorias do plano financeiro. A `chave` e o nome interno
-- que a regra usa; `descricao_procurada` e o texto que o dono escreveu no
-- plano financeiro dele.
CREATE TABLE IF NOT EXISTS analisesps.aporte_categoria (
    chave                 TEXT PRIMARY KEY,
    descricao_procurada   TEXT NOT NULL DEFAULT '',
    codigo                TEXT NOT NULL DEFAULT '',   -- ex.: '2.01.05'
    descricao_encontrada  TEXT NOT NULL DEFAULT '',
    transferencia         TEXT NOT NULL DEFAULT '',   -- 'S' = marcada como TRF
    definido_em           TIMESTAMPTZ NOT NULL DEFAULT now(),
    definido_por          TEXT NOT NULL DEFAULT ''
);

-- Cada titulo que este modulo mandou o OMIE criar. Uma linha por titulo; as
-- duas linhas de uma operacao interna compartilham o `grupo`.
CREATE TABLE IF NOT EXISTS analisesps.aporte_lancamento (
    id                      BIGSERIAL PRIMARY KEY,
    grupo                   TEXT NOT NULL,
    operacao                TEXT NOT NULL,              -- 'aporte_bws', ...
    papel                   TEXT NOT NULL DEFAULT '',   -- 'matriz' | 'parceria'
    sentido                 TEXT NOT NULL,              -- 'saida' | 'entrada'
    natureza                TEXT NOT NULL,              -- 'P' (pagar) | 'R' (receber)
    codigo_categoria        TEXT NOT NULL DEFAULT '',
    id_conta_corrente       BIGINT,
    codigo_cliente_fornecedor BIGINT,
    cod_departamento        TEXT NOT NULL DEFAULT '',   -- a obra
    valor                   NUMERIC(14, 2) NOT NULL DEFAULT 0,
    data                    DATE,
    numero_documento        TEXT NOT NULL DEFAULT '',
    observacao              TEXT NOT NULL DEFAULT '',
    codigo_integracao       TEXT NOT NULL DEFAULT '',   -- o que mandamos ao OMIE
    codigo_lancamento_omie  BIGINT,                     -- o que o OMIE devolveu
    baixado                 BOOLEAN NOT NULL DEFAULT FALSE,
    situacao                TEXT NOT NULL DEFAULT '',   -- 'gravado'|'falhou'|'desfeito'|'orfao'
    erro                    TEXT NOT NULL DEFAULT '',
    criado_em               TIMESTAMPTZ NOT NULL DEFAULT now(),
    criado_por              TEXT NOT NULL DEFAULT '',
    desfeito_em             TIMESTAMPTZ
);

-- "Quais titulos sao deste aporte?" e a pergunta de quem vai desfazer.
CREATE INDEX IF NOT EXISTS ix_aporte_lanc_grupo
    ON analisesps.aporte_lancamento (grupo);

-- "O que ficou orfao?" e a pergunta que nao pode depender de varrer a tabela:
-- e ela que aponta o trabalho manual pendente no OMIE.
CREATE INDEX IF NOT EXISTS ix_aporte_lanc_situacao
    ON analisesps.aporte_lancamento (situacao);

-- O codigo de integracao e unico por titulo: e ele que impede o OMIE de criar
-- dois titulos iguais se a mesma gravacao for tentada duas vezes (o navegador
-- que repete o envio, o clique duplo). O OMIE recusa a segunda com "codigo de
-- integracao ja cadastrado", e e assim que se quer.
CREATE UNIQUE INDEX IF NOT EXISTS ux_aporte_lanc_integracao
    ON analisesps.aporte_lancamento (codigo_integracao)
    WHERE codigo_integracao <> '';
