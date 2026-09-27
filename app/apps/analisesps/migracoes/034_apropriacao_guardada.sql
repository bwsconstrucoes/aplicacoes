-- ===========================================================================
-- 034 — A APROPRIAÇÃO GUARDADA
--
-- São DUAS coisas diferentes, e é importante não confundi-las:
--
--   apropriacao_ajuste  — O QUE ELE MEXEU À MÃO. Poucas linhas por competência.
--                         Sobrevive a recarregar o ponto, a trocar a regra de
--                         rateio e a reimportar a folha.
--   apropriacao         — O RESULTADO CONGELADO de um pagamento já gerado. Muitas
--                         linhas, escritas UMA vez, nunca recalculadas.
--
-- ⚠️ POR QUE AS DUAS, se o cálculo é rápido. Porque elas respondem perguntas
-- diferentes, e uma sozinha mente:
--
--   O AJUSTE tem de ficar SEPARADO DO CÁLCULO. Se eu guardasse o resultado com o
--   ajuste dentro, uma correção no cadastro (ou um ponto que veio pela metade e foi
--   recarregado) não apareceria mais na tela, e ninguém entenderia por quê.
--
--   O CONGELADO tem de existir porque o arquivo de pagamento já foi para o banco.
--   Depois de pago, "qual obra pagou o salário do Fulano em 09/2026" tem UMA
--   resposta, para sempre. Se essa resposta fosse recalculada, recarregar o ponto
--   de setembro em outubro mudaria a história de um dinheiro que já saiu — e o
--   rateio do mês seguinte, que ele decide olhando o total por obra, sairia sobre
--   número que não foi o pago.
--
-- Pedido do dono em 26/09/2026, que é a razão da tabela de ajustes:
--
--   "Eu preciso ter a liberdade (…) caso eu queira alterar a obra que aquela
--    pessoa vai ficar apropriada (…) eu não altero a base do ponto, eu altero só a
--    planilha naquele momento."
--
-- E em 27/09/2026, sobre o que ele não entendeu no meu resumo anterior — a
-- explicação está no `docs/FOLHA_DE_PAGAMENTO.md` §7.24: *"apropriação guardada.
-- Não entendi o que é isso aqui."* É isto.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 1. O QUE ELE MEXEU À MÃO
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analisesps.apropriacao_ajuste (
    id     SERIAL PRIMARY KEY,
    ano    INTEGER NOT NULL,
    mes    INTEGER NOT NULL CHECK (mes BETWEEN 1 AND 12),
    -- "quinzena" ou "fim_de_mes": o ajuste da quinzena não vale para o fim do mês.
    tipo   TEXT NOT NULL,
    cpf    TEXT NOT NULL,
    nome   TEXT NOT NULL DEFAULT '',

    -- TIRAR A PESSOA DESTE PAGAMENTO. É o "desmarcar" que ele pediu. O motivo é
    -- obrigatório na prática (a tela exige), porque quem abrir o relatório depois
    -- precisa saber por que faltou gente.
    fora   BOOLEAN NOT NULL DEFAULT FALSE,
    motivo TEXT NOT NULL DEFAULT '',

    -- O CAMINHO CURTO: tudo desta pessoa vai para UMA obra. É o caso comum ("o
    -- ponto dele está errado, joga tudo na CREPEOLINDA").
    obra_unica TEXT NOT NULL DEFAULT '',

    -- O CAMINHO LONGO fica na tabela filha abaixo: "um dia nesta obra, um dia
    -- naquela". Quando há filhas, elas mandam e `obra_unica` fica vazia.
    observacao TEXT NOT NULL DEFAULT '',

    criado_em    TIMESTAMPTZ NOT NULL DEFAULT now(),
    alterado_em  TIMESTAMPTZ NOT NULL DEFAULT now(),
    alterado_por TEXT NOT NULL DEFAULT ''
);

-- UM AJUSTE POR PESSOA E PAGAMENTO. Dois deixariam a apropriação sem saber qual
-- vale — e o valor iria para a obra errada sem nada na tela.
CREATE UNIQUE INDEX IF NOT EXISTS ix_analisesps_apropriacao_ajuste_unico
    ON analisesps.apropriacao_ajuste (ano, mes, tipo, cpf);

CREATE INDEX IF NOT EXISTS ix_analisesps_apropriacao_ajuste_competencia
    ON analisesps.apropriacao_ajuste (ano, mes, tipo);

-- A DIVISÃO À MÃO, obra por obra.
CREATE TABLE IF NOT EXISTS analisesps.apropriacao_ajuste_obra (
    id        SERIAL PRIMARY KEY,
    ajuste_id INTEGER NOT NULL
              REFERENCES analisesps.apropriacao_ajuste (id) ON DELETE CASCADE,
    obra      TEXT NOT NULL,
    -- Dias é informação de conferência; quem manda no dinheiro é `valor`.
    dias      INTEGER NOT NULL DEFAULT 0,
    valor     NUMERIC(14,2) NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS ix_analisesps_apropriacao_ajuste_obra_pai
    ON analisesps.apropriacao_ajuste_obra (ajuste_id);

-- ---------------------------------------------------------------------------
-- 2. O RESULTADO CONGELADO
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analisesps.apropriacao (
    id    SERIAL PRIMARY KEY,
    ano   INTEGER NOT NULL,
    mes   INTEGER NOT NULL CHECK (mes BETWEEN 1 AND 12),
    tipo  TEXT NOT NULL,
    -- A verba que este fechamento congela: "folha", "alimentacao",
    -- "transporte", "diaria"… Texto, para verba nova não exigir migração.
    verba TEXT NOT NULL DEFAULT 'folha',

    total_da_folha   NUMERIC(14,2) NOT NULL DEFAULT 0,
    total_apropriado NUMERIC(14,2) NOT NULL DEFAULT 0,
    pessoas          INTEGER NOT NULL DEFAULT 0,
    -- A conferência que autoriza pagar: o que se paga é o que foi apropriado.
    -- Guardada porque um fechamento que NÃO fechava tem de continuar dizendo que
    -- não fechava, mesmo que hoje feche.
    fecha            BOOLEAN NOT NULL DEFAULT FALSE,

    fechado_em  TIMESTAMPTZ NOT NULL DEFAULT now(),
    fechado_por TEXT NOT NULL DEFAULT ''
);

-- UM FECHAMENTO POR COMPETÊNCIA, PAGAMENTO E VERBA. Refechar substitui — e é
-- operação do mestre, com confirmação, porque apaga história de pagamento.
CREATE UNIQUE INDEX IF NOT EXISTS ix_analisesps_apropriacao_unico
    ON analisesps.apropriacao (ano, mes, tipo, verba);

CREATE TABLE IF NOT EXISTS analisesps.apropriacao_linha (
    id             BIGSERIAL PRIMARY KEY,
    apropriacao_id INTEGER NOT NULL
                   REFERENCES analisesps.apropriacao (id) ON DELETE CASCADE,
    cpf   TEXT NOT NULL DEFAULT '',
    nome  TEXT NOT NULL DEFAULT '',
    obra  TEXT NOT NULL DEFAULT '',
    dias  INTEGER NOT NULL DEFAULT 0,
    valor NUMERIC(14,2) NOT NULL DEFAULT 0,
    -- ⚠️ DE ONDE VEIO CADA VALOR: "ponto", "regra" ou "mao". Pedido do dono em
    -- 26/09/2026 — *"saber até de onde é que foi que veio aquela informação, se
    -- foi do ponto, se foi colocada de forma manual"*. Sem isto o relatório de
    -- auditoria não explica nada, e relatório que não se explica não é prova.
    origem TEXT NOT NULL DEFAULT ''
);

-- As duas perguntas do gerencial: "quanto esta obra custou" e "onde entrou o
-- salário desta pessoa".
CREATE INDEX IF NOT EXISTS ix_analisesps_apropriacao_linha_obra
    ON analisesps.apropriacao_linha (apropriacao_id, obra);
CREATE INDEX IF NOT EXISTS ix_analisesps_apropriacao_linha_pessoa
    ON analisesps.apropriacao_linha (cpf, apropriacao_id);
