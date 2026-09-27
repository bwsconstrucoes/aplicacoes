-- ===========================================================================
-- 032 — FERIADOS E FÉRIAS
--
-- Pedido do dono em 27/09/2026, com as palavras dele:
--
--   "Feriado nós vamos alimentar o sistema com os feriados, ok? (…) quais são os
--    feriados que são nacionais, quais são os feriados que é por obra, porque as
--    obras são em municípios diferentes."
--
--   "Férias (…) você já cria uma telazinha onde eu vou inserir as férias de cada
--    funcionário. Eu posso buscar pelo nome, pelo CPF e incluo o período. Só
--    isso. (…) E depois a gente pensa em numa forma de importar a informação via
--    arquivo."
--
-- PARA QUE SERVEM, e é a razão de existirem agora: o auxílio ALIMENTAÇÃO desconta
-- os dias de feriado e os dias de férias; o TRANSPORTE desconta férias mas não
-- feriado de um dia (decisão dele, §7.16.1 do documento da folha). Sem estas duas
-- tabelas, as duas telas de auxílio calculam o mês cheio para todo mundo.
--
-- ⚠️ POR OBRA, E NÃO POR MUNICÍPIO. Ele falou dos dois — "por obra, porque as
-- obras são em municípios diferentes". Guardar por MUNICÍPIO exigiria um de/para
-- obra → município que o sistema não tem, e inventá-lo agora seria mais uma peça
-- para dar errado. Por OBRA ele escolhe da lista que já existe (a mesma do
-- rateio), e a conta sai certa. Se um dia houver o município no cadastro da obra,
-- o feriado municipal passa a valer para todas as obras daquele município — e
-- esta tabela não muda, só ganha uma consulta nova.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS analisesps.feriado (
    id          SERIAL PRIMARY KEY,
    data        DATE NOT NULL,
    -- "nacional" vale para todo mundo; "obra" vale só para a obra escolhida.
    abrangencia TEXT NOT NULL CHECK (abrangencia IN ('nacional', 'obra')),
    -- Preenchida só quando a abrangência é "obra". O CHECK abaixo garante que as
    -- duas coisas não se contradigam — feriado "nacional" com obra escrita
    -- deixaria a consulta ambígua e ninguém saberia qual leitura vale.
    obra        TEXT NOT NULL DEFAULT '',
    descricao   TEXT NOT NULL DEFAULT '',
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    criado_por  TEXT NOT NULL DEFAULT '',
    CONSTRAINT feriado_obra_coerente CHECK (
        (abrangencia = 'nacional' AND obra = '')
        OR (abrangencia = 'obra' AND obra <> ''))
);

-- O MESMO FERIADO NÃO ENTRA DUAS VEZES. Sem isto, um 7 de setembro cadastrado em
-- duplicidade descontaria dois dias do auxílio de todo mundo.
CREATE UNIQUE INDEX IF NOT EXISTS ix_analisesps_feriado_unico
    ON analisesps.feriado (data, abrangencia, obra);

-- A pergunta do cálculo: "quais feriados caem neste período?".
CREATE INDEX IF NOT EXISTS ix_analisesps_feriado_data
    ON analisesps.feriado (data);


CREATE TABLE IF NOT EXISTS analisesps.ferias (
    id         SERIAL PRIMARY KEY,
    -- Só dígitos, como em todo o resto do módulo. É a chave para tudo.
    cpf        TEXT NOT NULL,
    -- O nome de quando foi cadastrado, para a tela reconhecer mesmo que a pessoa
    -- saia do cadastro depois. Quem manda continua sendo o CPF.
    nome       TEXT NOT NULL DEFAULT '',
    inicio     DATE NOT NULL,
    fim        DATE NOT NULL,
    observacao TEXT NOT NULL DEFAULT '',
    criado_em  TIMESTAMPTZ NOT NULL DEFAULT now(),
    criado_por TEXT NOT NULL DEFAULT '',
    -- Período invertido é erro de digitação, e o banco recusa: um fim antes do
    -- início daria contagem negativa de dias e o auxílio sairia a MAIS.
    CONSTRAINT ferias_periodo_coerente CHECK (fim >= inicio)
);

-- A pergunta do cálculo: "esta pessoa esteve de férias neste período?".
CREATE INDEX IF NOT EXISTS ix_analisesps_ferias_pessoa
    ON analisesps.ferias (cpf, inicio, fim);

-- ⚠️ NÃO HÁ ÍNDICE ÚNICO AQUI, e é decisão consciente: a mesma pessoa pode ter
-- dois períodos de férias no mesmo ano (férias fracionadas são a regra, não a
-- exceção). O que NÃO pode é dois períodos que se sobreponham — e isso o
-- Postgres não expressa sem `EXCLUDE USING gist`, que exigiria a extensão
-- btree_gist. Fica como crítica no código, onde a frase pode explicar o que
-- fazer (ver `folha_calendario.gravar_ferias`).
