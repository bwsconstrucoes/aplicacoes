-- ===========================================================================
-- 033 — OS AJUSTES DOS AUXÍLIOS, POR COMPETÊNCIA
--
-- ⚠️ ISTO É O QUE FAZ O AJUSTE À MÃO DELE SOBREVIVER. Palavras dele, em
-- 26/09/2026:
--
--   "Eu preciso ter a liberdade de escolher, de desmarcar uma pessoa para gerar o
--    pagamento ou não (…) caso eu queira alterar a obra que aquela pessoa vai
--    ficar apropriada (…) eu não altero a base do ponto, eu altero só a planilha
--    naquele momento."
--
-- Sem esta tabela, o cálculo do auxílio é refeito a cada visita à tela — e cada
-- recálculo apagaria o que ele mexeu. Ele teria de refazer, todo mês, tudo.
--
-- O QUE FICA AQUI É SÓ O QUE ELE MEXEU, não o cálculo. O valor, os dias e os
-- descontos continuam sendo calculados na hora, do cadastro + feriados + férias:
-- guardar o resultado do cálculo faria uma correção no cadastro não aparecer na
-- tela, e ninguém entenderia por quê.
--
-- Por competência E tipo: o ajuste de setembro não vale para outubro, e o da
-- alimentação não vale para o transporte.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS analisesps.auxilio_ajuste (
    id         SERIAL PRIMARY KEY,
    -- "alimentacao" ou "transporte". Texto e não ENUM porque uma verba nova
    -- (cesta, por exemplo) não deve exigir migração.
    tipo       TEXT NOT NULL,
    ano        INTEGER NOT NULL,
    mes        INTEGER NOT NULL CHECK (mes BETWEEN 1 AND 12),
    cpf        TEXT NOT NULL,

    -- NULO quer dizer "não mexi": vale a decisão do cálculo. É diferente de
    -- `false`, que é "eu decidi NÃO pagar". Um campo que não distinguisse as duas
    -- coisas faria o padrão virar decisão.
    pagar      BOOLEAN,
    -- Dias a mais ou a menos, somados ao que o cálculo achou. Pode ser negativo.
    dias       INTEGER,
    -- A obra que ELE escolheu, quando quer mudar de onde sai o dinheiro.
    obra       TEXT NOT NULL DEFAULT '',
    observacao TEXT NOT NULL DEFAULT '',

    alterado_em  TIMESTAMPTZ NOT NULL DEFAULT now(),
    alterado_por TEXT NOT NULL DEFAULT ''
);

-- UM AJUSTE POR PESSOA, VERBA E COMPETÊNCIA. Dois ajustes da mesma pessoa no
-- mesmo mês deixariam a tela sem saber qual vale.
CREATE UNIQUE INDEX IF NOT EXISTS ix_analisesps_auxilio_ajuste_unico
    ON analisesps.auxilio_ajuste (tipo, ano, mes, cpf);

-- A pergunta da tela: "o que foi mexido neste mês, nesta verba?".
CREATE INDEX IF NOT EXISTS ix_analisesps_auxilio_ajuste_competencia
    ON analisesps.auxilio_ajuste (tipo, ano, mes);
