-- ===========================================================================
-- LANCAR NO OMIE A PARTIR DA CONCILIACAO — 24/09/2026
--
-- Pedido do dono:
--
--   *"O que eu quero fazer e permitir a gravacao no OMIE a partir da
--   planilha. Por exemplo, tarifas bancarias, rentabilidade de investimento.
--   Porque tem muita tarifa de PIX. Esse ai a gente poder lancar direto no
--   OMIE: selecionar e gravar essa movimentacao financeira."*
--
-- Sao lancamentos que aparecem no extrato e NAO nascem de uma SP: ninguem
-- pede autorizacao para pagar tarifa de PIX. Hoje ele digita um por um no
-- OMIE; aqui ele marca no extrato e manda.
--
-- ⚠️ TRES COISAS PRECISAM EXISTIR ANTES, e sao as tres que esta migracao cria:
--
-- 1. A CONTA CORRENTE DO OMIE de cada conta bancaria. Sem ela o lancamento
--    nasceria na conta errada — o erro mais caro possivel aqui.
-- 2. O TIPO de movimento (tarifa, rentabilidade, tarifa PIX), com a CATEGORIA
--    do plano financeiro e o fornecedor. E o que o dono digitaria no OMIE.
-- 3. A MARCA NA LINHA de que ela ja foi lancada — para nao lancar duas vezes.
-- ===========================================================================

-- 1. A conta corrente do OMIE, na conta bancaria.
ALTER TABLE analisesps.conciliacao_conta
    ADD COLUMN IF NOT EXISTS omie_conta_corrente BIGINT;

-- 2. Os tipos de movimento que se repetem.
CREATE TABLE IF NOT EXISTS analisesps.conciliacao_tipo (
    id                  SERIAL PRIMARY KEY,
    nome                TEXT NOT NULL,          -- "Tarifa bancaria"
    -- O que procurar no historico do extrato, separado por ponto-e-virgula:
    -- "TARIFA;TAR PIX". Case e acento nao contam.
    palavras            TEXT NOT NULL DEFAULT '',
    codigo_categoria    TEXT NOT NULL DEFAULT '',   -- do plano financeiro
    -- O fornecedor (tarifa) ou cliente (rentabilidade) no OMIE.
    codigo_cliente      BIGINT,
    -- A obra/departamento, quando o movimento pertence a uma.
    cod_departamento    TEXT NOT NULL DEFAULT '',
    ativo               BOOLEAN NOT NULL DEFAULT TRUE,
    ordem               INTEGER NOT NULL DEFAULT 0,
    criado_em           TIMESTAMPTZ NOT NULL DEFAULT now(),
    criado_por          TEXT NOT NULL DEFAULT ''
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_conc_tipo_nome
    ON analisesps.conciliacao_tipo (lower(nome));

-- 3. A marca na linha do extrato.
ALTER TABLE analisesps.conciliacao_extrato
    ADD COLUMN IF NOT EXISTS omie_codigo BIGINT;
ALTER TABLE analisesps.conciliacao_extrato
    ADD COLUMN IF NOT EXISTS omie_integracao TEXT NOT NULL DEFAULT '';
ALTER TABLE analisesps.conciliacao_extrato
    ADD COLUMN IF NOT EXISTS omie_situacao TEXT NOT NULL DEFAULT '';
ALTER TABLE analisesps.conciliacao_extrato
    ADD COLUMN IF NOT EXISTS omie_erro TEXT NOT NULL DEFAULT '';
ALTER TABLE analisesps.conciliacao_extrato
    ADD COLUMN IF NOT EXISTS omie_em TIMESTAMPTZ;
ALTER TABLE analisesps.conciliacao_extrato
    ADD COLUMN IF NOT EXISTS omie_por TEXT NOT NULL DEFAULT '';
ALTER TABLE analisesps.conciliacao_extrato
    ADD COLUMN IF NOT EXISTS tipo_id INTEGER
        REFERENCES analisesps.conciliacao_tipo(id);

-- ⚠️ A TRAVA CONTRA LANCAR DUAS VEZES. O codigo de integracao e unico: se a
-- mesma linha for mandada de novo, o OMIE recusa com "codigo de integracao ja
-- cadastrado" — e e assim que se quer, porque a recusa dele vale mais que
-- qualquer conferencia feita deste lado.
CREATE UNIQUE INDEX IF NOT EXISTS ux_conc_extrato_omie_integracao
    ON analisesps.conciliacao_extrato (omie_integracao)
    WHERE omie_integracao <> '';

-- "O que ja foi lancado?" e "o que falhou?" sao as duas perguntas da tela.
CREATE INDEX IF NOT EXISTS ix_conc_extrato_omie
    ON analisesps.conciliacao_extrato (conta_id, omie_situacao)
    WHERE omie_situacao <> '';
