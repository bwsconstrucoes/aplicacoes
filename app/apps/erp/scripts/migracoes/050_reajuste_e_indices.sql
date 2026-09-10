-- ===========================================================================
-- Migração 050 — o REAJUSTE: data-base no contrato e a tabela dos índices.
--
-- PEDIDO DO DONO, 09/09/2026: *"dentro do cadastro do contrato a gente precisa
-- fazer alguma configuração que permita prever o recebimento de reajustes."*
--
-- POR QUE A DATA-BASE É CAMPO, E NÃO REGRA
--
-- Ele foi explícito: a data-base *"pode ser a do orçamento OU a da proposta da
-- licitação"*, e isso **muda por contrato**. Fixar uma das duas no código faria
-- o sistema errar em metade dos contratos — e errar num sentido perigoso, com
-- cara de certo, porque o valor sairia calculado e ninguém confere um número
-- que o computador deu.
--
-- POR QUE OS ÍNDICES SÃO TABELA, E NÃO CONTA NA HORA
--
-- Índice publicado é FATO com data. Guardar o número do mês, com a fonte e o
-- dia em que foi coletado, é o que permite refazer a conta de um reajuste de
-- dois anos atrás e chegar no mesmo valor. Buscar tudo na hora, toda vez,
-- daria resposta diferente conforme o dia — e reajuste vira discussão com o
-- órgão, então a conta tem de ser reproduzível.
--
-- A série guardada é a VARIAÇÃO MENSAL em porcento, que é como o Banco Central
-- publica no SGS (INCC-DI é a série 192). O número-índice se reconstrói
-- acumulando: quem guarda variação consegue produzir o acumulado de qualquer
-- período; quem guarda só o acumulado não consegue voltar.
--
-- A COLETA É PELO BOTÃO, nunca no start do gunicorn — mesma regra das
-- migrações. E a linha aceita origem MANUAL: o INCC-DI do mês só sai por volta
-- do dia 25, e quando o Banco Central estiver fora do ar alguém precisa poder
-- digitar o número do boletim da FGV e seguir a vida.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS indices_economicos (
    codigo        TEXT NOT NULL,          -- 'INCC-DI'
    competencia   DATE NOT NULL,          -- sempre o primeiro dia do mês
    variacao_pct  NUMERIC(12, 6) NOT NULL,
    fonte         TEXT NOT NULL DEFAULT 'BCB-SGS',
    coletado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (codigo, competencia)
);

-- A competência é SEMPRE o primeiro dia do mês. Sem isto, o mesmo mês entraria
-- duas vezes com dias diferentes e o acumulado contaria em dobro.
ALTER TABLE indices_economicos DROP CONSTRAINT IF EXISTS ck_indice_dia_um;
ALTER TABLE indices_economicos ADD CONSTRAINT ck_indice_dia_um
    CHECK (EXTRACT(DAY FROM competencia) = 1);

ALTER TABLE indices_economicos DROP CONSTRAINT IF EXISTS ck_indice_fonte;
ALTER TABLE indices_economicos ADD CONSTRAINT ck_indice_fonte
    CHECK (fonte IN ('BCB-SGS', 'MANUAL'));

CREATE INDEX IF NOT EXISTS idx_indice_codigo_competencia
    ON indices_economicos (codigo, competencia DESC);

-- ---------------------------------------------------------------------------
-- O contrato passa a dizer como reajusta
-- ---------------------------------------------------------------------------
ALTER TABLE contratos ADD COLUMN IF NOT EXISTS data_base DATE;
-- De ONDE veio a data-base. Não é enfeite: quando o órgão contestar o
-- reajuste, a primeira pergunta é "essa data é do orçamento ou da proposta?".
ALTER TABLE contratos ADD COLUMN IF NOT EXISTS data_base_origem TEXT;
ALTER TABLE contratos ADD COLUMN IF NOT EXISTS reajuste_meses INT;

ALTER TABLE contratos DROP CONSTRAINT IF EXISTS ck_contrato_data_base_origem;
ALTER TABLE contratos ADD CONSTRAINT ck_contrato_data_base_origem
    CHECK (data_base_origem IS NULL
           OR data_base_origem IN ('ORCAMENTO', 'PROPOSTA', 'ASSINATURA', 'OUTRA'));

-- Periodicidade em meses. O usual é 12 ("doze meses depois você tem direito ao
-- reajuste", palavras dele), mas contrato é contrato: fica configurável, com
-- teto para impedir digitação absurda.
ALTER TABLE contratos DROP CONSTRAINT IF EXISTS ck_contrato_reajuste_meses;
ALTER TABLE contratos ADD CONSTRAINT ck_contrato_reajuste_meses
    CHECK (reajuste_meses IS NULL OR (reajuste_meses BETWEEN 1 AND 120));

-- ---------------------------------------------------------------------------
-- O título de reajuste guarda COMO foi calculado
--
-- Um número solto não se defende. Guardando a data-base, o mês de referência e
-- o fator aplicado, o sistema consegue mostrar a conta inteira dois anos
-- depois — que é quando a pergunta aparece.
-- ---------------------------------------------------------------------------
ALTER TABLE titulos ADD COLUMN IF NOT EXISTS reajuste_indice TEXT;
ALTER TABLE titulos ADD COLUMN IF NOT EXISTS reajuste_data_base DATE;
ALTER TABLE titulos ADD COLUMN IF NOT EXISTS reajuste_ate DATE;
ALTER TABLE titulos ADD COLUMN IF NOT EXISTS reajuste_fator NUMERIC(14, 8);
ALTER TABLE titulos ADD COLUMN IF NOT EXISTS reajuste_previsto NUMERIC(14, 2);
