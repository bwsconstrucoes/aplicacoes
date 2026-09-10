-- ===========================================================================
-- Migração 048 — o CONTROLE DA NUMERAÇÃO das notas emitidas.
--
-- A PERGUNTA DO DONO, em 09/09/2026: *"tenho que ter cuidado em relação à
-- emissão manual e emissão via API e o controle da numeração das notas. Você
-- vai conseguir enxergar qual o número da nota que tem que ser emitida, para
-- registrar em sistema, e ser tranquilo? Eu digo para a gente manter o controle
-- da numeração corretamente."*
--
-- A RESPOSTA, e ela é boa: SIM, e por um motivo técnico concreto — no padrão
-- nacional e no ABRASF, **quem numera a DPS/RPS é QUEM EMITE**, não a
-- prefeitura. O município recebe a declaração numerada e devolve o número da
-- NOTA. São DOIS números diferentes, e confundi-los é a origem da bagunça:
--
--   numero_dps    a sequência da EMPRESA, por série. O ERP é dono dela.
--   numero_nota   o que a PREFEITURA devolveu. O ERP só registra.
--
-- ONDE ISSO QUEBRA NA VIDA REAL, e por que a tabela é assim:
--
--   1. DOIS PROCESSOS NA MESMA SÉRIE. Se alguém emite pelo portal da
--      prefeitura enquanto o ERP também emite, os dois consomem a mesma
--      sequência e nasce nota duplicada ou buraco. O índice único abaixo torna
--      o duplicado IMPOSSÍVEL — e o buraco, VISÍVEL.
--   2. TESTE GASTANDO NÚMERO DE PRODUÇÃO. Homologação tem sequência própria,
--      separada por `ambiente`. Sem isso, um teste queima número de verdade.
--   3. BURACO NA SEQUÊNCIA. É o que o fisco pergunta. Guardando cada número
--      reservado — inclusive os que FALHARAM — dá para dizer o que aconteceu
--      com cada um, em vez de "sumiu".
--
-- POR ISSO O NÚMERO É RESERVADO ANTES DE EMITIR, e a linha nasce PENDENTE.
-- Se a emissão falha, o número não some: fica registrado como falhado, com o
-- motivo. Número de nota fiscal não se apaga — se explica.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS notas_emitidas (
    id              BIGSERIAL PRIMARY KEY,
    empresa_id      BIGINT NOT NULL REFERENCES empresas(id),
    ambiente        TEXT NOT NULL DEFAULT 'HOMOLOGACAO',
    serie           TEXT NOT NULL DEFAULT '1',

    -- O número que o ERP controla, e o que a prefeitura devolveu.
    numero_dps      INT NOT NULL,
    numero_nota     TEXT,
    codigo_verificacao TEXT,
    chave_acesso    TEXT,

    -- De onde a nota nasceu. Um título a receber; e, quando for medição, a
    -- obra e a competência, que é como as pessoas procuram.
    titulo_id       BIGINT REFERENCES titulos(id),
    obra_id         BIGINT REFERENCES obras(id),
    competencia     DATE,

    modo            TEXT NOT NULL DEFAULT 'MANUAL',
    situacao        TEXT NOT NULL DEFAULT 'RESERVADA',
    valor_bruto     NUMERIC(14,2),
    valor_liquido   NUMERIC(14,2),
    retencoes       JSONB NOT NULL DEFAULT '{}'::jsonb,
    data_emissao    DATE,
    observacao      TEXT,
    motivo          TEXT,
    anexo_id        BIGINT REFERENCES anexos(id),
    substituida_por BIGINT REFERENCES notas_emitidas(id),

    criado_por      BIGINT REFERENCES usuarios(id),
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em   TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE notas_emitidas DROP CONSTRAINT IF EXISTS ck_nota_ambiente;
ALTER TABLE notas_emitidas ADD CONSTRAINT ck_nota_ambiente
    CHECK (ambiente IN ('HOMOLOGACAO', 'PRODUCAO'));

ALTER TABLE notas_emitidas DROP CONSTRAINT IF EXISTS ck_nota_modo;
ALTER TABLE notas_emitidas ADD CONSTRAINT ck_nota_modo
    CHECK (modo IN ('MANUAL', 'API'));

-- RESERVADA  → o número foi tomado, a emissão ainda não aconteceu
-- EMITIDA    → deu certo, e o número da prefeitura está guardado
-- FALHADA    → a emissão não foi; o número fica queimado, COM MOTIVO
-- CANCELADA  → a nota existiu e foi cancelada
-- SUBSTITUIDA→ trocada por outra, que fica em `substituida_por`
ALTER TABLE notas_emitidas DROP CONSTRAINT IF EXISTS ck_nota_situacao;
ALTER TABLE notas_emitidas ADD CONSTRAINT ck_nota_situacao
    CHECK (situacao IN ('RESERVADA', 'EMITIDA', 'FALHADA', 'CANCELADA', 'SUBSTITUIDA'));

-- Número queimado sem explicação é o que o fisco pergunta e ninguém sabe
-- responder. Falhar exige motivo escrito.
ALTER TABLE notas_emitidas DROP CONSTRAINT IF EXISTS ck_nota_falha_com_motivo;
ALTER TABLE notas_emitidas ADD CONSTRAINT ck_nota_falha_com_motivo
    CHECK (situacao NOT IN ('FALHADA', 'CANCELADA')
           OR nullif(btrim(motivo), '') IS NOT NULL);

-- Nota EMITIDA tem de ter o número que a prefeitura devolveu. Sem ele não há
-- o que informar ao cliente nem o que conciliar depois.
ALTER TABLE notas_emitidas DROP CONSTRAINT IF EXISTS ck_nota_emitida_tem_numero;
ALTER TABLE notas_emitidas ADD CONSTRAINT ck_nota_emitida_tem_numero
    CHECK (situacao <> 'EMITIDA' OR nullif(btrim(numero_nota), '') IS NOT NULL);

-- ===========================================================================
-- AS DUAS TRAVAS QUE FAZEM A NUMERAÇÃO SER CONFIÁVEL
-- ===========================================================================

-- 1. O MESMO NÚMERO DE DPS não sai duas vezes na mesma empresa, série e
--    ambiente. É isto que torna a duplicidade impossível, mesmo com duas
--    pessoas emitindo ao mesmo tempo.
CREATE UNIQUE INDEX IF NOT EXISTS idx_nota_dps
    ON notas_emitidas (empresa_id, ambiente, serie, numero_dps);

-- 2. O MESMO NÚMERO DE NOTA da prefeitura não é registrado duas vezes. Pega o
--    caso do lançamento manual digitado em duplicidade — que é justamente onde
--    o modo MANUAL erra.
CREATE UNIQUE INDEX IF NOT EXISTS idx_nota_numero
    ON notas_emitidas (empresa_id, ambiente, serie, numero_nota)
    WHERE numero_nota IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_nota_titulo ON notas_emitidas (titulo_id);
CREATE INDEX IF NOT EXISTS idx_nota_obra ON notas_emitidas (obra_id, competencia);
CREATE INDEX IF NOT EXISTS idx_nota_situacao ON notas_emitidas (situacao);
CREATE INDEX IF NOT EXISTS idx_nota_emissao ON notas_emitidas (data_emissao DESC);
