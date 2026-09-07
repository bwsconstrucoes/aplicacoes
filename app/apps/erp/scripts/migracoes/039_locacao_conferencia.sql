-- ===========================================================================
-- Migração 039 — a PREVISÃO DE DEVOLUÇÃO de cada equipamento e a CONFERÊNCIA
-- MENSAL dos equipamentos locados.
--
-- O PROBLEMA, nas palavras do dono: "muitas vezes eles são locados e deixam de
-- ser utilizados, não são devolvidos". O aluguel corre, ninguém devolve, e
-- meses depois já se pagou mais do que custaria comprar. O ERP já grita isso
-- (ver `_alertas` em core/locacoes.py) — o que faltava era ALGUÉM SER
-- OBRIGADO A RESPONDER.
--
-- DUAS COISAS ENTRAM, e as duas foram decididas com ele:
--
-- 1. PREVISÃO DE DEVOLUÇÃO POR EQUIPAMENTO, e não por contrato. É por
--    equipamento que a coisa acontece: a betoneira fica os oito meses da obra,
--    as escoras eram para três semanas na concretagem da laje. Um prazo só, do
--    contrato inteiro, não pega o esquecimento — que é justamente o das
--    escoras. A data é perguntada NA CONTRATAÇÃO, porque é ali que a pessoa
--    sabe a resposta e ainda não tem motivo para esconder.
--
-- 2. CONFERÊNCIA MENSAL, respondida pelo ADMINISTRATIVO DA OBRA. Uma por
--    contrato por competência. Para cada equipamento ele diz se está lá, se
--    está em uso, ONDE e para quê, e quando devolve. Fica gravado quem
--    respondeu — é isso que transforma "sumiu e ninguém sabe" em uma conversa
--    com uma pessoa.
--
-- O QUE NÃO ENTRA, e por decisão explícita dele: FOTO. Sem etiqueta no
-- equipamento, a foto prova muito pouco (metadado se falsifica e o WhatsApp
-- apaga o que existe) e dá trabalho a todo mundo todo mês.
--
-- A conferência NÃO BLOQUEIA o pagamento do aluguel: trocaria equipamento
-- esquecido por multa e briga com a locadora. Ela aparece como pendência e
-- avisa quem vai lançar a parcela.
-- ===========================================================================

-- --------------------------------------------------------------------------
-- 1. A previsão de devolução, no item do contrato
-- --------------------------------------------------------------------------
ALTER TABLE locacao_itens
    ADD COLUMN IF NOT EXISTS devolucao_prevista DATE;

-- A data ORIGINAL, de quando o contrato nasceu. Prorrogar é normal; prorrogar
-- em silêncio é que não pode. Guardando a primeira, dá para ver "esta escora
-- já foi empurrada três vezes".
ALTER TABLE locacao_itens
    ADD COLUMN IF NOT EXISTS devolucao_prevista_original DATE;

CREATE INDEX IF NOT EXISTS idx_locacao_itens_devolucao
    ON locacao_itens (devolucao_prevista)
    WHERE devolucao_prevista IS NOT NULL;

-- --------------------------------------------------------------------------
-- 2. A conferência mensal
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS locacao_conferencias (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    contrato_id     BIGINT      NOT NULL
                    REFERENCES contratos_locacao(id) ON DELETE CASCADE,
    -- primeiro dia do mês conferido
    competencia     DATE        NOT NULL,
    obra_id         BIGINT      REFERENCES obras(id),
    -- quem DEVE responder (o administrativo da obra) e quem respondeu
    responsavel_id  BIGINT      REFERENCES usuarios(id),
    respondida_por  BIGINT      REFERENCES usuarios(id),
    respondida_em   TIMESTAMPTZ,
    situacao        TEXT        NOT NULL DEFAULT 'ABERTA'
                    CHECK (situacao IN ('ABERTA', 'RESPONDIDA', 'DISPENSADA')),
    observacao      TEXT,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- uma por contrato por mês: abrir duas faria a equipe responder a mesma
    -- coisa duas vezes e não saber qual vale
    CONSTRAINT uq_conferencia_do_mes UNIQUE (contrato_id, competencia)
);

CREATE INDEX IF NOT EXISTS idx_conferencias_abertas
    ON locacao_conferencias (situacao, competencia);
CREATE INDEX IF NOT EXISTS idx_conferencias_responsavel
    ON locacao_conferencias (responsavel_id, situacao);

CREATE TABLE IF NOT EXISTS locacao_conferencia_itens (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    conferencia_id  BIGINT      NOT NULL
                    REFERENCES locacao_conferencias(id) ON DELETE CASCADE,
    item_id         BIGINT      NOT NULL
                    REFERENCES locacao_itens(id) ON DELETE CASCADE,

    -- o que a obra respondeu
    presente        TEXT        CHECK (presente IN ('SIM', 'NAO', 'NAO_ENCONTRADO')),
    em_uso          BOOLEAN,
    onde_esta       TEXT,          -- "na laje do bloco B, escorando até desforma"
    devolucao_prevista DATE,       -- confirmada ou empurrada, com motivo
    decisao         TEXT        CHECK (decisao IN ('MANTER', 'DEVOLVER', 'REMANEJAR')),
    obra_destino_id BIGINT      REFERENCES obras(id),   -- quando decide remanejar
    motivo          TEXT,
    -- a quantidade que a obra diz ter em mãos, quando difere do sistema
    quantidade_conferida NUMERIC(14,4),
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_conferencia_item UNIQUE (conferencia_id, item_id)
);

CREATE INDEX IF NOT EXISTS idx_conferencia_itens
    ON locacao_conferencia_itens (conferencia_id);
