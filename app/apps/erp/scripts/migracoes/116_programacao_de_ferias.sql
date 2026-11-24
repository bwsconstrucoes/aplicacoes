-- ===========================================================================
-- A PROGRAMAÇÃO DE FÉRIAS — o que a contabilidade confirmou fica registrado
--
-- Pedido dele em 20/11/2026: *"o sistema precisa ter a programação dessas
-- férias (…) a gente pede à contabilidade, confirma o que foi registrado. O
-- sistema registra. Lembrando que tem que pensar na possibilidade de alteração
-- e cancelamento."*
--
-- Uma linha por período de férias. Ela nasce quando o DP atende o pedido de
-- férias (situação PEDIDA — esperando a contabilidade) e vira CONFIRMADA quando
-- a contabilidade devolve. Alteração e cancelamento são pedidos próprios: até a
-- contabilidade devolver, a mudança fica em `mudanca_pendente` e as datas
-- continuam as confirmadas; no retorno, a mudança vale e a anterior vai para o
-- `historico`. Nada se apaga.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS ferias_programadas (
    id                  SERIAL PRIMARY KEY,
    colaborador_id      INTEGER NOT NULL REFERENCES colaboradores(id),
    obra_id             INTEGER REFERENCES obras(id),
    inicio              DATE NOT NULL,
    dias                INTEGER NOT NULL CHECK (dias > 0),
    fim                 DATE NOT NULL,
    abono_pecuniario    BOOLEAN NOT NULL DEFAULT FALSE,
    adiantamento_13     BOOLEAN NOT NULL DEFAULT FALSE,
    situacao            TEXT NOT NULL DEFAULT 'PEDIDA'
        CHECK (situacao IN ('PEDIDA', 'CONFIRMADA', 'CANCELADA')),
    solicitacao_id      INTEGER REFERENCES dp_solicitacoes(id),
    pedido_id           INTEGER REFERENCES dp_pedidos(id),
    -- {"tipo": "ALTERACAO"|"CANCELAMENTO", "inicio", "dias", "solicitacao_id",
    --  "numero", "pedido_em"} enquanto a contabilidade não devolve.
    mudanca_pendente    JSONB,
    historico           JSONB NOT NULL DEFAULT '[]'::jsonb,
    observacao          TEXT,
    confirmada_em       TIMESTAMPTZ,
    cancelada_em        TIMESTAMPTZ,
    criado_em           TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_ferias_programadas_inicio
    ON ferias_programadas (inicio) WHERE situacao <> 'CANCELADA';
CREATE INDEX IF NOT EXISTS ix_ferias_programadas_pessoa
    ON ferias_programadas (colaborador_id);
