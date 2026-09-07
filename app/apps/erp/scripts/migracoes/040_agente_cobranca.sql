-- ===========================================================================
-- Migração 040 — o registro do que o AGENTE falou, com quem, e quando.
--
-- POR QUE EXISTE
--
-- O agente vai atrás de quem tem pendência e manda mensagem. Sem registro,
-- três coisas ruins acontecem no primeiro mês:
--
--   1. ele repete a mesma cobrança várias vezes no mesmo dia (a rotina roda
--      diariamente, e sem memória toda execução acha que ninguém foi avisado);
--   2. ninguém consegue responder "o Ruan foi cobrado?" — que é exatamente a
--      pergunta que o dono vai fazer quando a obra disser que não sabia;
--   3. não há como saber se a mensagem saiu ou falhou, e mensagem que falha
--      em silêncio é pior do que não mandar: todo mundo acha que avisou.
--
-- A CHAVE ÚNICA é (assunto, referencia_id, destinatario_id, degrau): cada
-- pessoa recebe UMA vez cada degrau da escada para cada pendência. Se a rotina
-- rodar dez vezes no mesmo dia, o banco recusa a repetição — a trava é aqui,
-- não na esperança de o código lembrar.
--
-- ASSUNTO é texto de propósito, não ENUM: o agente nasce cobrando a
-- conferência de locação, mas foi pedido para servir ao sistema todo. Assunto
-- novo não pode exigir migração.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS agente_mensagens (
    id              BIGSERIAL PRIMARY KEY,

    -- o que está sendo cobrado: ('locacao_conferencia', 12)
    assunto         TEXT    NOT NULL,
    referencia_id   BIGINT  NOT NULL,

    destinatario_id BIGINT  REFERENCES usuarios(id),
    telefone        TEXT,                  -- para onde foi, como estava na hora

    -- Qual degrau da escada combinada com o dono:
    --   LEMBRETE  (dia 5)   — ainda é cortesia
    --   COBRANCA  (dia 10)  — já passou do prazo da obra
    --   ESCALADA  (dia 15)  — sobe para o dono e o financeiro
    degrau          TEXT    NOT NULL
                    CHECK (degrau IN ('LEMBRETE', 'COBRANCA', 'ESCALADA')),

    texto           TEXT    NOT NULL,
    canais          JSONB   NOT NULL DEFAULT '{}',   -- o que cada canal respondeu
    entregue        BOOLEAN NOT NULL DEFAULT FALSE,  -- algum canal aceitou
    erro            TEXT,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Uma vez cada degrau, por pendência, por pessoa. É esta linha que impede o
-- agente de virar spam quando a rotina rodar mais de uma vez no dia.
CREATE UNIQUE INDEX IF NOT EXISTS idx_agente_uma_vez
    ON agente_mensagens (assunto, referencia_id, destinatario_id, degrau);

CREATE INDEX IF NOT EXISTS idx_agente_assunto
    ON agente_mensagens (assunto, criado_em DESC);
