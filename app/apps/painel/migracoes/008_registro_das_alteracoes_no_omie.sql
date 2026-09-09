-- ===========================================================================
-- 008 — O registro de tudo que o painel alterou no OMIE
--
-- É o único lugar do painel que escreve num sistema de fora, e alteração sem
-- rastro é alteração que ninguém consegue auditar nem desfazer. Cada envio
-- entra aqui: o que foi mandado, quem mandou, e o que o OMIE respondeu —
-- inclusive quando deu errado, que é justamente quando o registro importa.
--
-- EM TABELA, e não em arquivo. O painel Streamlit gravava num `.jsonl` no
-- disco; aqui o disco do Render é apagado a cada reinício, e o registro
-- sumiria junto. É a mesma razão de a configuração da prestação de contas
-- morar no banco.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS painel.alteracoes_omie (
    id                BIGSERIAL PRIMARY KEY,
    quando            TIMESTAMPTZ NOT NULL DEFAULT now(),
    codigo_lancamento BIGINT      NOT NULL,
    tipo              TEXT        NOT NULL,   -- pagar | receber
    documento         TEXT,
    simulacao         BOOLEAN     NOT NULL,   -- ensaio ou envio de verdade
    categoria_nova    TEXT,
    departamento_novo TEXT,
    mudancas          TEXT,                   -- o "de → para", em português
    ok                BOOLEAN     NOT NULL,
    retorno           TEXT                    -- o que o OMIE respondeu
);

-- para achar rápido o histórico de um título, que é a pergunta que se faz
-- quando alguém desconfia de uma alteração
CREATE INDEX IF NOT EXISTS ix_alteracoes_titulo
    ON painel.alteracoes_omie(codigo_lancamento, quando DESC);
CREATE INDEX IF NOT EXISTS ix_alteracoes_quando
    ON painel.alteracoes_omie(quando DESC);
