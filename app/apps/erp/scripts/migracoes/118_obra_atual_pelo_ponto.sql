-- ===========================================================================
-- A OBRA ATUAL, LIDA DO PONTO (20/11/2026)
--
-- Ele: *"Transferência de obra deveremos fazer o seguinte, teremos a obra de
-- cadastro e a obra atual. Para a obra atual, vamos buscar a informação do
-- ponto. Uma rotina que atualize essa informação."*
--
-- A obra de CADASTRO continua em `colaboradores.obra_id` (o DP muda). A obra
-- ATUAL é onde a pessoa mais bateu ponto nos últimos 15 dias — a rotina diária
-- (`core/obra_atual.py`) recalcula com uma consulta só. Tabela própria, e não
-- coluna em `colaboradores`, de propósito: o modelo do colaborador é carregado
-- pelo ORM em quase toda tela, e coluna nova nele derruba o ERP se o código
-- subir antes desta atualização ser aplicada.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS colaborador_obra_atual (
    colaborador_id  BIGINT PRIMARY KEY REFERENCES colaboradores(id),
    obra_id         BIGINT REFERENCES obras(id),
    -- Desde quando o ponto mostra esta obra (muda quando a obra muda).
    desde           DATE NOT NULL DEFAULT CURRENT_DATE,
    ultima_batida   TIMESTAMPTZ,
    batidas         INTEGER NOT NULL DEFAULT 0,
    atualizado_em   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Cada mudança de obra que o ponto mostrou: é a lista das transferências.
CREATE TABLE IF NOT EXISTS colaborador_obra_mudancas (
    id              BIGSERIAL PRIMARY KEY,
    colaborador_id  BIGINT NOT NULL REFERENCES colaboradores(id),
    de_obra_id      BIGINT REFERENCES obras(id),
    para_obra_id    BIGINT REFERENCES obras(id),
    em              DATE NOT NULL DEFAULT CURRENT_DATE,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_colaborador_obra_mudancas_pessoa
    ON colaborador_obra_mudancas (colaborador_id, em);
