-- 034 — Suprimentos, fase 2: a solicitação e seus itens
--
-- Duas mudanças em relação à planilha de hoje, ambas pedidas pelo dono:
--
--   1. A OBRA É POR ITEM. A planilha obriga uma solicitação por obra; aqui uma
--      solicitação pode pedir material para obras diferentes, e o relatório
--      enviado ao fornecedor separa por endereço de entrega.
--   2. O ACOMPANHAMENTO É POR ITEM, não pela solicitação inteira — os itens de
--      um mesmo pedido seguem caminhos diferentes (um vai a cotação, outro sai
--      do almoxarifado, outro é cancelado).
--
-- As 15 situações e a lista de prioridades vieram da planilha em uso.

DO $$ BEGIN
    CREATE TYPE prioridade_solicitacao AS ENUM ('ALTA', 'MEDIA', 'NORMAL');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE status_item_suprimento AS ENUM (
        'SOLICITACAO', 'SALA_TECNICA', 'COTACAO', 'ANALISE_PROPOSTAS',
        'AUTORIZACAO', 'PEDIDO_EMITIDO', 'ALMOXARIFADO', 'AGUARDANDO_COLETA',
        'AGUARDANDO_ENTREGA', 'EM_TRANSITO', 'ENTREGUE', 'RECEBIDO',
        'PENDENCIA', 'CANCELADO', 'SUSPENSO');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS suprimento_solicitacoes (
    id               BIGSERIAL PRIMARY KEY,
    numero           TEXT NOT NULL UNIQUE,
    titulo           TEXT NOT NULL,
    previsao_entrega DATE,
    prioridade       prioridade_solicitacao NOT NULL DEFAULT 'NORMAL',
    observacoes      TEXT,
    solicitante_id   BIGINT NOT NULL REFERENCES usuarios(id),
    criado_em        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_sup_solicitacoes_solicitante
    ON suprimento_solicitacoes (solicitante_id, criado_em DESC);

CREATE TABLE IF NOT EXISTS suprimento_itens (
    id                   BIGSERIAL PRIMARY KEY,
    solicitacao_id       BIGINT NOT NULL REFERENCES suprimento_solicitacoes(id)
                                ON DELETE CASCADE,
    numero               INT    NOT NULL,
    insumo_id            BIGINT NOT NULL REFERENCES insumos(id),
    especificacao        TEXT,
    quantidade           NUMERIC(14,3) NOT NULL,
    quantidade_recebida  NUMERIC(14,3) NOT NULL DEFAULT 0,
    unidade              TEXT   NOT NULL REFERENCES unidades_compra(codigo),
    obra_id              BIGINT NOT NULL REFERENCES obras(id),
    status               status_item_suprimento NOT NULL DEFAULT 'SOLICITACAO',
    observacoes          TEXT,
    criado_em            TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (solicitacao_id, numero),
    CONSTRAINT ck_item_quantidade CHECK (quantidade > 0),
    -- Recebido não pode passar do pedido: o que sobra vira pendência, e
    -- pendência é saldo deste item — não um registro novo em outra tabela.
    CONSTRAINT ck_item_recebida CHECK (quantidade_recebida >= 0
                                   AND quantidade_recebida <= quantidade)
);

CREATE INDEX IF NOT EXISTS ix_sup_itens_obra_status
    ON suprimento_itens (obra_id, status);
CREATE INDEX IF NOT EXISTS ix_sup_itens_solicitacao
    ON suprimento_itens (solicitacao_id, numero);

-- Numeração da solicitação, separada das SPs do financeiro.
CREATE SEQUENCE IF NOT EXISTS seq_suprimento_solicitacao START 1;
