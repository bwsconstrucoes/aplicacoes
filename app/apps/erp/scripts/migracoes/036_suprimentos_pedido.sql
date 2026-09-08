-- 036 — Suprimentos, fase 4: pedido de compra, autorização e previsão
--
-- Existem DOIS tipos de pedido e ambos vão para a mesma fila de autorização:
--   - VIA MAPA: nasce da seleção de itens e fornecedor dentro do mapa, e quem
--     autoriza vê as alternativas que o comprador tinha;
--   - DIRETO: fechado sem mapa, quando o valor é pequeno ou já se sabe o
--     melhor preço.
--
-- PEDIDO AUTORIZADO GERA PREVISÃO DE PAGAMENTO — e só então. Antes de
-- autorizado não gera nada, porque um pedido que ninguém liberou não é
-- obrigação da empresa.

DO $$ BEGIN
    CREATE TYPE status_pedido_compra AS ENUM (
        'AGUARDANDO_AUTORIZACAO', 'AUTORIZADO', 'RECUSADO', 'CANCELADO');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS pedidos_compra (
    id                    BIGSERIAL PRIMARY KEY,
    numero                TEXT NOT NULL UNIQUE,
    cotacao_id            BIGINT REFERENCES cotacoes(id),
    fornecedor_id         BIGINT NOT NULL REFERENCES fornecedores(id),
    contato_id            BIGINT REFERENCES fornecedor_contatos(id),
    condicao_pagamento_id BIGINT REFERENCES condicoes_pagamento(id),
    entrega               modo_entrega,
    frete                 NUMERIC(14,2) NOT NULL DEFAULT 0,
    desconto              NUMERIC(14,2) NOT NULL DEFAULT 0,
    previsao_entrega      DATE,
    antecipado            BOOLEAN NOT NULL DEFAULT FALSE,
    codigo_barras         TEXT,
    observacoes           TEXT,
    status                status_pedido_compra NOT NULL DEFAULT 'AGUARDANDO_AUTORIZACAO',
    motivo                TEXT,
    criado_por            BIGINT NOT NULL REFERENCES usuarios(id),
    criado_em             TIMESTAMPTZ NOT NULL DEFAULT now(),
    autorizado_por        BIGINT REFERENCES usuarios(id),
    autorizado_em         TIMESTAMPTZ,
    CONSTRAINT ck_pedido_encargos CHECK (frete >= 0 AND desconto >= 0),
    -- Autorizado tem de ter quem autorizou; recusado tem de ter motivo. Sem
    -- isso, meses depois ninguém sabe quem liberou a compra nem por que a
    -- outra foi barrada.
    CONSTRAINT ck_pedido_autorizado
        CHECK (status <> 'AUTORIZADO' OR autorizado_por IS NOT NULL),
    CONSTRAINT ck_pedido_recusado
        CHECK (status <> 'RECUSADO' OR motivo IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS ix_pedidos_status ON pedidos_compra (status, criado_em DESC);
CREATE INDEX IF NOT EXISTS ix_pedidos_fornecedor ON pedidos_compra (fornecedor_id);

CREATE TABLE IF NOT EXISTS pedido_itens (
    id                 BIGSERIAL PRIMARY KEY,
    pedido_id          BIGINT NOT NULL REFERENCES pedidos_compra(id) ON DELETE CASCADE,
    suprimento_item_id BIGINT NOT NULL REFERENCES suprimento_itens(id),
    numero             INT    NOT NULL,
    quantidade         NUMERIC(14,3) NOT NULL,
    preco_unitario     NUMERIC(14,4) NOT NULL,
    UNIQUE (pedido_id, numero),
    CONSTRAINT ck_pedido_item_valores CHECK (quantidade > 0 AND preco_unitario > 0)
);

-- Um item de solicitação só pode estar em UM pedido vivo. Dois cliques no mesmo
-- mapa comprariam o mesmo material duas vezes.
--
-- Não dá para expressar isso como índice parcial: a condição depende do status
-- do PEDIDO, em outra tabela, e índice do Postgres não aceita subconsulta. A
-- saída é esta tabela de reserva, cuja chave primária é a própria garantia: o
-- item entra aqui quando o pedido nasce e sai quando o pedido é recusado ou
-- cancelado. A regra fica no banco, e não só na boa vontade do código.
CREATE TABLE IF NOT EXISTS pedido_item_reserva (
    suprimento_item_id BIGINT PRIMARY KEY REFERENCES suprimento_itens(id)
                              ON DELETE CASCADE,
    pedido_id          BIGINT NOT NULL REFERENCES pedidos_compra(id) ON DELETE CASCADE,
    criado_em          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Previsão de pagamento
--
-- Não é título: é a obrigação que NASCE com a autorização do pedido e que vira
-- título quando a nota fiscal chegar (fase 5). Guardar separado deixa claro o
-- que já é dívida documentada e o que ainda é compromisso assumido.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS previsoes_pagamento (
    id            BIGSERIAL PRIMARY KEY,
    pedido_id     BIGINT NOT NULL REFERENCES pedidos_compra(id) ON DELETE CASCADE,
    numero        INT    NOT NULL,
    vencimento    DATE   NOT NULL,
    valor         NUMERIC(14,2) NOT NULL,
    entrada       BOOLEAN NOT NULL DEFAULT FALSE,
    titulo_id     BIGINT REFERENCES titulos(id),
    criado_em     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (pedido_id, numero),
    CONSTRAINT ck_previsao_valor CHECK (valor > 0)
);

CREATE INDEX IF NOT EXISTS ix_previsoes_vencimento
    ON previsoes_pagamento (vencimento) WHERE titulo_id IS NULL;

CREATE SEQUENCE IF NOT EXISTS seq_pedido_compra START 1;
