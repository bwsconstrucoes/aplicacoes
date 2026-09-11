-- 037 — Suprimentos, fase 5: recebimento na obra e pendência por saldo
--
-- Quem confere é a OBRA, não o suprimento: o colaborador acha o pedido do
-- material que chegou e registra o que veio.
--
-- A PENDÊNCIA É O SALDO DO PRÓPRIO ITEM, não um registro novo em outra tabela.
-- O item já tem quantidade pedida e quantidade recebida; o que sobra continua
-- vivo e pode entrar numa cotação nova sem perder o vínculo com a origem.
-- É o mesmo padrão que já funciona na medição de empreita.

CREATE TABLE IF NOT EXISTS recebimentos (
    id           BIGSERIAL PRIMARY KEY,
    pedido_id    BIGINT NOT NULL REFERENCES pedidos_compra(id),
    obra_id      BIGINT REFERENCES obras(id),
    data         DATE   NOT NULL DEFAULT CURRENT_DATE,
    nota_numero  TEXT,
    anexo_id     BIGINT REFERENCES anexos(id),
    observacoes  TEXT,
    recebido_por BIGINT NOT NULL REFERENCES usuarios(id),
    criado_em    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_recebimentos_pedido ON recebimentos (pedido_id);

CREATE TABLE IF NOT EXISTS recebimento_itens (
    id              BIGSERIAL PRIMARY KEY,
    recebimento_id  BIGINT NOT NULL REFERENCES recebimentos(id) ON DELETE CASCADE,
    pedido_item_id  BIGINT NOT NULL REFERENCES pedido_itens(id),
    quantidade      NUMERIC(14,3) NOT NULL,
    UNIQUE (recebimento_id, pedido_item_id),
    CONSTRAINT ck_recebimento_item_qtd CHECK (quantidade > 0)
);
