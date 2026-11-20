-- ===========================================================================
-- ONDE O MATERIAL DESCE, QUANDO NÃO É NA OBRA (20/11/2026)
--
-- Ele: *"precisamos prever também quando a entrega não for na obra, por
-- exemplo na sede. Isso acho que pode ser definido no fechamento. A princípio
-- a entrega é na obra associada ao pedido."*
--
-- Uma linha AQUI é a exceção: o material da obra X, neste pedido, desce em
-- outro lugar. Sem linha, vale a regra — o endereço da obra.
--
-- Tabela própria, e não coluna em `pedido_itens`, de propósito: `pedido_itens`
-- é lida pela tela de Notas do Financeiro (o cruzamento nota × pedido), e
-- coluna nova nela derrubaria aquela tela se o código subisse antes de esta
-- atualização ser aplicada.
--
-- O endereço é guardado POR EXTENSO (e não um ponteiro para a empresa): o
-- pedido é documento mandado ao fornecedor, e não pode mudar de endereço
-- sozinho se alguém corrigir o cadastro da sede meses depois.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS pedido_entregas (
    pedido_id   BIGINT NOT NULL REFERENCES pedidos_compra(id) ON DELETE CASCADE,
    obra_id     BIGINT NOT NULL REFERENCES obras(id),
    -- SEDE (endereço da empresa da obra) ou OUTRO (digitado no fechamento).
    tipo        TEXT NOT NULL CHECK (tipo IN ('SEDE', 'OUTRO')),
    rotulo      TEXT NOT NULL,
    endereco    TEXT NOT NULL,
    definido_por BIGINT REFERENCES usuarios(id),
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pedido_id, obra_id)
);

-- O painel do coordenador e a projeção de pagamentos perguntam "o que é deste
-- pedido?" a várias tabelas. Sem estes índices, cada pergunta lê a tabela
-- inteira — e a numeração antiga já passa de 19 mil pedidos.
CREATE INDEX IF NOT EXISTS ix_pedido_itens_pedido ON pedido_itens (pedido_id);
CREATE INDEX IF NOT EXISTS ix_previsoes_pagamento_pedido ON previsoes_pagamento (pedido_id);
CREATE INDEX IF NOT EXISTS ix_recebimento_itens_recebimento ON recebimento_itens (recebimento_id);
CREATE INDEX IF NOT EXISTS ix_titulos_documento_fiscal
    ON titulos (documento_fiscal_id) WHERE documento_fiscal_id IS NOT NULL;
