-- 125 — O PEDIDO DE MATERIAL NÃO ESPERA MAIS O CADASTRO DO INSUMO
--
-- O dono, em 23/11/2026: "quando não tem um insumo, quem vai lançar o pedido
-- fica meio travado. Ele faz o pleito do novo insumo, para depois esse insumo
-- ser cadastrado, para então ser autorizado, aí ele é informado e então lança
-- o pedido completo." O que ele pediu: "ele faz o pedido já incluindo aquele
-- cadastro de insumo (...) aquele pedido fica pendente, aguardando a
-- confirmação do cadastro. Na hora que liberar, o pedido já fica pronto."
--
-- Cada linha é um material pedido cujo insumo ainda não existe. Quando o
-- administrador cadastra o insumo, a linha vira item da solicitação sozinha.
-- Tabela à parte (e não coluna em `suprimento_itens`) porque o item exige um
-- insumo do cadastro — e aqui ele ainda não existe.
CREATE TABLE IF NOT EXISTS suprimento_itens_esperando_insumo (
    id                    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    insumo_solicitacao_id BIGINT NOT NULL REFERENCES insumo_solicitacoes(id) ON DELETE CASCADE,
    solicitacao_id        BIGINT NOT NULL REFERENCES suprimento_solicitacoes(id) ON DELETE CASCADE,
    obra_id               BIGINT NOT NULL REFERENCES obras(id),
    quantidade            NUMERIC(14,3) NOT NULL CHECK (quantidade > 0),
    unidade               TEXT NOT NULL REFERENCES unidades_compra(codigo),
    especificacao         TEXT,
    criado_por            BIGINT NOT NULL REFERENCES usuarios(id),
    criado_em             TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- preenchido quando o insumo é cadastrado e a linha vira item de verdade
    item_id               BIGINT REFERENCES suprimento_itens(id) ON DELETE SET NULL,
    -- preenchido quando o cadastro do insumo é recusado
    recusado_motivo       TEXT
);
CREATE INDEX IF NOT EXISTS ix_itens_esperando_insumo_pendentes
    ON suprimento_itens_esperando_insumo (insumo_solicitacao_id)
    WHERE item_id IS NULL AND recusado_motivo IS NULL;
