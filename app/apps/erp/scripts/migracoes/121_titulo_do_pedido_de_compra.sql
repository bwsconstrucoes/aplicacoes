-- ===========================================================================
-- O LANÇAMENTO FINANCEIRO NASCE DO RECEBIMENTO (20/11/2026)
--
-- Ele: *"eu quero gerar o financeiro a partir do recebimento (…) que
-- atualmente é assim que funciona. A pessoa vai receber o pedido e lança
-- nota, lança boleto, lança tudo e gera a obrigação financeira. Ou confirma
-- uma previsão que já existe."*
--
-- Até aqui o pedido só chegava ao financeiro por um caminho comprido: alguém
-- vinculava a nota ao pedido na tela de Notas e outra pessoa lançava o título
-- com aquela nota. Título lançado sem esse vínculo não era enxergado — o
-- painel continuava dizendo "falta a nota".
--
-- Esta tabela é o vínculo DIRETO: este título é deste pedido de compra (e, se
-- nasceu de um recebimento, de qual). Tabela própria, e não coluna em
-- `titulos`: o modelo do título é carregado em quase toda tela do Financeiro,
-- e coluna nova nele derrubaria o Financeiro inteiro se o código subisse antes
-- de esta atualização ser aplicada.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS pedido_titulos (
    titulo_id      BIGINT PRIMARY KEY REFERENCES titulos(id) ON DELETE CASCADE,
    pedido_id      BIGINT NOT NULL REFERENCES pedidos_compra(id) ON DELETE CASCADE,
    recebimento_id BIGINT REFERENCES recebimentos(id) ON DELETE SET NULL,
    criado_por     BIGINT REFERENCES usuarios(id),
    criado_em      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_pedido_titulos_pedido ON pedido_titulos (pedido_id);
