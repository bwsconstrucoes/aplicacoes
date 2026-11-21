-- ===========================================================================
-- MATERIAL COMPRADO SEM PASSAR PELO SUPRIMENTO (21/11/2026)
--
-- Ele: *"a gente tem que tentar inibir a possibilidade de uma compra sem ter
-- seguido o processo do suprimento. Imagina um engenheiro da obra que foi lá e
-- comprou um material (…) quando fosse anexar uma nota e identificar que não
-- identificou um pedido — por que estão lançando isso? (…) alguém autorizar
-- esse lançamento para o financeiro."*
--
-- Uma linha aqui é um lançamento de MATERIAL sem pedido de compra: quem
-- lançou escreveu o motivo, e o título fica bloqueado até alguém com alçada
-- autorizar. A regra só vale com a chave ligada em `parametros`
-- (`suprimentos_exigir_pedido_desde`) — desligada, esta tabela fica vazia.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS compras_fora_do_fluxo (
    titulo_id     BIGINT PRIMARY KEY REFERENCES titulos(id) ON DELETE CASCADE,
    motivo        TEXT NOT NULL,
    lancado_por   BIGINT REFERENCES usuarios(id),
    criado_em     TIMESTAMPTZ NOT NULL DEFAULT now(),
    autorizado_por BIGINT REFERENCES usuarios(id),
    autorizado_em TIMESTAMPTZ,
    observacao    TEXT
);
CREATE INDEX IF NOT EXISTS ix_compras_fora_do_fluxo_pendentes
    ON compras_fora_do_fluxo (criado_em) WHERE autorizado_em IS NULL;
