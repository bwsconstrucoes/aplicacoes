-- ---------------------------------------------------------------------------
-- 097 — OS EQUIPAMENTOS DO DIÁRIO VIRAM LISTA
--
-- Até aqui "equipamentos" era UMA LINHA DE TEXTO em diarios_obra. Funciona
-- para escrever, não para ler: ninguém soma "1 retroescavadeira, 2 betoneira
-- e uma serra" de trinta dias para saber quanto tempo o equipamento ficou na
-- obra — e é exatamente essa conta que sustenta pleito de prazo e medição de
-- mobilização.
--
-- Mesma decisão que já havia sido tomada para o efetivo terceirizado
-- (diario_efetivo_extra, migração 092): descrição + quantidade, uma linha por
-- equipamento. Assim o equipamento conta, repete do dia anterior sozinho
-- (retroescavadeira não vai embora à noite) e entra no resumo.
--
-- A COLUNA DE TEXTO NÃO É APAGADA. Diário é documento: o que já foi escrito
-- e assinado continua escrito, e o papel continua imprimindo o texto quando
-- não houver lista. Quem apagasse a coluna reescreveria o passado.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS diario_equipamentos (
    id              BIGSERIAL PRIMARY KEY,
    diario_id       BIGINT      NOT NULL REFERENCES diarios_obra(id) ON DELETE CASCADE,
    descricao       TEXT        NOT NULL,   -- "Retroescavadeira"
    quantidade      INTEGER     NOT NULL DEFAULT 1,
    observacao      TEXT,                   -- "parada por manutenção"
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT diario_equip_quantidade_positiva CHECK (quantidade > 0)
);

CREATE INDEX IF NOT EXISTS idx_diario_equipamentos
    ON diario_equipamentos (diario_id);
