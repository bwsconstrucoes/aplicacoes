-- ===========================================================================
-- 040 — O REGISTRO DO QUE FOI GRAVADO NO MOBPONTO POR AQUI
--
-- Pedido do dono em 01/10/2026: corrigir o ponto a partir da janela do
-- funcionário na folha — "altera a obra ou adiciona uma obra que não existia,
-- salva e grava as alterações. Assim fica rápido de corrigir as possíveis
-- distorções do ponto."
--
-- Gravar no Mobponto é escrever em sistema de terceiro, e não se desfaz por
-- aqui. Cada batida enviada fica registrada: quem, quando, para quem, o que se
-- mandou e o que o Mobponto respondeu — inclusive quando recusou.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS analisesps.ponto_batida_enviada (
    id            SERIAL PRIMARY KEY,
    cpf           VARCHAR(11)  NOT NULL,
    nome          VARCHAR(160) NOT NULL DEFAULT '',
    data          DATE         NOT NULL,
    hora          VARCHAR(5)   NOT NULL,
    obra          VARCHAR(120) NOT NULL,
    justificativa VARCHAR(500) NOT NULL DEFAULT '',
    ok            BOOLEAN      NOT NULL DEFAULT FALSE,
    resposta      VARCHAR(1000) NOT NULL DEFAULT '',
    enviado_por   VARCHAR(120) NOT NULL DEFAULT '',
    enviado_em    TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_ponto_batida_enviada_cpf
    ON analisesps.ponto_batida_enviada (cpf, data);
