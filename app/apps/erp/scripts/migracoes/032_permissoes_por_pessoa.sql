-- 032 — Permissão fina: exceções por pessoa, sobre o cargo
--
-- O cargo (perfil) continua sendo a base: é ele que responde por tudo que já
-- está no ar. Esta tabela guarda apenas as EXCEÇÕES marcadas no cadastro da
-- pessoa: "além do cargo, esta também autoriza pedido" (concedida = TRUE) ou
-- "apesar do cargo, esta não paga" (concedida = FALSE).
--
-- Sem linha aqui, vale exatamente o cargo — então a tabela nascer vazia não
-- muda o comportamento de ninguém.

CREATE TABLE IF NOT EXISTS usuario_permissoes (
    usuario_id   BIGINT      NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    acao         TEXT        NOT NULL,
    concedida    BOOLEAN     NOT NULL,
    definida_em  TIMESTAMPTZ NOT NULL DEFAULT now(),
    definida_por BIGINT      REFERENCES usuarios(id),
    PRIMARY KEY (usuario_id, acao)
);

CREATE INDEX IF NOT EXISTS ix_usuario_permissoes_usuario
    ON usuario_permissoes (usuario_id);
