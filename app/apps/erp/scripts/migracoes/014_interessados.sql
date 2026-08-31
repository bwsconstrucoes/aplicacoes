-- ============================================================================
-- Migração 014 — interessados no título
-- Além de quem lançou, outras pessoas podem acompanhar o título e receber os
-- avisos: o supervisor da obra, o engenheiro que pediu o material, o
-- almoxarife. Cada interessado é um operador do sistema (para receber, ele
-- precisa estar cadastrado no bot do Telegram).
-- ============================================================================
CREATE TABLE IF NOT EXISTS titulo_interessados (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    titulo_id    BIGINT NOT NULL REFERENCES titulos(id) ON DELETE CASCADE,
    usuario_id   BIGINT NOT NULL REFERENCES usuarios(id),
    motivo       TEXT,
    adicionado_por BIGINT REFERENCES usuarios(id),
    criado_em    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_titulo_interessado UNIQUE (titulo_id, usuario_id)
);

CREATE INDEX IF NOT EXISTS idx_interessados_titulo ON titulo_interessados (titulo_id);
CREATE INDEX IF NOT EXISTS idx_interessados_usuario ON titulo_interessados (usuario_id);

-- interessados fixos por obra: quem entra automaticamente em todo título dela
CREATE TABLE IF NOT EXISTS obra_interessados (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    obra_id      BIGINT NOT NULL REFERENCES obras(id) ON DELETE CASCADE,
    usuario_id   BIGINT NOT NULL REFERENCES usuarios(id),
    criado_em    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_obra_interessado UNIQUE (obra_id, usuario_id)
);
