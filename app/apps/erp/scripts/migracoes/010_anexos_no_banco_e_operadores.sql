-- ============================================================================
-- Migração 010 — anexos dentro do banco e cadastro de operadores
--
-- (a) ANEXOS: o conteúdo passa a viver no PostgreSQL (bytea), sem Dropbox. O
--     sistema é independente. Antes de gravar, o arquivo é comprimido:
--     imagem redimensionada e recomprimida, PDF passado por limpeza —
--     economia de espaço sem perder legibilidade do documento.
-- (b) OPERADORES: perfis novos e escopo por obra. Administrativo de obra vê o
--     que ele mesmo lançou; supervisor vê as obras designadas; gestor vê todas
--     as obras; financeiro opera tudo menos configuração; admin faz tudo.
-- ============================================================================

ALTER TABLE anexos
    ADD COLUMN IF NOT EXISTS conteudo        BYTEA,
    ADD COLUMN IF NOT EXISTS mime_type       TEXT,
    ADD COLUMN IF NOT EXISTS tamanho_original BIGINT,
    ADD COLUMN IF NOT EXISTS comprimido      BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS descricao       TEXT,
    ADD COLUMN IF NOT EXISTS categoria_anexo TEXT;   -- COMPROVANTE|NOTA|CONTRATO|ART|SEGURO|OS|OUTRO

ALTER TABLE anexos ALTER COLUMN dropbox_path DROP NOT NULL;

-- perfis novos
ALTER TYPE perfil_usuario ADD VALUE IF NOT EXISTS 'ADMINISTRATIVO_OBRA';
ALTER TYPE perfil_usuario ADD VALUE IF NOT EXISTS 'SUPERVISOR_OBRA';
ALTER TYPE perfil_usuario ADD VALUE IF NOT EXISTS 'GESTOR_OBRA';

ALTER TABLE usuarios
    ADD COLUMN IF NOT EXISTS cpf       TEXT,
    ADD COLUMN IF NOT EXISTS telefone  TEXT,
    ADD COLUMN IF NOT EXISTS observacoes TEXT;

-- obras que o supervisor enxerga
CREATE TABLE IF NOT EXISTS usuario_obras (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    usuario_id  BIGINT NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    obra_id     BIGINT NOT NULL REFERENCES obras(id),
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_usuario_obra UNIQUE (usuario_id, obra_id)
);

CREATE INDEX IF NOT EXISTS idx_usuario_obras ON usuario_obras (usuario_id);
