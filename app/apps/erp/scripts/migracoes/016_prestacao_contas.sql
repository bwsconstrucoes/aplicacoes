-- ============================================================================
-- Migração 016 — prestação de contas (fundo fixo) e fatura de cartão
--
-- Fundo fixo e cartão não são um lançamento: são MUITOS lançamentos pequenos
-- dentro de um título só. Por isso ganham ITENS, cada um com sua data,
-- descrição, valor, obra e (no cartão) categoria, e cada um com o seu
-- comprovante anexado.
--
-- Fundo fixo tem duas naturezas: ADIANTAMENTO (a empresa adiantou e a pessoa
-- presta contas) e REEMBOLSO (a pessoa gastou do bolso e pede de volta).
--
-- As CRÍTICAS de cada item ficam gravadas, e a confirmação de quem analisou
-- também — indício não some da tela sem alguém assumir que olhou.
-- ============================================================================

ALTER TABLE titulos
    ADD COLUMN IF NOT EXISTS modalidade      TEXT NOT NULL DEFAULT 'NORMAL',
        -- NORMAL | FUNDO_FIXO | CARTAO
    ADD COLUMN IF NOT EXISTS fundo_fixo_tipo TEXT,
        -- ADIANTAMENTO | REEMBOLSO
    ADD COLUMN IF NOT EXISTS adiantamento_titulo_id BIGINT REFERENCES titulos(id),
    ADD COLUMN IF NOT EXISTS periodo_prestacao_inicio DATE,
    ADD COLUMN IF NOT EXISTS periodo_prestacao_fim    DATE,
    ADD COLUMN IF NOT EXISTS alertas_confirmados JSONB NOT NULL DEFAULT '[]'::jsonb;

CREATE INDEX IF NOT EXISTS idx_titulos_modalidade ON titulos (modalidade, solicitante_id);

CREATE TABLE IF NOT EXISTS titulo_itens (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    titulo_id      BIGINT      NOT NULL REFERENCES titulos(id) ON DELETE CASCADE,
    ordem          INTEGER     NOT NULL DEFAULT 0,
    data_despesa   DATE,
    descricao      TEXT        NOT NULL,
    estabelecimento TEXT,
    documento      TEXT,                       -- nº do cupom/nota, quando houver
    valor          NUMERIC(14,2) NOT NULL CHECK (valor > 0),
    obra_id        BIGINT      REFERENCES obras(id),
    categoria_id   BIGINT      REFERENCES categorias(id),
    anexo_id       BIGINT      REFERENCES anexos(id),
    origem_leitura TEXT,                       -- IA | MANUAL | FATURA
    confianca      TEXT,
    criticas       JSONB       NOT NULL DEFAULT '[]'::jsonb,
    conferido_por  BIGINT      REFERENCES usuarios(id),
    conferido_em   TIMESTAMPTZ,
    observacao     TEXT,
    criado_em      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_itens_titulo ON titulo_itens (titulo_id, ordem);
CREATE INDEX IF NOT EXISTS idx_itens_obra ON titulo_itens (obra_id);
