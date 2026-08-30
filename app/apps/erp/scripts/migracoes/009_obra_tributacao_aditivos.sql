-- ============================================================================
-- Migração 009 — cadastro completo da obra, tributação e aditivos
--
-- A obra passa a carregar o que a emissão de nota precisa (alíquota de ISS,
-- se o município aceita dedução de material, base do INSS, quais federais são
-- retidos) e o que o acompanhamento de contrato exige (valor, vigência,
-- data-base do reajuste, conta de recebimento, ART/RRT, CNO).
-- Aditivos ficam em tabela própria: valor e prazo se somam ao contrato e o
-- histórico de cada alteração é preservado.
-- ============================================================================

ALTER TABLE obras
    -- identificação e local (usado também como endereço de entrega nas compras)
    ADD COLUMN IF NOT EXISTS cep                  TEXT,
    ADD COLUMN IF NOT EXISTS bairro               TEXT,
    ADD COLUMN IF NOT EXISTS numero_endereco      TEXT,
    ADD COLUMN IF NOT EXISTS complemento          TEXT,
    ADD COLUMN IF NOT EXISTS codigo_ibge          TEXT,
    ADD COLUMN IF NOT EXISTS responsavel_tecnico  TEXT,
    ADD COLUMN IF NOT EXISTS art_rrt              TEXT,
    ADD COLUMN IF NOT EXISTS engenheiro_fiscal    TEXT,
    -- contrato
    ADD COLUMN IF NOT EXISTS vigencia_inicio      DATE,
    ADD COLUMN IF NOT EXISTS vigencia_fim         DATE,
    ADD COLUMN IF NOT EXISTS prazo_execucao_dias  INTEGER,
    ADD COLUMN IF NOT EXISTS data_base_orcamento  DATE,
    ADD COLUMN IF NOT EXISTS indice_reajuste      TEXT,
    ADD COLUMN IF NOT EXISTS conta_recebimento_id BIGINT REFERENCES contas_bancarias(id),
    ADD COLUMN IF NOT EXISTS ordem_servico        TEXT,
    ADD COLUMN IF NOT EXISTS data_ordem_servico   DATE,
    -- tributação da nota (espelha o que o emissaonf usa)
    ADD COLUMN IF NOT EXISTS aliquota_iss_pct     NUMERIC(6,4),
    ADD COLUMN IF NOT EXISTS iss_retido           BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS aceita_deducao_material BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS pct_servico_iss      NUMERIC(6,2),   -- ex.: 60 (60/40)
    ADD COLUMN IF NOT EXISTS pct_servico_inss     NUMERIC(6,2),   -- ex.: 50 (11% sobre 50%)
    ADD COLUMN IF NOT EXISTS inss_retido          BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS federais_retidos     TEXT[] NOT NULL DEFAULT '{}',  -- IR,PIS,COFINS,CSLL
    ADD COLUMN IF NOT EXISTS regime_obra          TEXT,           -- ONERADA / DESONERADA
    ADD COLUMN IF NOT EXISTS observacoes_fiscais  TEXT;

CREATE TABLE IF NOT EXISTS obra_aditivos (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    obra_id         BIGINT      NOT NULL REFERENCES obras(id),
    numero          TEXT        NOT NULL,
    tipo            TEXT        NOT NULL,          -- VALOR | PRAZO | VALOR_E_PRAZO | REAJUSTE | SUPRESSAO
    valor           NUMERIC(14,2) NOT NULL DEFAULT 0,   -- positivo acresce, negativo suprime
    dias            INTEGER     NOT NULL DEFAULT 0,
    nova_vigencia_fim DATE,
    data_assinatura DATE,
    objeto          TEXT,
    anexo_id        BIGINT      REFERENCES anexos(id),
    criado_por      BIGINT      REFERENCES usuarios(id),
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_aditivo UNIQUE (obra_id, numero)
);

CREATE INDEX IF NOT EXISTS idx_aditivos_obra ON obra_aditivos (obra_id);
