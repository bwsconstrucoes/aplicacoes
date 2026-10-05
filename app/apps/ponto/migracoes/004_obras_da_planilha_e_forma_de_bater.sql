-- ===========================================================================
-- 004 — AS OBRAS DA PLANILHA "C. DIÁRIOS" E A FORMA DE BATER POR PESSOA
--
-- Pedido do dono, 05/10/2026:
--   · "Quero utilizar temporariamente as obras de C. Diários. Depois vamos usar
--     o cadastro do ERP." O status está na coluna V; as coordenadas, na coluna
--     AM ("Coordenadas Geográficas"). Não aparecem as obras "Concluída",
--     "Concluída com Dívida" e "Distratada".
--   · "Em relação ao padrão de batida é só o aparelho da obra que bate. Vamos
--     cadastrar apenas as exceções."
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- A cópia da aba "C. Diários" (planilha "Bases de Dados Pipefy"), refeita a
-- cada leitura. É CÓPIA: quem manda é a planilha. `obra_id` é a linha de
-- `public.obras` a que a obra da planilha corresponde (pelo código primário ou
-- pelo da coluna A) — é nela que batidas, cercas e aparelhos se penduram.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ponto.obras_planilha (
    codigo               TEXT        PRIMARY KEY,
    codigo_secundario    TEXT,
    nome                 TEXT        NOT NULL,
    municipio            TEXT,
    status_planilha      TEXT,
    encerrada            BOOLEAN     NOT NULL DEFAULT FALSE,
    latitude             NUMERIC(9, 6),
    longitude            NUMERIC(9, 6),
    coordenada_texto     TEXT,
    coordenada_problema  TEXT,
    coordenada_aviso     TEXT,
    obra_id              BIGINT      REFERENCES public.obras (id) ON DELETE SET NULL,
    linha                INTEGER,
    lida_em              TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS obras_planilha_obra_unica
    ON ponto.obras_planilha (obra_id) WHERE obra_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- A forma de bater. O PADRÃO É FALSO: só o aparelho da obra bate. Marcado, a
-- pessoa também bate no próprio celular (depois de o celular ser aprovado).
--
-- Quem JÁ tem celular próprio aprovado continua batendo nele: vira exceção
-- aqui, para a migração não tirar de ninguém o que funcionava ontem.
-- ---------------------------------------------------------------------------
ALTER TABLE ponto.colaborador_config
    ADD COLUMN IF NOT EXISTS bate_no_celular BOOLEAN NOT NULL DEFAULT FALSE;

INSERT INTO ponto.colaborador_config (colaborador_id, bate_no_celular)
SELECT DISTINCT d.colaborador_id, TRUE
  FROM ponto.dispositivos d
 WHERE d.perfil = 'INDIVIDUAL' AND d.status = 'APROVADO' AND d.colaborador_id IS NOT NULL
ON CONFLICT (colaborador_id) DO UPDATE SET bate_no_celular = TRUE;
