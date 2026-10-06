-- ===========================================================================
-- 007 — A CONFERÊNCIA DO ROSTO (Amazon Rekognition)
--
-- Pedido do dono, 06/10/2026: "acho que devemos ativar a análise via AWS. Hoje
-- pago 2 mil reais pelo ponto que usamos (…) devemos permitir colocar padrão
-- para tudo, excluir alguma obra ou algum horário ou dia caso entenda que não
-- precise ser 100% das fotos."
--
-- Uma linha por batida conferida: o resultado, a semelhança com a foto
-- cadastral e o custo. A configuração (ligada, teto do mês, exclusões) fica em
-- `ponto.parametros` (chave rosto.config).
-- ===========================================================================
CREATE TABLE IF NOT EXISTS ponto.conferencias_rosto (
    marcacao_id     BIGINT      PRIMARY KEY REFERENCES ponto.marcacoes (id) ON DELETE CASCADE,
    resultado       TEXT        NOT NULL
                                CHECK (resultado IN ('MESMA_PESSOA', 'OUTRA_PESSOA', 'SEM_ROSTO',
                                                     'SEM_CADASTRAL', 'VIROU_CADASTRAL', 'ERRO')),
    semelhanca      NUMERIC(5, 2),
    custo_usd       NUMERIC(8, 4) NOT NULL DEFAULT 0,
    detalhe         TEXT,
    conferido_em    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_ponto_conferencias_rosto_mes ON ponto.conferencias_rosto (conferido_em);
