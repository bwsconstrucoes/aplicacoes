-- ===========================================================================
-- 027 — A ASSINATURA DA FOLHA DE PONTO NA TELA DO CELULAR
--
-- O dono, 06/11/2026: "o operador do ponto, com o celular, vai mostrar para a pessoa a folha de ponto, e a pessoa vai
-- assinar (…) essa assinatura vai ser arquivada (…) para a gente imprimir posteriormente". Prazo: até o dia 20 do mês
-- seguinte; quem conduz: o operador do ponto da obra ou o administrativo (normalmente a mesma pessoa).
--
-- Uma linha por pessoa e mês: ASSINADA (com a assinatura desenhada, o PDF arquivado e o LACRE — o SHA-256 da folha
-- como foi mostrada), RECUSADA (a pessoa não quis assinar: o motivo e quem conduziu) ou SUBSTITUIDA (o mês foi
-- reaberto e a folha mudou: a assinatura antiga fica guardada, e a folha volta a pedir assinatura).
-- A prova de quem assinou: o PIN da própria pessoa no momento, a hora do servidor, o aparelho, o local e o lacre.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS ponto.espelho_assinaturas (
    id                  BIGSERIAL PRIMARY KEY,
    colaborador_id      BIGINT      NOT NULL REFERENCES public.colaboradores (id),
    competencia         DATE        NOT NULL,                -- o 1º dia do mês da folha
    situacao            TEXT        NOT NULL CHECK (situacao IN ('ASSINADA', 'RECUSADA', 'SUBSTITUIDA')),
    lacre               TEXT        NOT NULL,                -- SHA-256 da folha mostrada
    assinatura_png      BYTEA,                               -- o traço do dedo (vazio na recusa)
    documento_id        BIGINT      REFERENCES ponto.documentos (id),    -- o PDF com o comprovante
    motivo_recusa       TEXT,
    conduzida_por_id    BIGINT,                              -- o colaborador (operador/administrativo) que mostrou
    conduzida_por_nome  TEXT        NOT NULL DEFAULT '',
    dispositivo_id      BIGINT,
    latitude            NUMERIC(10, 7),
    longitude           NUMERIC(10, 7),
    ip                  TEXT,
    criado_em           TIMESTAMPTZ NOT NULL DEFAULT now(),
    substituida_em      TIMESTAMPTZ,
    substituida_motivo  TEXT
);

-- Só uma valendo (assinada ou recusada) por pessoa e mês; as substituídas ficam guardadas.
CREATE UNIQUE INDEX IF NOT EXISTS ux_ponto_espelho_assinatura_valendo
    ON ponto.espelho_assinaturas (colaborador_id, competencia) WHERE situacao <> 'SUBSTITUIDA';
CREATE INDEX IF NOT EXISTS ix_ponto_espelho_assinaturas_mes ON ponto.espelho_assinaturas (competencia);
