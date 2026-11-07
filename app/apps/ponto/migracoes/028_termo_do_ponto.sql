-- ===========================================================================
-- 028 — O TERMO DO PONTO ELETRÔNICO, ASSINADO NO CELULAR
--
-- O dono, 07/11/2026: "para fechar a formalização do aplicativo em relação aos funcionários (…) coletar a assinatura
-- deles (…) das pessoas que já estão admitidas e das que ainda vão ser admitidas. Toda vida que entrar tem que gerar
-- alguma pendência (…) o DP também tem que visualizar (…) o mais curto e simples possível (…) o ideal é que eles
-- assinem no próprio celular deles, mas quem não conseguir também poder ser disponibilizado pelo pessoal da obra".
--
-- Uma linha por assinatura. Vale a mais nova que não foi substituída; o texto do termo tem um LACRE (o SHA-256 do
-- texto): se o termo mudar, quem assinou o antigo volta a ter a pendência, e a assinatura antiga fica guardada.
--   forma      PELA_PESSOA (ela entrou com CPF e PIN e assinou) ou CONDUZIDA (o operador da obra mostrou o termo)
--   situacao   ASSINADO, RECUSADO ou SUBSTITUIDO
-- A prova de quem assinou: o PIN da pessoa no momento OU, para quem não tem PIN, a foto dela tirada na hora; mais a
-- hora do servidor, o aparelho, o local, quem conduziu e o lacre do texto.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS ponto.termo_aceites (
    id                  BIGSERIAL PRIMARY KEY,
    colaborador_id      BIGINT      NOT NULL REFERENCES public.colaboradores (id),
    versao              TEXT        NOT NULL,
    lacre               TEXT        NOT NULL,                -- SHA-256 do texto do termo mostrado
    situacao            TEXT        NOT NULL CHECK (situacao IN ('ASSINADO', 'RECUSADO', 'SUBSTITUIDO')),
    forma               TEXT        NOT NULL CHECK (forma IN ('PELA_PESSOA', 'CONDUZIDA')),
    com_pin             BOOLEAN     NOT NULL DEFAULT FALSE,  -- a pessoa digitou o PIN dela
    assinatura_png      BYTEA,                               -- o traço do dedo (vazio na recusa)
    foto_jpeg           BYTEA,                               -- a foto da pessoa na hora (quem não tem PIN)
    documento_id        BIGINT      REFERENCES ponto.documentos (id),    -- o PDF com o termo e o comprovante
    motivo_recusa       TEXT,
    conduzida_por_id    BIGINT,
    conduzida_por_nome  TEXT        NOT NULL DEFAULT '',
    dispositivo_id      BIGINT,
    latitude            NUMERIC(10, 7),
    longitude           NUMERIC(10, 7),
    ip                  TEXT,
    criado_em           TIMESTAMPTZ NOT NULL DEFAULT now(),
    substituido_em      TIMESTAMPTZ,
    substituido_motivo  TEXT
);

-- Só uma valendo por pessoa; as substituídas ficam guardadas.
CREATE UNIQUE INDEX IF NOT EXISTS ux_ponto_termo_valendo
    ON ponto.termo_aceites (colaborador_id) WHERE situacao <> 'SUBSTITUIDO';
