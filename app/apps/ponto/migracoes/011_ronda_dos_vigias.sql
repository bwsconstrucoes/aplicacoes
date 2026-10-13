-- ===========================================================================
-- 011 — A RONDA SURPRESA DOS VIGIAS NOTURNOS
--
-- Pedido do dono, 13/10/2026: "os vigias batem dois pontos, a entrada e a saída.
-- Porém nada garante que esse pessoal fique na obra o tempo todo. (…) Vamos fazer
-- a ronda surpresa pelo WhatsApp: ele vai bater a entrada, a saída e fazer quatro
-- rondas surpresa. Só para os vigias noturnos, vinculado ao cadastro dele."
--
-- A RONDA NÃO É BATIDA DE PONTO: não entra em ponto.marcacoes (nem no NSR, nem
-- no cálculo da jornada — a Portaria 671 é da jornada). É um registro de
-- presença, com foto, localização e hora, numa tabela à parte.
--
-- As rondas de um turno nascem na ENTRADA do vigia (sem entrada — folga ou falta
-- —, não há ronda) e as que ainda não saíram morrem na SAÍDA.
-- ===========================================================================

ALTER TABLE ponto.colaborador_config
    ADD COLUMN IF NOT EXISTS faz_ronda BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS ponto.rondas (
    id                BIGSERIAL   PRIMARY KEY,
    colaborador_id    BIGINT      NOT NULL REFERENCES public.colaboradores (id) ON DELETE CASCADE,
    entrada_id        BIGINT      NOT NULL REFERENCES ponto.marcacoes (id),   -- a batida que abriu o turno
    data_referencia   DATE        NOT NULL,                                   -- o dia da entrada
    obra_id           BIGINT      REFERENCES public.obras (id),
    numero            SMALLINT    NOT NULL,                                   -- 1ª, 2ª… do turno
    agendada_para     TIMESTAMPTZ NOT NULL,
    status            TEXT        NOT NULL DEFAULT 'AGENDADA'
                      CHECK (status IN ('AGENDADA', 'ENVIADA', 'FEITA', 'ATRASADA', 'PERDIDA',
                                        'CANCELADA', 'NAO_ENVIADA')),
    enviada_em        TIMESTAMPTZ,
    prazo_ate         TIMESTAMPTZ,
    token_hash        TEXT,                                                   -- o link do WhatsApp
    feita_em          TIMESTAMPTZ,
    latitude          DOUBLE PRECISION,
    longitude         DOUBLE PRECISION,
    precisao_metros   DOUBLE PRECISION,
    distancia_metros  DOUBLE PRECISION,
    dentro_da_cerca   BOOLEAN,
    foto_id           BIGINT      REFERENCES ponto.fotos (id),
    motivo            TEXT,                                                   -- por que cancelou, não saiu…
    avisado_em        TIMESTAMPTZ,                                            -- o aviso da ronda perdida
    criado_em         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_rondas_fila ON ponto.rondas (status, agendada_para);
CREATE INDEX IF NOT EXISTS ix_rondas_pessoa ON ponto.rondas (colaborador_id, data_referencia);
CREATE UNIQUE INDEX IF NOT EXISTS ux_rondas_token ON ponto.rondas (token_hash) WHERE token_hash IS NOT NULL;
