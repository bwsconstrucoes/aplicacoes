-- ===========================================================================
-- 019 — A HORA EXTRA NEGADA NA CONFERÊNCIA
--
-- Pedido do dono, 29/10/2026, olhando as horas extras por obra: "eu preciso poder, por exemplo,
-- discordar com alguma hora — eu queria poder selecionar e negar que aquela hora daquele dia
-- entre no cálculo."
--
-- As batidas NÃO mudam (Portaria 671): o que se guarda é a decisão de que a hora extra daquele dia
-- não entra na conta — com o motivo, quem negou e quando. Voltar a contar fica registrado também
-- (a linha não se apaga: `desfeita_em`). O espelho zera o extra do dia e mostra "hora extra negada".
-- ===========================================================================

CREATE TABLE IF NOT EXISTS ponto.horas_extras_negadas (
    id              BIGSERIAL PRIMARY KEY,
    colaborador_id  BIGINT      NOT NULL,
    data            DATE        NOT NULL,
    minutos         INTEGER     NOT NULL DEFAULT 0,     -- o extra do dia quando foi negado
    motivo          TEXT        NOT NULL,
    negada_por      TEXT        NOT NULL,
    negada_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    desfeita_por    TEXT,
    desfeita_em     TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_ponto_he_negada_ativa
    ON ponto.horas_extras_negadas (colaborador_id, data) WHERE desfeita_em IS NULL;
