-- ===========================================================================
-- 023 — O ALCANCE LIBERADO PELO DP NO APLICATIVO
--
-- O dono, 31/10/2026: "tem como eu liberar pessoas que batem ponto pra poder visualizar pontos/fazer ajustes de
-- outras pessoas que estão em obras específicas ou ainda de pessoas específicas, mesmo que não sejam elas que batem
-- o ponto dessas pessoas?"
--
-- Até aqui, o alcance no aplicativo vinha só do papel (o administrativo da obra: a obra em que está e as em que
-- bateu na semana; o responsável pelo ponto da obra; o ponto de equipe). Agora o DP pode LIBERAR, para uma pessoa,
-- obras específicas (ela consulta e pede por quem bate ou é cadastrado nelas) e pessoas específicas — uma linha
-- por liberação, com quem liberou e quando. Tirar a liberação apaga a linha (fica no log).
-- ===========================================================================

CREATE TABLE IF NOT EXISTS ponto.alcance_liberado (
    id             BIGSERIAL PRIMARY KEY,
    colaborador_id BIGINT      NOT NULL REFERENCES public.colaboradores (id) ON DELETE CASCADE,
    obra_id        BIGINT      REFERENCES public.obras (id) ON DELETE CASCADE,
    alvo_id        BIGINT      REFERENCES public.colaboradores (id) ON DELETE CASCADE,
    liberado_por   TEXT        NOT NULL DEFAULT '',
    liberado_em    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((obra_id IS NULL) <> (alvo_id IS NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS alcance_liberado_obra ON ponto.alcance_liberado (colaborador_id, obra_id)
    WHERE obra_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS alcance_liberado_alvo ON ponto.alcance_liberado (colaborador_id, alvo_id)
    WHERE alvo_id IS NOT NULL;
