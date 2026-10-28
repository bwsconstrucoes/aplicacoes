-- ===========================================================================
-- 016 — O QUE O DP MUDOU NUM PEDIDO, ANTES DE DECIDIR
--
-- Pedido do dono, 29/10/2026: "nessa situação que a pessoa errou a obra, a gente aqui do DP poderia
-- até fazer essa alteração. Mas baseado na portaria, a gente pode fazer isso? (…) a gente precisaria
-- meio que editar o pedido e alterar a obra."
--
-- Pode, enquanto o pedido espera decisão: ele ainda não é batida — a Portaria 671 protege as
-- MARCAÇÕES (nada se apaga nem se altera depois de gravada), e o pedido de ajuste é o "tratamento"
-- que vem antes. Mas a mudança fica registrada aqui, para sempre: o que era, o que ficou, quem
-- mudou, quando e por quê — a pessoa pediu uma coisa, e o registro mostra que o DP corrigiu.
-- Depois de aprovado, a batida já existe; aí vale "Corrigir a obra ou a hora" da batida.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS ponto.ocorrencia_alteracoes (
    id              BIGSERIAL PRIMARY KEY,
    ocorrencia_id   BIGINT      NOT NULL REFERENCES ponto.ocorrencias (id) ON DELETE CASCADE,
    campo           TEXT        NOT NULL,
    de              TEXT,
    para            TEXT,
    motivo          TEXT        NOT NULL,
    usuario_id      BIGINT,
    usuario_nome    TEXT        NOT NULL,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_ponto_ocorrencia_alteracoes ON ponto.ocorrencia_alteracoes (ocorrencia_id);
