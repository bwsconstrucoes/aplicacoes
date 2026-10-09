-- ===========================================================================
-- 009 — OBRA QUE SÓ BATE ENTRADA E SAÍDA (intervalo pré-assinalado)
--
-- Pedido do dono, 09/10/2026: "tem algumas obras que, por convenção, (...) só é
-- para bater o ponto de entrada e de saída ao final do dia. A gente tem que
-- prever isso aí no sistema também, para isso não gerar pendência de batida."
--
-- É o intervalo PRÉ-ASSINALADO (CLT art. 74, § 2º): o horário do almoço é o da
-- escala, sem batida. Marcado, o dia com só a entrada e a saída fica completo,
-- e a conta desconta o intervalo da escala (sem isso, o almoço viraria hora
-- extra). Quem bater as quatro, conta as quatro. Nasce desmarcado.
-- ===========================================================================

ALTER TABLE ponto.obra_config
    ADD COLUMN IF NOT EXISTS intervalo_pre_assinalado BOOLEAN NOT NULL DEFAULT FALSE;
