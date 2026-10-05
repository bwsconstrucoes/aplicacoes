-- ===========================================================================
-- 048 — AUXÍLIO: DESCONTO PARCIAL DAS AUSÊNCIAS
--
-- O dono, 05/10/2026: *"precisamos poder aplicar o desconto integralmente ou
-- não, pois pode ser que de 5 dias, um tenha justificativa e vamos descontar
-- somente 4."*
--
-- `ausencias_relevadas`: as datas (AAAA-MM-DD, separadas por vírgula) das
-- ausências que NÃO são descontadas; `motivo_relevadas`: a justificativa. Só
-- valem com o desconto aplicado (`desconto_ausencias`, da 046). Vazio = desconta
-- todas, que é o que valia antes.
--
-- O código não lê nada daqui antes de este arquivo rodar (`tem_coluna`).
-- ===========================================================================
ALTER TABLE analisesps.auxilio_ajuste
    ADD COLUMN IF NOT EXISTS ausencias_relevadas TEXT NOT NULL DEFAULT '';
ALTER TABLE analisesps.auxilio_ajuste
    ADD COLUMN IF NOT EXISTS motivo_relevadas TEXT NOT NULL DEFAULT '';
