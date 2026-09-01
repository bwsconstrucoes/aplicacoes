-- ============================================================================
-- Migração 024 — coordenadas da obra
-- Para ver no mapa onde estão as obras, os equipamentos locados e o volume
-- financeiro. Sem coordenada a obra ainda aparece na visão por município.
-- ============================================================================
ALTER TABLE obras
    ADD COLUMN IF NOT EXISTS latitude  NUMERIC(10,6),
    ADD COLUMN IF NOT EXISTS longitude NUMERIC(10,6);
