-- Migração 001 — sequence do número SP (usada por core/titulos/service.py)
CREATE SEQUENCE IF NOT EXISTS seq_numero_sp START 1;
