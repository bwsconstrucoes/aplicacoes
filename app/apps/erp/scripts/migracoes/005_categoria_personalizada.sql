-- Migração 005 — categoria editável pela BWS
-- 'personalizada' marca a conta que o usuário renomeou/ajustou: a reinstalação
-- do plano padrão NÃO sobrescreve mais o texto dela (a edição do usuário vence).
ALTER TABLE categorias
    ADD COLUMN IF NOT EXISTS personalizada BOOLEAN NOT NULL DEFAULT FALSE;
