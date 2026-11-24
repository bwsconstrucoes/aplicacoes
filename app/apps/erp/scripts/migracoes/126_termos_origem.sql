-- ===========================================================================
-- A CENTRAL DE ASSINATURAS (23/11/2026) — feito pelo chat do PONTO, a pedido
-- do dono: *"vamos concentrar o aplicativo do ponto para ser o caminho para a
-- coleta dessas assinaturas, porque está tudo diretamente associado ao
-- colaborador"*.
--
-- `dp_termos` (migração 117) vira a base única dos documentos que o
-- colaborador assina. O que falta nela:
--
-- 1. origem / origem_ids — de onde o documento nasceu, quando não foi de um
--    atendimento do DP. Primeiro caso: o TERMO DE ACORDO DE COMPENSAÇÃO, que
--    nasce sozinho quando o DP lança a compensação (origem 'PONTO_COMPENSACAO',
--    origem_ids = os pedidos de compensação do ponto que ele cobre). Se os
--    pedidos forem cancelados, o termo aberto é cancelado junto.
--
-- 2. assinado_pela_pessoa — a própria pessoa assinou, logada no aplicativo do
--    ponto com CPF e PIN (antes só havia quem conduzia: o administrativo).
--
-- 3. O tipo do Arquivo para o termo de compensação.
-- ===========================================================================
ALTER TABLE dp_termos ADD COLUMN IF NOT EXISTS origem TEXT;
ALTER TABLE dp_termos ADD COLUMN IF NOT EXISTS origem_ids BIGINT[];
ALTER TABLE dp_termos ADD COLUMN IF NOT EXISTS assinado_pela_pessoa BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS ix_dp_termos_colaborador ON dp_termos (colaborador_id);
CREATE INDEX IF NOT EXISTS ix_dp_termos_origem ON dp_termos (origem) WHERE origem IS NOT NULL;

INSERT INTO documento_tipos (codigo, nome, grupo, dono, sigilo) VALUES
    ('TERMO-COMPENSACAO', 'Termo de acordo de compensação de jornada', 'PESSOA', 'PESSOA', 'PESSOAL')
ON CONFLICT (codigo) DO NOTHING;
