-- ===========================================================================
-- Migração 043 — o anexo passa a poder morar no Google Drive.
--
-- POR QUE
--
-- Decisão do dono em 08/09/2026, com a conta dele: o plano de banco é de 2 GB
-- e a empresa já paga 2 TB de Google Drive no Workspace. Anexo é o que mais
-- cresce em tamanho no ERP — comprovante, nota, foto de medição — e é também
-- o que menos precisa estar dentro do banco.
--
-- O QUE MUDA, E O QUE NÃO MUDA
--
-- Não muda nada para quem usa: o arquivo continua sendo aberto pelo endereço
-- do ERP, que confere permissão e escopo antes de entregar. O link do Drive
-- NUNCA vai para a tela — se fosse, qualquer um com o endereço abriria
-- holerite e comprovante bancário sem passar por login.
--
-- Muda onde os bytes ficam. Por isso as duas colunas novas:
--
--   guardado_em    'BANCO' (como sempre foi) ou 'DRIVE'.
--   drive_file_id  o identificador do arquivo lá, quando for 'DRIVE'.
--
-- A COLUNA `conteudo` CONTINUA EXISTINDO e continua sendo o padrão. A troca é
-- por escolha, ligada na tela de Configurações quando a pasta estiver criada;
-- sem pasta configurada, tudo segue exatamente como antes. E anexo antigo só
-- sai do banco depois que a cópia no Drive for conferida byte a byte.
-- ===========================================================================

ALTER TABLE anexos ADD COLUMN IF NOT EXISTS guardado_em TEXT NOT NULL DEFAULT 'BANCO';
ALTER TABLE anexos ADD COLUMN IF NOT EXISTS drive_file_id TEXT;

ALTER TABLE anexos DROP CONSTRAINT IF EXISTS ck_anexo_guardado_em;
ALTER TABLE anexos ADD CONSTRAINT ck_anexo_guardado_em
    CHECK (guardado_em IN ('BANCO', 'DRIVE'));

-- Anexo no Drive TEM de ter o identificador do arquivo; anexo no banco TEM de
-- ter os bytes. Sem isto, um defeito de código produziria anexo que não está
-- em lugar nenhum — e ninguém descobriria antes de precisar do documento.
ALTER TABLE anexos DROP CONSTRAINT IF EXISTS ck_anexo_tem_onde_morar;
ALTER TABLE anexos ADD CONSTRAINT ck_anexo_tem_onde_morar CHECK (
    (guardado_em = 'DRIVE' AND drive_file_id IS NOT NULL)
    OR (guardado_em = 'BANCO' AND conteudo IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_anexos_onde ON anexos (guardado_em);
CREATE UNIQUE INDEX IF NOT EXISTS idx_anexos_drive
    ON anexos (drive_file_id) WHERE drive_file_id IS NOT NULL;
