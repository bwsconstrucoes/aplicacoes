-- ============================================================================
-- Migração 067 — uma conta de e-mail servindo várias empresas
--
-- O PEDIDO, do dono, em 14/09/2026:
--
--   "O ideal seria que a gente continuasse utilizando um e-mail principal para
--   encaminhar de outras empresas, e de repente usar um alias — mas eu acho
--   que não dá, né? (…) é e-mail da Locaweb."
--
-- DÁ, e é isto que a coluna permite: a empresa deixa de ter conta própria e
-- passa a **usar a conta de outra** — mesmo servidor, mesmo usuário, mesma
-- senha —, mudando só COMO ELA APARECE para quem recebe (o remetente e o
-- "responder para", que continuam sendo dela).
--
-- ⚠️ O QUE O SISTEMA NÃO RESOLVE, e precisa estar escrito: o provedor pode
-- recusar um remetente de domínio diferente do da conta que entrou, e os
-- domínios que não autorizarem o servidor no DNS (SPF/DKIM) tendem a cair no
-- spam de quem recebe. Endereços do MESMO domínio (um alias, por exemplo)
-- funcionam direto; domínio diferente depende de configuração no provedor e no
-- DNS daquele domínio. Isso é fora do ERP.
-- ============================================================================

ALTER TABLE empresas
    ADD COLUMN IF NOT EXISTS conta_email_de_id BIGINT REFERENCES empresas(id);

COMMENT ON COLUMN empresas.conta_email_de_id IS
    'Empresa cuja conta de envio esta empresa usa. Nulo = conta própria.';

CREATE INDEX IF NOT EXISTS idx_empresas_conta_email
    ON empresas (conta_email_de_id);
