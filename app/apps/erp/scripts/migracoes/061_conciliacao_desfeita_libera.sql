-- ============================================================================
-- Migração 061 — conciliação desfeita libera a linha do extrato, de verdade
--
-- Varredura adversarial de 11/09/2026, pedida pelo dono: *"o casamento das
-- informações bancárias de conciliação, de extratos, com a informação de
-- baixa, isso aí é extremamente sensível"*.
--
-- A migração 031 criou o índice parcial que diz, com estas palavras no
-- comentário dela: "uma linha do extrato comprova no máximo UM pagamento
-- (enquanto a conciliação estiver de pé; desfeita, a linha volta a ficar
-- livre)". A promessa nunca valeu: as restrições antigas da tabela
-- (uq_conc_extrato / uq_conc_pagamento) valem MESMO depois de desfeita e
-- nunca foram removidas. Como o sistema inteiro filtra por "não desfeita",
-- ele oferecia a linha como livre e o banco recusava com texto de
-- programador na tela.
--
-- Aqui as antigas saem e o pagamento ganha o índice parcial equivalente ao do
-- extrato, que a 031 tinha criado só para um dos dois lados.
--
-- Não depende de dado nenhum: aplica sempre.
-- ============================================================================

ALTER TABLE conciliacoes DROP CONSTRAINT IF EXISTS uq_conc_extrato;
ALTER TABLE conciliacoes DROP CONSTRAINT IF EXISTS uq_conc_pagamento;

CREATE UNIQUE INDEX IF NOT EXISTS uq_conciliacao_pagamento_vigente
    ON conciliacoes (pagamento_id) WHERE desfeita_em IS NULL;
