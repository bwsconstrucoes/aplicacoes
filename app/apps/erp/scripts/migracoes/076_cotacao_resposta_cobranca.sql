-- ============================================================================
-- Migração 076 — a RESPOSTA do fornecedor, a COBRANÇA e a memória que torna o
-- sistema inteligente
--
-- Pedido do dono em 18/09/2026, respondendo ao que eu havia proposto:
--
--   "Disparar sozinho eu acho que nem é o ideal. O sistema pode ter até a
--   inteligência de fazer isso, mas tem algumas nuances que só o humano sabe o
--   que deve ser feito. A cobrança também não deve ser feita sozinha, mas eu
--   quero que o sistema SUGIRA o que deve ser cobrado: 'a gente já disparou
--   cotação, não recebeu resposta desses fornecedores'. Por quê? De repente o
--   fornecedor não respondeu pelo e-mail, respondeu pelo WhatsApp, e o
--   comprador ainda não alimentou o sistema — na verdade o fornecedor já
--   respondeu."
--
--   "Aí você já tem que deixar ele pronto para se tornar inteligente: os dados
--   que a gente vai trabalhar já estarem sendo guardados, para que os
--   compradores vão aos poucos alimentando."
--
-- O QUE CADA COLUNA EXISTE PARA RESPONDER
--
--   `cobrado_em` / `cobrancas`  — "já cobrei este?" e "quantas vezes?". Sem
--       isso a tela de cobrança sugeriria o mesmo fornecedor de hora em hora,
--       e cobrar três vezes no mesmo dia queima o fornecedor bom.
--
--   `respondido_canal`          — a nuance do WhatsApp. O comprador marca que
--       a resposta chegou por fora do e-mail; o fornecedor sai da cobrança na
--       hora, mesmo antes de os preços serem digitados.
--
--   `sem_interesse` / `motivo`  — "esse não vai cotar". Não é falta de
--       resposta: é resposta negativa, e contar as duas juntas mediria errado
--       quem responde e quem não responde.
--
--   `prazo_entrega_dias` / `validade_proposta` — o que a leitura da proposta
--       agora extrai junto com os preços. Prazo e validade mudam a decisão de
--       compra tanto quanto o preço, e hoje se perdiam no corpo do e-mail.
--
-- POR QUE NÃO EXISTE UMA TABELA DE "NOTA DO FORNECEDOR"
--
-- Porque ela seria um número que ninguém sabe de onde veio. Tempo de resposta,
-- taxa de resposta, quantas vezes ganhou o pedido e se entregou no prazo são
-- todos DERIVÁVEIS do que o sistema já guarda — envio, resposta, pedido e
-- recebimento. O que faltava eram só os carimbos acima. Nota guardada
-- envelhece e mente; nota calculada na hora acompanha a realidade.
-- ============================================================================

ALTER TABLE cotacao_fornecedores
    ADD COLUMN IF NOT EXISTS cobrado_em TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS cobrancas INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS respondido_canal TEXT,
    ADD COLUMN IF NOT EXISTS sem_interesse BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS motivo_sem_interesse TEXT,
    ADD COLUMN IF NOT EXISTS prazo_entrega_dias INTEGER,
    ADD COLUMN IF NOT EXISTS validade_proposta DATE;

COMMENT ON COLUMN cotacao_fornecedores.cobrado_em IS
    'Quando este fornecedor foi cobrado pela última vez nesta cotação.';
COMMENT ON COLUMN cotacao_fornecedores.respondido_canal IS
    'Por onde a resposta chegou: EMAIL, WHATSAPP, TELEFONE ou PRESENCIAL. '
    'O comprador marca, e o fornecedor sai da fila de cobrança na hora.';
COMMENT ON COLUMN cotacao_fornecedores.sem_interesse IS
    'O fornecedor respondeu que NÃO vai cotar. Diferente de não responder — '
    'e contar as duas juntas mediria errado quem responde.';

-- O canal é escrito por gente numa lista fechada; livre viraria "zap", "Zap",
-- "whats" e nenhuma conta sairia certa depois.
ALTER TABLE cotacao_fornecedores
    DROP CONSTRAINT IF EXISTS ck_cotacao_forn_canal;
ALTER TABLE cotacao_fornecedores
    ADD CONSTRAINT ck_cotacao_forn_canal CHECK (
        respondido_canal IS NULL
        OR respondido_canal IN ('EMAIL', 'WHATSAPP', 'TELEFONE', 'PRESENCIAL'));

ALTER TABLE cotacao_fornecedores
    DROP CONSTRAINT IF EXISTS ck_cotacao_forn_prazo;
ALTER TABLE cotacao_fornecedores
    ADD CONSTRAINT ck_cotacao_forn_prazo CHECK (
        prazo_entrega_dias IS NULL
        OR (prazo_entrega_dias >= 0 AND prazo_entrega_dias <= 365));

-- A cobrança lê "o que saiu para este fornecedor e quando". Sem este índice a
-- tela varreria a tabela inteira de e-mails a cada abertura.
CREATE INDEX IF NOT EXISTS idx_envios_cotacao_coluna
    ON envios_email (destinatario_tipo, destinatario_id, criado_em DESC);
