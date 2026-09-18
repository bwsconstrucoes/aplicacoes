-- ============================================================================
-- Migração 077 — o BANCO DE PREÇOS HISTÓRICO, vindo da planilha
--
-- Pedido do dono em 18/09/2026:
--
--   "Como a gente já tem um histórico de banco de preços (…) vai tentar já
--   criar o nosso banco de informações de preços, que vai servir com toda
--   certeza já para o nosso sistema inteligente. Eu quero ter o histórico de
--   tudo que eu comprei (…) isso é interessante inclusive para quando a gente
--   está na tela das solicitações já poder estar visualizando: ó, o insumo,
--   onde está o último menor preço, qual o fornecedor, qual o valor."
--
-- O QUE ESTA MIGRAÇÃO ACRESCENTA, e por que cada coisa:
--
--   `chave_externa`   a linha da planilha, identificada. É o que impede que
--       importar o mesmo arquivo duas vezes dobre o banco de preços — e é
--       quase certo que alguém vá importar duas vezes, porque a primeira vez
--       ninguém confia. Índice ÚNICO: a trava fica no banco, não na boa
--       intenção do importador.
--
--   `origem`          de onde o preço veio: SISTEMA (nasceu numa cotação
--       daqui) ou PLANILHA (veio do histórico antigo). Sem isso, daqui a um
--       ano ninguém distingue o preço que passou pelo mapa do preço que veio
--       de um arquivo que já não existe mais — e são coisas de confiabilidade
--       diferente.
--
--   `comprador_nome`  o "Responsável" da planilha. Não é usuário do sistema:
--       é o nome de quem cotou, escrito à mão numa planilha. Fica como TEXTO
--       de propósito — amarrar a uma tabela de usuários exigiria inventar
--       gente que talvez não trabalhe mais aqui.
--
-- OS DOIS ÍNDICES NÃO SÃO ENFEITE. A planilha traz dezenas de milhares de
-- linhas, e a tela de Solicitações vai perguntar "qual o último e o menor
-- preço deste insumo?" para cada item da lista. Sem índice por (insumo, data)
-- isso varre a tabela inteira por item — e a tela que deveria ajudar o
-- comprador passa a fazê-lo esperar.
-- ============================================================================

ALTER TABLE precos_historico
    ADD COLUMN IF NOT EXISTS chave_externa TEXT,
    ADD COLUMN IF NOT EXISTS origem TEXT NOT NULL DEFAULT 'SISTEMA',
    ADD COLUMN IF NOT EXISTS comprador_nome TEXT;

COMMENT ON COLUMN precos_historico.chave_externa IS
    'Identifica a linha da planilha de origem. Único: reimportar o mesmo '
    'arquivo não duplica o histórico.';
COMMENT ON COLUMN precos_historico.origem IS
    'SISTEMA (nasceu numa cotação daqui) ou PLANILHA (histórico importado).';
COMMENT ON COLUMN precos_historico.comprador_nome IS
    'Quem cotou, como estava escrito na planilha. Texto, não usuário: pode '
    'ser gente que não trabalha mais aqui.';

ALTER TABLE precos_historico
    DROP CONSTRAINT IF EXISTS ck_preco_origem;
ALTER TABLE precos_historico
    ADD CONSTRAINT ck_preco_origem CHECK (origem IN ('SISTEMA', 'PLANILHA'));

CREATE UNIQUE INDEX IF NOT EXISTS uq_preco_chave_externa
    ON precos_historico (chave_externa)
    WHERE chave_externa IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_precos_insumo_data
    ON precos_historico (insumo_id, data DESC);

CREATE INDEX IF NOT EXISTS idx_precos_fornecedor
    ON precos_historico (fornecedor_id)
    WHERE fornecedor_id IS NOT NULL;
