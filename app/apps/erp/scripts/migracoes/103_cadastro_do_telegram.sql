-- ============================================================================
-- Migração 103 — O CADASTRO DO TELEGRAM ENTRA NO ERP, e o convite pelo WhatsApp
--
-- O dono, em 14/11/2026: *"tem muita gente fazendo solicitações, mas não está
-- recebendo mensagens porque não tem cadastro no Telegram (…) quando a
-- mensagem não foi entregue, a pessoa receber as orientações de como proceder
-- para se cadastrar (…) essa questão dessa planilha, trazer para dentro do
-- sistema também, e a gente visualizar esse cadastramento dentro do
-- mensageria."* E quem pode se cadastrar: os colaboradores com telefone.
--
-- Até aqui quem tinha Telegram estava na aba "TelegramID" de uma planilha
-- Google, e o ERP não sabia. Nasce, no schema `mensageria`:
--
--   telegram_cadastros  quem tem Telegram ligado à empresa: o id do chat, o
--                       CPF, o telefone, e A QUEM do ERP ele pertence
--                       (colaborador ou operador). Substitui a aba "TelegramID"
--                       da planilha: o botão "Migrar da planilha" (tela
--                       Mensagens) copia o que existe, e dali em diante a
--                       planilha não é mais lida nem escrita.
--   convites            cada convite mandado pelo WhatsApp: para quem, por
--                       quê (mensagem que não chegou, ou pedido da tela), o
--                       código do link pessoal e se a pessoa usou.
--
-- E o tipo "Convite para cadastrar o Telegram" passa de Desligado para Só
-- WhatsApp — decisão do dono no mesmo dia. Só se ninguém mexeu nele na tela.
-- ============================================================================
CREATE TABLE IF NOT EXISTS mensageria.telegram_cadastros (
    id             BIGSERIAL PRIMARY KEY,
    chat_id        TEXT NOT NULL UNIQUE,
    cpf            TEXT NOT NULL DEFAULT '',
    telefone       TEXT NOT NULL DEFAULT '',      -- só dígitos, 55 + DDD + número
    nome           TEXT NOT NULL DEFAULT '',
    colaborador_id BIGINT REFERENCES colaboradores(id) ON DELETE SET NULL,
    usuario_id     BIGINT REFERENCES usuarios(id) ON DELETE SET NULL,
    origem         TEXT NOT NULL DEFAULT '',      -- TELEFONE | CPF | CONVITE | PLANILHA
    observacao     TEXT NOT NULL DEFAULT '',
    ativo          BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em      TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS telegram_cadastros_cpf ON mensageria.telegram_cadastros (cpf);
CREATE INDEX IF NOT EXISTS telegram_cadastros_telefone ON mensageria.telegram_cadastros (telefone);

CREATE TABLE IF NOT EXISTS mensageria.convites (
    id             BIGSERIAL PRIMARY KEY,
    criado_em      TIMESTAMPTZ NOT NULL DEFAULT now(),
    codigo         TEXT NOT NULL UNIQUE,          -- vai no link pessoal t.me/<robô>?start=c_<codigo>
    telefone       TEXT NOT NULL DEFAULT '',
    cpf            TEXT NOT NULL DEFAULT '',
    nome           TEXT NOT NULL DEFAULT '',
    colaborador_id BIGINT REFERENCES colaboradores(id) ON DELETE SET NULL,
    usuario_id     BIGINT REFERENCES usuarios(id) ON DELETE SET NULL,
    motivo         TEXT NOT NULL DEFAULT '',      -- AUTOMATICO | MANUAL
    tipo_que_falhou TEXT NOT NULL DEFAULT '',     -- a mensagem que não chegou
    status         TEXT NOT NULL DEFAULT '',      -- ENVIADO | FALHOU | DESLIGADO | LIMITE
    detalhe        TEXT NOT NULL DEFAULT '',
    enviado_por    TEXT NOT NULL DEFAULT '',
    aberto_em      TIMESTAMPTZ,                   -- a pessoa tocou no link
    chat_id        TEXT NOT NULL DEFAULT '',
    usado_em       TIMESTAMPTZ                    -- o cadastro foi feito por este convite
);
CREATE INDEX IF NOT EXISTS convites_cpf ON mensageria.convites (cpf, criado_em DESC);
CREATE INDEX IF NOT EXISTS convites_telefone ON mensageria.convites (telefone, criado_em DESC);
CREATE INDEX IF NOT EXISTS convites_chat ON mensageria.convites (chat_id) WHERE chat_id <> '';

-- Quem tentou se cadastrar no robô e NÃO conseguiu — no lugar da aba
-- "Pendências Telegram" da planilha. A regra é rígida (decisão do dono,
-- 14/11/2026): o número do Telegram tem de bater com o telefone do cadastro do
-- ERP; se não bate, quem resolve é o DP, atualizando o telefone. Esta lista é a
-- fila do DP para isso.
CREATE TABLE IF NOT EXISTS mensageria.telegram_tentativas (
    id             BIGSERIAL PRIMARY KEY,
    criado_em      TIMESTAMPTZ NOT NULL DEFAULT now(),
    chat_id        TEXT NOT NULL DEFAULT '',
    telefone       TEXT NOT NULL DEFAULT '',      -- o número que o Telegram confirmou
    cpf            TEXT NOT NULL DEFAULT '',      -- digitado pela pessoa, quando digitou
    nome_telegram  TEXT NOT NULL DEFAULT '',
    nome_erp       TEXT NOT NULL DEFAULT '',      -- quem o convite ou o CPF indicam ser
    motivo         TEXT NOT NULL DEFAULT ''       -- TELEFONE_NAO_BATE | CPF_SEM_TELEFONE_IGUAL | ...
);
CREATE INDEX IF NOT EXISTS telegram_tentativas_criado ON mensageria.telegram_tentativas (criado_em DESC);

UPDATE mensageria.tipos SET politica = 'WHATSAPP'
 WHERE chave = 'telegram.aviso_cadastro' AND politica = 'DESLIGADO'
   AND coalesce(atualizado_por, '') = '';
