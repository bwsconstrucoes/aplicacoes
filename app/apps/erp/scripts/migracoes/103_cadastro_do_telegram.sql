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
--                       (colaborador ou operador). A planilha continua sendo
--                       escrita junto, durante a transição, porque o Make e a
--                       emissão de nota ainda procuram lá.
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

UPDATE mensageria.tipos SET politica = 'WHATSAPP'
 WHERE chave = 'telegram.aviso_cadastro' AND politica = 'DESLIGADO'
   AND coalesce(atualizado_por, '') = '';
