-- ===========================================================================
-- 052 — O LOTE "WHATSAPP" ALIMENTADO PELO ROBÔ DO TELEGRAM
--
-- O dono, 08/10/2026: *"Muitas pessoas me pedem para colocar para pagar alguma
-- SP via WhatsApp (…) eu copio essas informações, colo lá no Extrair SPs do
-- lote (…) como é que a gente poderia redirecionar essas mensagens e alimentar
-- um lote chamado WhatsApp (…) sempre o primeiro de todos (…) atrelar isso ao
-- usuário."*
--
-- Três tabelas:
--
--   usuario_telegram  — qual conversa do Telegram é de qual usuário. Uma por
--                       pessoa, e uma pessoa por conversa.
--   telegram_convite  — o link de ligar, de USO ÚNICO e curto. Guarda só o
--                       resumo (sha256) do código: quem lê o banco não
--                       consegue montar um link válido.
--   lote_telegram     — cada SP que chegou pelo robô, de quem e quando. É o
--                       que impede a tela do Lote, aberta antes, de apagar no
--                       "Salvar" o que chegou depois pelo Telegram.
--
-- O código não lê nada daqui antes de este arquivo rodar (`tem_coluna`).
-- ===========================================================================
CREATE TABLE IF NOT EXISTS analisesps.usuario_telegram (
    usuario_id BIGINT PRIMARY KEY
               REFERENCES analisesps.usuarios(id) ON DELETE CASCADE,
    chat_id    BIGINT NOT NULL UNIQUE,
    ligado_em  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analisesps.telegram_convite (
    codigo_hash TEXT PRIMARY KEY,
    usuario_id  BIGINT NOT NULL
                REFERENCES analisesps.usuarios(id) ON DELETE CASCADE,
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analisesps.lote_telegram (
    id         BIGSERIAL PRIMARY KEY,
    pessoa     TEXT NOT NULL,          -- a chave do lote (`auth.chave_pessoa`)
    usuario_id BIGINT,
    sp         TEXT NOT NULL,
    chegou_em  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_analisesps_lote_telegram_pessoa
    ON analisesps.lote_telegram (pessoa, chegou_em);
