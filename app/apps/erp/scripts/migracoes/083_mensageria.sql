-- ============================================================================
-- Migração 083 — a MENSAGERIA: por onde cada mensagem sai, com registro
--
-- Pedido do dono em 05/10/2026: "eu preciso ter gestão sobre quais mensagens
-- vão para o WhatsApp e quais não vão (…) a prioridade de envio é Telegram;
-- por enquanto, ponto permitido no WhatsApp". E o Z-API bloqueia quando o
-- volume sobe, então o WhatsApp tem teto para a empresa inteira.
--
-- O que nasce aqui, no schema `mensageria` (o código mora em
-- app/apps/mensageria/, e a tela "Mensagens" entra no menu do ERP como o ponto):
--
--   tipos       o catálogo: um tipo por aviso que o sistema manda (QR do ponto,
--               título pago…) e a POLÍTICA escolhida na tela para ele:
--               TELEGRAM | TELEGRAM_OU_WHATSAPP | WHATSAPP | DESLIGADO.
--               O código declara os tipos; a tela decide a política.
--   envios      o REGISTRO de tudo que o notificador tentou mandar: tipo,
--               canal, destinatário, texto, resultado. Guardado 90 dias
--               (decisão do dono) — a faxina é do próprio código.
--   parametros  chave/valor da mensageria: "WhatsApp ligado" (nasce
--               DESLIGADO — ligar é um clique do dono), teto por hora e por
--               dia, hora do último aviso de teto.
--
-- E as seções "Mensagens" nos perfis semeados (mesma regra da 082): quem tem o
-- cargo correspondente em permissoes.py recebe a seção — Administrador edita;
-- Diretor financeiro e Departamento pessoal leem.
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS mensageria;

CREATE TABLE IF NOT EXISTS mensageria.tipos (
    chave          TEXT PRIMARY KEY,
    nome           TEXT NOT NULL,
    modulo         TEXT NOT NULL DEFAULT '',
    descricao      TEXT NOT NULL DEFAULT '',
    politica       TEXT NOT NULL DEFAULT 'TELEGRAM'
                   CHECK (politica IN ('TELEGRAM', 'TELEGRAM_OU_WHATSAPP', 'WHATSAPP', 'DESLIGADO')),
    atualizado_em  TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_por TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS mensageria.envios (
    id            BIGSERIAL PRIMARY KEY,
    criado_em     TIMESTAMPTZ NOT NULL DEFAULT now(),
    tipo          TEXT NOT NULL DEFAULT '',
    canal         TEXT NOT NULL DEFAULT '',          -- telegram | whatsapp
    destinatario  TEXT NOT NULL DEFAULT '',          -- telefone ou id do chat
    cpf           TEXT NOT NULL DEFAULT '',
    texto         TEXT NOT NULL DEFAULT '',
    nome_arquivo  TEXT NOT NULL DEFAULT '',
    status        TEXT NOT NULL DEFAULT '',          -- ENVIADO | FALHOU | DESLIGADO | SEM_DESTINO | LIMITE
    detalhe       TEXT NOT NULL DEFAULT '',
    origem        TEXT NOT NULL DEFAULT ''           -- módulo que pediu
);
CREATE INDEX IF NOT EXISTS envios_criado_em ON mensageria.envios (criado_em DESC);
CREATE INDEX IF NOT EXISTS envios_canal_status_criado ON mensageria.envios (canal, status, criado_em);

CREATE TABLE IF NOT EXISTS mensageria.parametros (
    chave          TEXT PRIMARY KEY,
    valor          TEXT NOT NULL DEFAULT '',
    atualizado_em  TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_por TEXT NOT NULL DEFAULT ''
);

INSERT INTO perfil_secoes (perfil_id, secao, nivel)
SELECT p.id, v.secao, v.nivel
  FROM perfis p
  JOIN (VALUES
    ('Administrador',        'adm_mensagens', 'EDITAR'),
    ('Diretor financeiro',   'adm_mensagens', 'LER'),
    ('Departamento pessoal', 'adm_mensagens', 'LER')
  ) AS v(perfil, secao, nivel) ON v.perfil = p.nome
ON CONFLICT (perfil_id, secao) DO NOTHING;
