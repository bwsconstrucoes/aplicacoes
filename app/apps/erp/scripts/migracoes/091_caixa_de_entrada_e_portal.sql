-- ===========================================================================
-- 091 — A CAIXA DE ENTRADA DO AGENTE, E O PORTAL DA OBRA
--
-- O dono, em 05/11/2026, sobre o ponto cego que eu havia registrado:
--
-- > *"Isso é fundamental. Que agente é esse que depende de uma intervenção
-- > humana? Aí não é agente. Todo e-mail que chegar, vou ter que jogar os
-- > anexos? Não tem sentido nenhum. Então o ERP precisa estar fazendo a leitura
-- > das caixas de entrada, baixando anexo de forma completamente autônoma para
-- > saber o que fazer."*
--
-- Ele está certo, e o conserto é este arquivo mais `core/comum/email_entrada.py`.
--
-- E os canais ficaram definidos por ele, cada um com o seu papel:
--   · CONTABILIDADE → e-mail, nos dois sentidos (pedido e retorno);
--   · ADMINISTRATIVO DE OBRA → WhatsApp para COBRAR e avisar;
--   · DEVOLUÇÃO DE DOCUMENTO pela obra → **pelo sistema, nunca por WhatsApp**
--     (*"essa devolução, ela não pode ser via WhatsApp, né? Tem que ser via
--     sistema"*). É a tabela `dp_devolucoes`.
--
-- O que esta migração cria:
--
-- 1. `dp_pedidos.message_id` e `assunto` com o número — é o que permite CASAR a
--    resposta da contabilidade com o pedido. Sem a marca, casar resposta com
--    pedido seria adivinhação por nome de pessoa.
-- 2. `email_entrada_mensagens` — o que já foi lido da caixa, para não processar
--    a mesma mensagem duas vezes (e para dar o que mostrar na tela quando o
--    agente errar o casamento).
-- 3. `dp_devolucoes` — documento que foi PARA a obra e tem de voltar assinado.
-- 4. `dp_solicitacoes.solicitante_colaborador_id` — quem abre pelo PORTAL é
--    colaborador, não usuário do ERP. O administrativo de obra não tem (e não
--    deve ter) login do ERP.
-- 5. a rotina `caixa` do agente, semeada DESLIGADA como as outras.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 1. CASAR A RESPOSTA COM O PEDIDO
-- ---------------------------------------------------------------------------
ALTER TABLE dp_pedidos
    ADD COLUMN IF NOT EXISTS message_id TEXT;

COMMENT ON COLUMN dp_pedidos.message_id IS
    'O Message-ID do e-mail enviado. A resposta da contabilidade traz este '
    'valor em In-Reply-To/References — é o casamento mais confiável, melhor '
    'que o número no assunto (que alguém pode apagar ao responder).';

CREATE INDEX IF NOT EXISTS ix_dp_pedidos_message_id
    ON dp_pedidos (message_id) WHERE message_id IS NOT NULL;

-- O MESMO no registro de envio, que é onde o Message-ID nasce: o módulo de
-- e-mail passa a gerar o seu próprio (antes cada servidor de saída inventava o
-- dele, e nós não ficávamos sabendo qual foi).
ALTER TABLE envios_email
    ADD COLUMN IF NOT EXISTS message_id TEXT;

CREATE INDEX IF NOT EXISTS ix_envios_email_message_id
    ON envios_email (message_id) WHERE message_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 2. O QUE JÁ FOI LIDO DA CAIXA
--
-- Processar duas vezes a mesma mensagem significaria dois documentos iguais no
-- Drive e dois eventos "respondido" no mesmo pedido. O UID do IMAP não serve
-- como chave: ele muda quando a pasta é recriada. O Message-ID serve.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS email_entrada_mensagens (
    id            BIGSERIAL PRIMARY KEY,
    message_id    TEXT NOT NULL UNIQUE,
    caixa         TEXT NOT NULL DEFAULT 'contabilidade',
    de            TEXT,
    assunto       TEXT,
    recebido_em   TIMESTAMPTZ,
    -- CASOU com quê? Quando não casa, fica tudo NULL e a situação diz por quê —
    -- e é essa lista que a tela mostra para alguém resolver à mão. Agente que
    -- não acha o dono da mensagem tem de DIZER, não engolir.
    pedido_id     BIGINT REFERENCES dp_pedidos(id),
    devolucao_id  BIGINT,
    situacao      TEXT NOT NULL DEFAULT 'LIDA',
    motivo        TEXT,
    anexos        INTEGER NOT NULL DEFAULT 0,
    arquivados    INTEGER NOT NULL DEFAULT 0,
    processado_em TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT email_entrada_situacao CHECK (situacao IN (
        'LIDA', 'CASADA', 'SEM_DONO', 'SEM_ANEXO', 'IGNORADA', 'FALHOU'))
);

CREATE INDEX IF NOT EXISTS ix_email_entrada_situacao
    ON email_entrada_mensagens (situacao, processado_em DESC);

-- ---------------------------------------------------------------------------
-- 3. A DEVOLUÇÃO DA OBRA — o fluxo que ele desenhou em voz alta
--
-- > *"digamos que eu precise que a obra me mande um determinado documento.
-- > Imagina aí, a rescisão chegou, contabilidade fez tal, a gente encaminhou
-- > para a obra, para a obra coletar a assinatura das pessoas (…) eu preciso
-- > que aquela documentação seja devolvida. E essa devolução não pode ser via
-- > WhatsApp. Tem que ser via sistema."*
--
-- Então: o DP manda o documento para a obra, o ERP abre uma PENDÊNCIA DE
-- DEVOLUÇÃO com prazo, o WhatsApp só AVISA e COBRA, e o arquivo volta pelo
-- portal. O WhatsApp nunca recebe documento: foto de documento em conversa não
-- é arquivo, é foto perdida num celular.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dp_devolucoes (
    id             BIGSERIAL PRIMARY KEY,
    numero         TEXT NOT NULL UNIQUE,
    -- De onde nasceu: do retorno de um pedido à contabilidade, ou solto.
    pedido_id      BIGINT REFERENCES dp_pedidos(id),
    colaborador_id BIGINT REFERENCES colaboradores(id),
    obra_id        BIGINT REFERENCES obras(id),
    -- O que foi mandado para a obra fazer, em português.
    o_que          TEXT NOT NULL,
    -- O documento que FOI (para assinar) e o tipo do que tem de VOLTAR.
    documento_enviado_id BIGINT REFERENCES documentos(id),
    documento_esperado   TEXT NOT NULL,
    prazo          DATE,
    situacao       TEXT NOT NULL DEFAULT 'ABERTA',
    cobrancas      INTEGER NOT NULL DEFAULT 0,
    cobrado_em     TIMESTAMPTZ,
    -- O que voltou.
    documento_id   BIGINT REFERENCES documentos(id),
    devolvido_em   TIMESTAMPTZ,
    -- Quem devolveu é COLABORADOR (o administrativo da obra), não usuário do
    -- ERP: ele entra pelo portal, com CPF e PIN do aplicativo do ponto.
    devolvido_por_colaborador BIGINT REFERENCES colaboradores(id),
    aberto_por     BIGINT REFERENCES usuarios(id),
    criado_em      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT dp_devolucao_situacao CHECK (situacao IN (
        'ABERTA', 'COBRADA', 'DEVOLVIDA', 'CANCELADA'))
);

CREATE SEQUENCE IF NOT EXISTS dp_devolucao_numero_seq;

CREATE INDEX IF NOT EXISTS ix_dp_devolucoes_abertas
    ON dp_devolucoes (prazo) WHERE situacao IN ('ABERTA', 'COBRADA');
CREATE INDEX IF NOT EXISTS ix_dp_devolucoes_obra
    ON dp_devolucoes (obra_id, situacao);

-- ---------------------------------------------------------------------------
-- 4. QUEM ABRE PELO PORTAL É COLABORADOR, NÃO USUÁRIO DO ERP
--
-- O administrativo de obra não tem login do ERP, e não deve ter: ele precisa
-- entrar, preencher alguns campos e encaminhar. É o "portal de solicitações"
-- do Pipefy, e no ERP ele reusa o acesso que JÁ EXISTE — o aplicativo do ponto
-- (CPF + PIN). Por isso a solicitação passa a poder ter como autor um
-- colaborador em vez de um usuário.
-- ---------------------------------------------------------------------------
ALTER TABLE dp_solicitacoes
    ADD COLUMN IF NOT EXISTS solicitante_colaborador_id BIGINT
        REFERENCES colaboradores(id);

COMMENT ON COLUMN dp_solicitacoes.solicitante_colaborador_id IS
    'Quem abriu pelo PORTAL (aplicativo do ponto, CPF+PIN). Exclusivo com '
    'solicitante_id, que é o usuário do ERP.';

CREATE INDEX IF NOT EXISTS ix_dp_solicitacoes_portal
    ON dp_solicitacoes (solicitante_colaborador_id)
    WHERE solicitante_colaborador_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 5. A ROTINA NOVA DO AGENTE — desligada, como todas
-- ---------------------------------------------------------------------------
INSERT INTO dp_agente_rotinas (chave, modo) VALUES ('caixa', 'DESLIGADA')
ON CONFLICT (chave) DO NOTHING;
