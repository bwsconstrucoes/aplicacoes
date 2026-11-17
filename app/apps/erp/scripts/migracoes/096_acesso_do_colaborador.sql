-- ===========================================================================
-- 096 — O COLABORADOR E O ACESSO AO SISTEMA (09/11/2026)
--
-- Três coisas que ele apontou olhando o DP antes de colocar em prática:
--
-- 1. *"Você colocou lá pré-cadastro colaborador, NÃO EXISTE esse pré-cadastro,
--    existe o cadastro, ponto final. O cadastro já tem que vir com
--    documentação e tudo mais."*
-- 2. *"Nosso cadastro tem o cadastro de colaboradores, tem o cadastro de
--    usuários do sistema. É para haver uma correlação entre isso aí (…) quando
--    uma pessoa for desativada do cadastro, for demitido, sair da empresa, a
--    gente precisaria automaticamente bloquear o acesso dela."*
-- 3. *"Facilitar o primeiro acesso (…) eu vou cadastrar o e-mail da pessoa,
--    configurar o perfil, e disparar um e-mail para ela gravar a senha dela."*
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 1. NÃO EXISTE PRÉ-CADASTRO
--
-- O tipo se chamava PRE_CADASTRO e dizia "a obra manda o nome e o CPF; o DP
-- completa" — eu tinha copiado a ideia do pipe. Ele desfez: o cadastro é UM,
-- e nasce com documentação. O que a obra abre é um pedido de CADASTRO, com os
-- papéis junto; o que o DP faz é CONFERIR, não completar o que faltou.
--
-- A linha já gravada muda junto. Tipo renomeado no código e linha velha com o
-- nome antigo no banco seria um pedido que some da tela sem ninguém apagar.
-- ---------------------------------------------------------------------------
UPDATE dp_solicitacoes SET tipo = 'CADASTRO' WHERE tipo = 'PRE_CADASTRO';

-- ---------------------------------------------------------------------------
-- 2. O COLABORADOR E O OPERADOR SÃO A MESMA PESSOA — e agora o sistema sabe
--
-- Hoje são dois cadastros sem ligação nenhuma: `colaboradores` (quem trabalha
-- e recebe) e `usuarios` (quem entra no ERP). A mesma pessoa aparece nos dois
-- e ninguém liga um ao outro — então demitir alguém deixava o acesso dela de
-- pé até alguém lembrar de ir na outra tela.
--
-- ⚑ A LIGAÇÃO FICA NO COLABORADOR, e não no usuário, de propósito: nem todo
-- operador do ERP é colaborador da BWS (o PARCEIRO da obra, a fiscalização do
-- contratante, o contador), mas todo acesso ligado a um colaborador tem de
-- cair quando ele sai. Pôr a coluna no usuário obrigaria todo mundo a ter um
-- colaborador do outro lado.
--
-- UNIQUE porque um operador é uma pessoa: dois colaboradores apontando para o
-- mesmo login seria exatamente o tipo de coisa que faz o desligamento de um
-- derrubar o acesso do outro.
-- ---------------------------------------------------------------------------
ALTER TABLE colaboradores
    ADD COLUMN IF NOT EXISTS usuario_id BIGINT REFERENCES usuarios(id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_colaboradores_usuario
    ON colaboradores (usuario_id) WHERE usuario_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 3. O CONVITE DE PRIMEIRO ACESSO
--
-- A pessoa é cadastrada com e-mail e perfil, recebe um link e GRAVA A PRÓPRIA
-- SENHA. É o desenho dele, e é melhor que o de hoje por um motivo de
-- segurança: hoje alguém digita uma senha pela pessoa e manda por WhatsApp —
-- a senha passa por um terceiro, fica escrita numa conversa e quase nunca é
-- trocada depois.
--
-- ⚑ O TOKEN NÃO É GUARDADO. Só o resumo (SHA-256) dele. Quem tiver acesso ao
-- banco — um backup, uma consulta, um relatório — NÃO consegue entrar na
-- conta de ninguém, porque o que está aqui não serve para abrir nada. É a
-- mesma razão de a senha ser hash e não texto.
--
-- `usado_em` é o que faz o link ser de USO ÚNICO: link de definir senha que
-- continua valendo depois de usado é link que, reencaminhado, troca a senha
-- de alguém.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS usuario_convites (
    id          BIGSERIAL PRIMARY KEY,
    usuario_id  BIGINT      NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    -- PRIMEIRO_ACESSO (nunca teve senha) ou NOVA_SENHA (esqueceu/foi trocada).
    tipo        TEXT        NOT NULL DEFAULT 'PRIMEIRO_ACESSO',
    token_hash  TEXT        NOT NULL,
    expira_em   TIMESTAMPTZ NOT NULL,
    usado_em    TIMESTAMPTZ,
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    criado_por  BIGINT      REFERENCES usuarios(id),
    -- O envio: para onde foi e se saiu. Sem isto, "não recebi o e-mail" não
    -- tem resposta.
    enviado_para TEXT,
    envio_id     BIGINT REFERENCES envios_email(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_usuario_convites_token
    ON usuario_convites (token_hash);
CREATE INDEX IF NOT EXISTS ix_usuario_convites_usuario
    ON usuario_convites (usuario_id, criado_em DESC);
