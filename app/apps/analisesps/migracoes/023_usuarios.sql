-- ===========================================================================
-- 023 — QUEM ENTRA PASSA A TER CADASTRO PRÓPRIO: usuário, senha e telas
--
-- Até aqui a Análise de SPs tinha DUAS SENHAS SÓ, iguais para todo mundo: uma
-- de Consulta e uma de Operador, nas variáveis do Render. Quem digitava a de
-- Operador via e fazia TUDO — e o nome escolhido na entrada era só uma
-- etiqueta, sem tranca nenhuma por trás.
--
-- Pedido do dono em 25/09/2026: *"os usuários eu adiciono, eles estão com
-- senha única. Eu quero fazer similar ao painel. Vou poder cadastrar o
-- operador, definir a senha, definir as telas que ele tem acesso. Aí vai ter
-- um usuário master, e os outros a gente define as permissões."*
--
-- A SENHA DO RENDER CONTINUA SENDO O MESTRE, e isso não é preguiça: é o que
-- impede o dono de se trancar para fora. Se o cadastro falhar, se a migração
-- não tiver rodado, se alguém apagar o próprio acesso sem querer — a senha do
-- Render ainda entra e ainda vê tudo. Quem é cadastrado aqui entra com usuário
-- e senha próprios e alcança SÓ as telas marcadas.
--
-- A SENHA FICA EMBARALHADA (PBKDF2 com sal, pelo werkzeug que o Flask já
-- traz). Nem o dono lê a senha de alguém depois — só troca. Este banco tem os
-- pagamentos da empresa: senha legível aqui seria vazamento esperando dia.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS analisesps.usuarios (
    id          BIGSERIAL PRIMARY KEY,
    usuario     TEXT NOT NULL,            -- o que a pessoa digita para entrar
    nome        TEXT NOT NULL DEFAULT '', -- como ela aparece nas telas e no log
    senha_hash  TEXT NOT NULL,
    ativo       BOOLEAN NOT NULL DEFAULT TRUE,
    -- FALSE = vê e exporta, não altera nada (o antigo perfil Consulta).
    -- TRUE  = também altera, agenda, gera BeeVale, concilia (o Operador).
    -- O padrão é o MENOR poder: cadastro pela metade não vira permissão.
    pode_operar BOOLEAN NOT NULL DEFAULT FALSE,
    criado_em     TIMESTAMPTZ NOT NULL DEFAULT now(),
    ultimo_acesso TIMESTAMPTZ
);

-- Sem login repetido, e sem depender de quem digitou com maiúscula.
CREATE UNIQUE INDEX IF NOT EXISTS ux_analisesps_usuario_login
    ON analisesps.usuarios (lower(usuario));

-- As telas liberadas, uma linha por tela.
--
-- ⚠️ SEM NENHUMA LINHA AQUI, A PESSOA NÃO ENTRA EM TELA NENHUMA. Lista vazia
-- quer dizer NENHUMA, nunca "todas" — é a mesma regra do painel e do ERP, e
-- existe porque o contrário transforma um cadastro esquecido pela metade em
-- acesso total.
CREATE TABLE IF NOT EXISTS analisesps.usuario_telas (
    usuario_id BIGINT NOT NULL REFERENCES analisesps.usuarios(id) ON DELETE CASCADE,
    tela       TEXT   NOT NULL,
    PRIMARY KEY (usuario_id, tela)
);
