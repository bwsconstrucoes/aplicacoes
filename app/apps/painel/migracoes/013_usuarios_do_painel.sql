-- ===========================================================================
-- 013 — PESSOAS COM ACESSO PROPRIO, PRESAS A OBRAS E A TELAS
--
-- Ate aqui o painel tinha UMA SENHA SO (PAINEL_SENHA): quem entrava via tudo.
-- Pedido do dono em 21/09/2026: dar acesso a alguem — um parceiro de obra, por
-- exemplo — que entre com senha propria e veja SO as obras dele, e so as telas
-- que ele liberar. Comecando por DRE e Despesas Analitico, com o resto pronto
-- para liberar quando fizer sentido.
--
-- A SENHA MESTRE CONTINUA SENDO O ADMINISTRADOR. Ela nao vira usuario: quem
-- entra por ela ve tudo, configura, escreve no OMIE e cadastra as pessoas.
-- Pessoa cadastrada aqui NUNCA escreve no OMIE — so olha e baixa o que e dela.
--
-- A senha fica embaralhada (hash com sal). Nem o dono consegue ler a senha de
-- alguem depois — so trocar. E o certo: senha guardada legivel e vazamento
-- esperando acontecer, e este banco tem o financeiro inteiro da empresa.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS painel.usuarios (
    id          BIGSERIAL PRIMARY KEY,
    usuario     TEXT NOT NULL,          -- o que a pessoa digita para entrar
    nome        TEXT NOT NULL DEFAULT '',
    senha_hash  TEXT NOT NULL,
    ativo       BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    ultimo_acesso TIMESTAMPTZ
);

-- Sem duplicar login, e sem depender de quem digitou com maiuscula.
CREATE UNIQUE INDEX IF NOT EXISTS ix_usuarios_login
    ON painel.usuarios (lower(usuario));

-- As obras que a pessoa pode ver. SEM NENHUMA LINHA AQUI, ELA NAO VE NADA —
-- falha fechado, que e o padrao do resto do repositorio.
CREATE TABLE IF NOT EXISTS painel.usuario_obras (
    usuario_id   BIGINT NOT NULL REFERENCES painel.usuarios(id) ON DELETE CASCADE,
    departamento TEXT   NOT NULL,
    PRIMARY KEY (usuario_id, departamento)
);

-- As telas liberadas, uma linha por tela. Mesma regra: sem linha, sem tela.
CREATE TABLE IF NOT EXISTS painel.usuario_telas (
    usuario_id BIGINT NOT NULL REFERENCES painel.usuarios(id) ON DELETE CASCADE,
    tela       TEXT   NOT NULL,
    PRIMARY KEY (usuario_id, tela)
);
