-- ============================================================================
-- Migração 029 — o escopo de quem lança deixa de ser regra fixa do perfil
--
-- Até aqui, ADMINISTRATIVO_OBRA e LANCADOR viam SEMPRE apenas o que eles
-- mesmos lançaram: regra colada no perfil, igual para todo mundo. Na prática o
-- administrativo de uma obra grande precisa enxergar a obra inteira, e o de
-- outra não deve. É decisão por PESSOA, não por cargo.
--
-- Passa a ser um campo do cadastro do operador:
--   PROPRIOS          — só os lançamentos dele (comportamento atual)
--   OBRAS_DESIGNADAS  — tudo das obras já associadas a ele em usuario_obras
--
-- O DEFAULT é PROPRIOS, o mais restritivo: quem já está cadastrado não muda de
-- alcance ao aplicar esta migração. Ampliar é escolha consciente, feita na
-- tela de Configurações, operador por operador.
-- ============================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'escopo_visao_usuario') THEN
        CREATE TYPE escopo_visao_usuario AS ENUM ('PROPRIOS', 'OBRAS_DESIGNADAS');
    END IF;
END$$;

ALTER TABLE usuarios
    ADD COLUMN IF NOT EXISTS escopo_visao escopo_visao_usuario
        NOT NULL DEFAULT 'PROPRIOS';

COMMENT ON COLUMN usuarios.escopo_visao IS
    'Alcance da visão deste operador: PROPRIOS (só o que ele lançou) ou '
    'OBRAS_DESIGNADAS (tudo das obras associadas a ele). Vale igual para '
    'listagem e detalhe. Só tem efeito nos perfis que filtram por autoria.';
