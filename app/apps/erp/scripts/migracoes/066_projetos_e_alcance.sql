-- ============================================================================
-- Migração 066 — PROJETOS, e o alcance do operador por empresa/projeto/obra
--
-- O PEDIDO, do dono, em 13/09/2026:
--
--   "No cadastro das obras, eu precisaria criar PROJETOS, porque com projetos
--   eu faço uma associação de algumas obras e coloco todas dentro do projeto.
--   Vai ter obra que está dentro de um projeto ou não. Se a obra estiver
--   dentro de algum projeto, tudo que eu for visualizar em relação a elas — a
--   nível de relatórios, de resultados, de custos — eu poder visualizar o
--   projeto, ou seja, o somatório daquelas obras. (…) E a gente poder
--   adicionar ao usuário a obra, ou um projeto, ou todas as obras, ou uma
--   empresa ou outra empresa."
--
-- Duas coisas, e elas se encaixam:
--
--   1. **Projeto agrupa obras.** Empresa › projeto › obra. Obra sem projeto
--      continua existindo e é o caso comum.
--   2. **O alcance do operador passa a ser dito em qualquer um dos três
--      níveis.** Marcar a EMPRESA alcança as obras dela; marcar o PROJETO
--      alcança as obras dele; marcar a OBRA alcança só ela.
--
-- ⚠️ A OBRA CONTINUA SENDO A UNIDADE DO RECORTE, e isso é escolha de
-- engenharia: empresa e projeto são apenas jeitos de NOMEAR um conjunto de
-- obras, e esse conjunto é resolvido na hora da consulta. Assim, obra nova
-- dentro de um projeto já marcado entra sozinha no alcance de quem tem o
-- projeto — ninguém precisa lembrar de voltar no cadastro de cada pessoa. E
-- todo o recorte que já existe (títulos, notas, agenda, colaboradores,
-- suprimentos) passa a valer para empresa e projeto sem reescrever nada.
-- ============================================================================

CREATE TABLE IF NOT EXISTS projetos (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    codigo      TEXT        NOT NULL UNIQUE,
    nome        TEXT        NOT NULL,
    descricao   TEXT,
    -- Opcional: há projeto que atravessa empresas do grupo. Quando existe,
    -- serve de filtro na tela e de conferência ao pendurar a obra.
    empresa_id  BIGINT      REFERENCES empresas(id),
    ativo       BOOLEAN     NOT NULL DEFAULT TRUE,
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    criado_por  BIGINT      REFERENCES usuarios(id)
);

-- Obra pertence a NO MÁXIMO um projeto. Mais de um faria o somatório do
-- projeto contar a mesma obra duas vezes em relatórios diferentes, e ninguém
-- entenderia por que os números não fecham.
ALTER TABLE obras ADD COLUMN IF NOT EXISTS projeto_id BIGINT REFERENCES projetos(id);
CREATE INDEX IF NOT EXISTS idx_obras_projeto ON obras (projeto_id);

-- O alcance dito no nível do PROJETO e no nível da EMPRESA. As obras marcadas
-- uma a uma continuam em `usuario_obras`, que também guarda quem RESPONDE
-- pela obra — por isso ela não muda.
CREATE TABLE IF NOT EXISTS usuario_projetos (
    usuario_id  BIGINT NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    projeto_id  BIGINT NOT NULL REFERENCES projetos(id) ON DELETE CASCADE,
    PRIMARY KEY (usuario_id, projeto_id)
);

CREATE TABLE IF NOT EXISTS usuario_empresas (
    usuario_id  BIGINT NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    empresa_id  BIGINT NOT NULL REFERENCES empresas(id) ON DELETE CASCADE,
    PRIMARY KEY (usuario_id, empresa_id)
);
