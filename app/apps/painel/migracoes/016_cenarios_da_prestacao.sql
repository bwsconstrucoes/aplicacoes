-- 016 — A prestação de contas passa a girar em torno do CENÁRIO
--
-- O que o dono pediu em 22/09/2026, depois de olhar o que existia: uma tela só,
-- onde ele nomeia sócios e parceiros, escolhe a régua do rateio (mão de obra ou
-- faturamento), diz quais contas da matriz se dividem e em que percentual, e
-- pode guardar isso com nome para comparar com outro jeito de dividir.
--
--   "Esse cenário eu vou utilizar 50% da mão de obra. Esse cenário eu vou
--    utilizar 70%. Esse cenário eu vou utilizar o faturamento."
--
-- O que muda em relação às `regras`: lá cada linha era um cadastro com nome,
-- escopo e vigência, e configurar dava trabalho. Aqui é uma lista dos grupos da
-- matriz com um campo de percentual ao lado. O que não for tocado herda o
-- percentual padrão do cenário — configurar é mexer em poucas linhas, não em
-- todas.
--
-- As `regras` continuam existindo e a tela antiga continua funcionando: elas só
-- serão aposentadas quando o dono aprovar a nova.

CREATE TABLE IF NOT EXISTS painel.cenario (
    id                BIGSERIAL PRIMARY KEY,
    nome              TEXT NOT NULL UNIQUE,
    -- a régua do rateio da estrutura
    criterio          TEXT NOT NULL DEFAULT 'pessoal',     -- pessoal / faturamento
    janela            TEXT NOT NULL DEFAULT '1',           -- 1 / 3 / 12 / acumulado
    -- quanto se divide de uma conta da matriz que ninguém marcou
    pct_padrao        NUMERIC(6,3) NOT NULL DEFAULT 100,
    -- a régua dos juros de empréstimo
    juros_por_deficit INTEGER NOT NULL DEFAULT 1,
    juros_sem_deficit TEXT NOT NULL DEFAULT 'sobra',       -- sobra / estrutura
    -- a conta entre sócios
    taxa_adm_pct      NUMERIC(6,3) NOT NULL DEFAULT 1.5,
    medida            TEXT NOT NULL DEFAULT 'comprometido',
    observacao        TEXT NOT NULL DEFAULT '',
    criado_em         TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Quanto de cada conta da matriz entra no bolo a repartir.
--
-- Três níveis, do mais amplo ao mais específico: GRUPO, CATEGORIA e o
-- LANÇAMENTO individual. O mais específico ganha — é o que permite dizer
-- "despesa com pessoal divide 50%, mas AQUELES três lançamentos não dividem
-- nada" sem ter de listar tudo o que divide.
CREATE TABLE IF NOT EXISTS painel.cenario_peso (
    cenario_id  BIGINT NOT NULL REFERENCES painel.cenario(id) ON DELETE CASCADE,
    nivel       TEXT NOT NULL,            -- grupo / categoria / lancamento
    chave       TEXT NOT NULL,            -- o nome do grupo, o da categoria, ou o código do título
    pct         NUMERIC(6,3) NOT NULL DEFAULT 0,
    PRIMARY KEY (cenario_id, nivel, chave)
);

-- Quem divide o resultado, e quanto.
--
-- Por OBRA, e não por projeto como nas `participacoes`: o dono fala de "as
-- obras do Ceará", e parceiro entra em obra, não na construtora. Obra em branco
-- vale para todas as obras que o cenário alcança — é o atalho para o sócio que
-- participa de tudo, sem ter de repetir a linha 174 vezes.
CREATE TABLE IF NOT EXISTS painel.cenario_participacao (
    id         BIGSERIAL PRIMARY KEY,
    cenario_id BIGINT NOT NULL REFERENCES painel.cenario(id) ON DELETE CASCADE,
    obra       TEXT NOT NULL DEFAULT '',
    socio_id   BIGINT NOT NULL REFERENCES painel.socios(id) ON DELETE CASCADE,
    pct        NUMERIC(6,3) NOT NULL DEFAULT 0,
    UNIQUE (cenario_id, obra, socio_id)
);

CREATE INDEX IF NOT EXISTS ix_cenario_peso ON painel.cenario_peso(cenario_id, nivel);
CREATE INDEX IF NOT EXISTS ix_cenario_part ON painel.cenario_participacao(cenario_id);
