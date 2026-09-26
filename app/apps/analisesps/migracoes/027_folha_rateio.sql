-- ===========================================================================
-- 027 — AS REGRAS DE RATEIO DA FOLHA
--
-- Pedido do dono em 26/09/2026, com as palavras dele:
--
--   "Em algum local a gente eleger as pessoas que vão ser rateadas e, para cada
--    uma — ou para um grupo, porque pode ser que tenha variação entre uma e
--    outra — definir para quais obras o valor dela vai ser rateado. Um detalhe:
--    pode ser que uma obra entre mais que a outra. Tipo assim, uma obra é 50% e
--    o restante dividido entre outras."
--
-- Para que serve: há gente cuja folha não pode ser apropriada pelo ponto.
--   1. Quem não bate ponto por causa da função — supervisores de obra. Não
--      existe dia nenhum no ponto para ratear.
--   2. Quem bate ponto na MATRIZ ou na FILIAL (CONS, BWSNE). O dia existe, mas
--      aponta para a matriz de propósito; o valor tem de ir para as obras.
--
-- ⚠️ POR QUE É CADASTRO E NÃO AJUSTE DA FOLHA. O ajuste de uma folha morre com
-- ela (é o ajuste fino, que corrige erro de ponto daquela quinzena). Isto aqui é
-- como aquela pessoa SEMPRE é apropriada — vale para toda quinzena até alguém
-- mudar. Misturar as duas coisas obrigaria a refazer, a cada quinzena, o mesmo
-- trabalho para as mesmas dez pessoas.
--
-- ⚠️ A REGRA É POR GRUPO, E A PESSOA SÓ PODE ESTAR EM UM. Um grupo com uma
-- pessoa só é o caso individual, e assim existe uma forma só de perguntar "como
-- essa pessoa é rateada?". Duas regras ativas para a mesma pessoa não têm
-- resposta certa — então o banco proíbe.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS analisesps.folha_regra_rateio (
    id          SERIAL PRIMARY KEY,
    nome        TEXT NOT NULL,
    ativa       BOOLEAN NOT NULL DEFAULT TRUE,
    observacao  TEXT NOT NULL DEFAULT '',
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    criado_por  TEXT NOT NULL DEFAULT '',
    alterado_em TIMESTAMPTZ,
    alterado_por TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_folha_regra_nome
    ON analisesps.folha_regra_rateio (lower(btrim(nome)));

-- As obras da regra, com o peso de cada uma.
--
-- `percentual` em branco com `resto` verdadeiro quer dizer "o que sobrar,
-- dividido em partes iguais entre as obras marcadas assim" — é o "50% numa obra
-- e o restante dividido entre outras" que ele descreveu, sem obrigar ninguém a
-- fazer a conta de cabeça.
CREATE TABLE IF NOT EXISTS analisesps.folha_regra_obra (
    id          SERIAL PRIMARY KEY,
    regra_id    INTEGER NOT NULL REFERENCES analisesps.folha_regra_rateio(id)
                ON DELETE CASCADE,
    obra        TEXT NOT NULL,
    percentual  NUMERIC(7,4),
    resto       BOOLEAN NOT NULL DEFAULT FALSE,
    ordem       INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_folha_regra_obra
    ON analisesps.folha_regra_obra (regra_id, upper(btrim(obra)));

-- Quem a regra alcança. O CPF é a identidade que atravessa tudo: a folha da
-- contabilidade vem por ID Fortes, o ponto vem por CPF, e o cadastro é a ponte —
-- mas é o CPF que não muda.
--
-- `nome` fica guardado junto só para a tela mostrar algo enquanto o cadastro de
-- colaboradores não estiver importado. Quando estiver, o nome vem de lá.
CREATE TABLE IF NOT EXISTS analisesps.folha_regra_pessoa (
    id          SERIAL PRIMARY KEY,
    regra_id    INTEGER NOT NULL REFERENCES analisesps.folha_regra_rateio(id)
                ON DELETE CASCADE,
    cpf         TEXT NOT NULL,          -- só dígitos
    nome        TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_folha_regra_pessoa
    ON analisesps.folha_regra_pessoa (regra_id, cpf);

-- Para achar depressa a regra de uma pessoa na hora de montar a folha.
CREATE INDEX IF NOT EXISTS ix_folha_regra_pessoa_cpf
    ON analisesps.folha_regra_pessoa (cpf);

-- ⚠️ A TRAVA DE "UMA REGRA ATIVA POR PESSOA NÃO ESTÁ AQUI, E ISSO É DÍVIDA.
--
-- A trava certa seria um índice único em (cpf) valendo só para as regras ativas.
-- O Postgres NÃO aceita: a condição de um índice parcial não pode ter subconsulta
-- (`WHERE regra_id IN (SELECT ... WHERE ativa)`), e a coluna `ativa` mora na
-- outra tabela.
--
-- As saídas seriam: repetir `ativa` aqui (dois lugares para a mesma verdade, que
-- é como se cria divergência silenciosa) ou um gatilho. Por ora a conferência é
-- no código, em `folha_rateio.gravar`, com teste. Fica ESCRITO aqui porque quem
-- gravar nessas tabelas por outro caminho não vai ter essa proteção.
--
-- Por que a pessoa pode reaparecer numa regra DESATIVADA: regra desativada é
-- histórico — ela explica como a folha de março foi rateada, e apagar isso
-- tiraria do relatório a capacidade de se explicar.
