-- ---------------------------------------------------------------------------
-- 003 — QUEM fez, e o que é de cada um
--
-- Ate aqui o modulo sabia apenas QUE PERFIL mexeu (Consulta ou Operador), e a
-- senha e a mesma para todo mundo. O lote era um so, compartilhado: duas
-- pessoas viam a mesma lista, e a segunda a salvar sobrescrevia a primeira.
-- Os filtros nao eram guardados em lugar nenhum.
--
-- Esta migracao introduz a PESSOA. Ela nao e um cadastro de usuarios com hash
-- e tela de administracao - a senha continua sendo do perfil. E o nome que a
-- pessoa informa ao entrar, e serve para tres coisas:
--
--   1. o lote passa a ser de cada um;
--   2. os filtros ficam guardados por pessoa e voltam sozinhos;
--   3. o registro de alteracoes passa a dizer QUEM, nao so que perfil.
--
-- O nome nao autentica nada: quem digita "Marcelo" nao ganha poder nenhum a
-- mais. Quem autentica continua sendo a senha. Isso esta dito assim de
-- proposito, para ninguem confundir identificacao com autenticacao.
-- ---------------------------------------------------------------------------


-- ---------------------------------------------------------------------------
-- Preferencias por pessoa (hoje: o ultimo filtro usado)
--
-- Uma linha por pessoa e chave. O valor e texto (JSON), e nao JSONB, porque
-- nada aqui e consultado POR DENTRO - o modulo le a preferencia inteira, usa e
-- devolve. JSONB pagaria o custo de indexar o que ninguem procura.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analisesps.preferencias (
    pessoa      TEXT NOT NULL,
    chave       TEXT NOT NULL,
    valor       TEXT NOT NULL DEFAULT '',
    salvo_em    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pessoa, chave)
);


-- ---------------------------------------------------------------------------
-- O lote deixa de ser um so
--
-- A tabela nasceu com "id INTEGER PRIMARY KEY CHECK (id = 1)": existia uma
-- linha unica, de propriedade de ninguem. Agora a chave e a pessoa.
--
-- O conteudo que estava la NAO e jogado fora. Ele vira o lote da pessoa
-- '' (string vazia) - o "lote de antes", de quando era compartilhado. A tela
-- do Lote oferece traze-lo para quem chegar com o lote proprio vazio, uma vez,
-- por botao. Copiar automaticamente para todo mundo faria quatro copias do
-- mesmo lote sem ninguem pedir.
-- ---------------------------------------------------------------------------
ALTER TABLE analisesps.lote ADD COLUMN IF NOT EXISTS pessoa TEXT NOT NULL DEFAULT '';

-- A trava antiga (id = 1) impede qualquer segunda linha. Sai antes de tudo.
ALTER TABLE analisesps.lote DROP CONSTRAINT IF EXISTS lote_linha_unica;

-- Troca a chave primaria de `id` para `pessoa`. O nome da constraint da PK de
-- uma tabela criada com PRIMARY KEY inline e "<tabela>_pkey".
ALTER TABLE analisesps.lote DROP CONSTRAINT IF EXISTS lote_pkey;
ALTER TABLE analisesps.lote ALTER COLUMN id DROP NOT NULL;

-- Pode haver mais de uma linha com pessoa = '' se esta migracao rodar pela
-- metade e for repetida. Garante uma so antes de criar a chave.
DELETE FROM analisesps.lote a
 USING analisesps.lote b
 WHERE a.pessoa = b.pessoa
   AND a.ctid > b.ctid;

ALTER TABLE analisesps.lote ADD CONSTRAINT lote_pkey PRIMARY KEY (pessoa);


-- ---------------------------------------------------------------------------
-- O registro de alteracoes passa a guardar a pessoa
--
-- A coluna `perfil` continua: ela responde "tinha alcada?". A `pessoa`
-- responde "quem foi?". As duas juntas e que contam a historia. Linhas
-- antigas ficam com pessoa vazia, e isso e correto: naquele momento o modulo
-- realmente nao sabia.
-- ---------------------------------------------------------------------------
ALTER TABLE analisesps.log_alteracoes ADD COLUMN IF NOT EXISTS pessoa TEXT;
