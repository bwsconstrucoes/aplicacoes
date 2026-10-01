-- ===========================================================================
-- 029 — A FOLHA IMPORTADA FICA GUARDADA
--
-- POR QUE ISTO É O PRÓXIMO PASSO, e não uma tela bonita: o dono pediu, em
-- 27/09/2026, um painel com os totais por obra e por conta — e disse por quê:
--
--   "Tem que ter informação gerencial, tipo dashboard, para poder estar vendo
--    qual é o total por obra, porque isso já ajuda nessa questão do rateio."
--
-- ⚠️ NÃO HÁ COMO SOMAR POR OBRA O QUE NÃO ESTÁ GRAVADO. Até aqui a folha era
-- lida do `.xls` e calculada em memória, e morria com a requisição. Sem esta
-- tabela não existe painel, não existe tela por verba, não existe arquivo de
-- pagamento e não existe log do que foi gerado — tudo isso soma linhas que
-- precisam estar em algum lugar.
--
-- O QUE FICA AQUI É O QUE A CONTABILIDADE MANDOU, cru, e mais nada:
--   - `folha` é o arquivo: competência, tipo (quinzena/fim de mês), quem
--     importou, e os totais que o relatório DECLARA;
--   - `folha_linha` é uma pessoa na folha, como ela veio.
--
-- ⚠️ A APROPRIAÇÃO (para qual obra vai cada real) **NÃO ENTRA AQUI**, e é de
-- propósito. Ela depende do ponto, do rateio e do ajuste à mão, e muda depois de
-- a folha estar importada. Misturar as duas coisas na mesma tabela faria uma
-- reimportação apagar o ajuste fino que alguém fez — e o ajuste é o trabalho
-- mais caro do processo. Ela vem numa migração própria, quando as telas por
-- verba existirem.
--
-- REIMPORTAR O MESMO ARQUIVO É NORMAL: o dono corrige algo na contabilidade e
-- manda de novo. Por isso existe a impressão do arquivo (`impressao`) e a
-- unicidade por competência + tipo: a segunda importação SUBSTITUI a primeira,
-- em vez de criar duas folhas do mesmo mês que ninguém saberia escolher.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS analisesps.folha (
    id               SERIAL PRIMARY KEY,
    -- Competência: 8/2026. Separada em dois números porque a tela filtra e
    -- ordena por ano e por mês — texto "08/2026" ordenaria dezembro antes de
    -- fevereiro.
    ano              INTEGER NOT NULL,
    mes              INTEGER NOT NULL CHECK (mes BETWEEN 1 AND 12),
    -- "quinzena" (dia 1 ao 15) ou "fim_de_mes" (16 ao último). É o que decide o
    -- período do ponto, então não pode ser adivinhado: quando o título do
    -- relatório não diz, a tela pergunta.
    tipo             TEXT NOT NULL CHECK (tipo IN ('quinzena', 'fim_de_mes')),

    titulo           TEXT NOT NULL DEFAULT '',   -- o título do relatório, como veio
    empresa          TEXT NOT NULL DEFAULT '',
    cnpj             TEXT NOT NULL DEFAULT '',
    arquivo_nome     TEXT NOT NULL DEFAULT '',
    -- A impressão do conteúdo do arquivo. Serve para dizer na tela "este é o
    -- MESMO arquivo que você já importou" — que é diferente de "é o mesmo mês".
    impressao        TEXT NOT NULL DEFAULT '',

    -- O QUE NÓS SOMAMOS, e o que o RELATÓRIO DECLARA, lado a lado. Guardar os
    -- dois é o que permite dizer "não fecha" depois, sem reabrir o arquivo — e
    -- um total que não fecha é a crítica mais importante desta área.
    total            NUMERIC(14,2) NOT NULL DEFAULT 0,
    total_declarado  NUMERIC(14,2),
    pessoas          INTEGER NOT NULL DEFAULT 0,
    pessoas_declaradas INTEGER,
    avisos           TEXT NOT NULL DEFAULT '',

    importado_em     TIMESTAMPTZ NOT NULL DEFAULT now(),
    importado_por    TEXT NOT NULL DEFAULT ''
);

-- UMA FOLHA POR COMPETÊNCIA E TIPO. Reimportar substitui; não acumula. Duas
-- folhas de 08/2026 quinzena deixariam qualquer total ambíguo, e ninguém
-- saberia qual é a boa.
CREATE UNIQUE INDEX IF NOT EXISTS ix_analisesps_folha_competencia
    ON analisesps.folha (ano, mes, tipo);

CREATE TABLE IF NOT EXISTS analisesps.folha_linha (
    id           SERIAL PRIMARY KEY,
    folha_id     INTEGER NOT NULL
                 REFERENCES analisesps.folha (id) ON DELETE CASCADE,
    -- O código do empregado no Fortes: "000013". TEXT COM OS ZEROS — como
    -- número viraria 13, e o casamento com o cadastro deixaria de funcionar.
    id_fortes    TEXT NOT NULL DEFAULT '',
    nome         TEXT NOT NULL DEFAULT '',
    -- O CPF não vem na Folha Sintética: ela traz código e nome. O CPF é
    -- resolvido depois, pelo cadastro, e fica aqui quando for. Vazio quer dizer
    -- "ainda não casou com ninguém" — e é a crítica que a tela mostra.
    cpf          TEXT NOT NULL DEFAULT '',
    valor        NUMERIC(14,2) NOT NULL,
    filial_codigo TEXT NOT NULL DEFAULT '',
    filial_nome  TEXT NOT NULL DEFAULT ''
);

-- A tela abre uma folha e lista as pessoas dela, em ordem de nome.
CREATE INDEX IF NOT EXISTS ix_analisesps_folha_linha_folha
    ON analisesps.folha_linha (folha_id, lower(nome));

-- E procura uma pessoa pelo código do Fortes (é por ele que se casa com o
-- cadastro) e pelo CPF, quando já resolvido.
CREATE INDEX IF NOT EXISTS ix_analisesps_folha_linha_fortes
    ON analisesps.folha_linha (folha_id, id_fortes);
CREATE INDEX IF NOT EXISTS ix_analisesps_folha_linha_cpf
    ON analisesps.folha_linha (cpf) WHERE cpf <> '';
