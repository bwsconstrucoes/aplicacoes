-- ---------------------------------------------------------------------------
-- A CIENCIA DA OPERACAO E O ARQUIVO DA NOTA — 17/09/2026
--
-- Pedido do dono: *"tem como visualizar facil a partir da tela de associacao,
-- clicar e ver a nota fiscal? (...) se esse PDF nao pudesse ser gerado de
-- imediato, que ficasse um processamento apos a baixa das notas, baixando,
-- gerando esses PDFs e salvando no Google Drive."*
--
-- E a autorizacao, com todas as letras: *"pode baixar as notas dando essa
-- ciencia."*
--
-- ⚠️ POR QUE ISTO PRECISA DE REGISTRO PROPRIO, e nao e so mais uma coluna:
--
-- A "ciencia da operacao" (evento 210210) NAO E UMA LEITURA. E uma declaracao
-- assinada com o certificado A1, em nome da BWS, que fica gravada no historico
-- daquela nota na Receita para sempre. Tres coisas decorrem disso:
--
--   1. TEM DE SER AUDITAVEL: quando foi enviada, o que a Receita respondeu, e
--      se deu certo. Sem isso ninguem consegue responder "por que a BWS deu
--      ciencia nesta nota?" seis meses depois.
--   2. NAO PODE SER REPETIDA a esmo: a Receita recusa a segunda ciencia da
--      mesma nota, e insistir e o caminho para o bloqueio por consumo indevido.
--   3. TEM PRAZO: a ciencia so vale dentro de ~90 dias da emissao. Nota velha
--      nao aceita mais, e tentar e gastar chamada a toa.
--
-- `nota_evento` guarda cada tentativa. `nota_arquivo` guarda onde o documento
-- ficou depois de baixado — o XML e a unica coisa que tem valor fiscal; o PDF
-- e representacao. Por isso o tipo e explicito.
--
-- ⚠️ O ARQUIVO NAO FICA NO BANCO. Um XML de NF-e tem de 10 a 50 KB; seis mil
-- notas seriam centenas de megabytes num Postgres de 0,25 GB de RAM que ja
-- morreu de memoria uma vez. Vai para o Drive, e aqui fica so o endereco.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analisesps.nota_evento (
    chave        TEXT NOT NULL,
    tipo         TEXT NOT NULL,              -- '210210' (ciencia da operacao)
    enviado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    ok           BOOLEAN NOT NULL DEFAULT FALSE,
    codigo       TEXT NOT NULL DEFAULT '',   -- cStat da Receita
    motivo       TEXT NOT NULL DEFAULT '',   -- xMotivo, com as palavras dela
    protocolo    TEXT NOT NULL DEFAULT '',
    quem         TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (chave, tipo)
);

-- "O que ainda falta manifestar" e a pergunta da rotina; "o que deu errado" e
-- a pergunta de quem cuida.
CREATE INDEX IF NOT EXISTS ix_nota_evento_ok ON analisesps.nota_evento (ok);

CREATE TABLE IF NOT EXISTS analisesps.nota_arquivo (
    chave        TEXT NOT NULL,
    tipo         TEXT NOT NULL,              -- 'xml' | 'pdf'
    link         TEXT NOT NULL DEFAULT '',   -- endereco no Drive
    arquivo_id   TEXT NOT NULL DEFAULT '',   -- id no Drive, para substituir
    tamanho      INTEGER NOT NULL DEFAULT 0,
    guardado_em  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chave, tipo)
);

CREATE INDEX IF NOT EXISTS ix_nota_arquivo_chave
    ON analisesps.nota_arquivo (chave);
