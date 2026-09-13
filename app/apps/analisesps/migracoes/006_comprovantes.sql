-- ---------------------------------------------------------------------------
-- 006 — OS COMPROVANTES ARRASTADOS PARA DENTRO DA TELA
--
-- Pedido do dono em 11/09/2026: *"eu arrasto esses comprovantes pra dentro e
-- dispara a automacao, sem nem precisar passar pelo Make."*
--
-- O QUE ESTA MIGRACAO **NAO** CRIA, e e a decisao que poupou um modulo
-- inteiro: nada de baixa. O robo que da a baixa JA EXISTE e roda em producao
-- ha meses (`baixabradesco`): ele recebe o PDF, descobre a SP, baixa no Omie,
-- marca paga na SPsBD, move o card no Pipefy e guarda o comprovante. Aqui so
-- se cria a PORTA DE ENTRADA e a MEMORIA do que aconteceu.
--
-- POR QUE A MEMORIA PRECISA DE TABELA. O dono perguntou com todas as letras:
-- *"se eu sair da tela e voltar, a informacao vai ser me dada ainda ou eu vou
-- perder?"* Se o resultado morasse na tela, trocar de aba perderia tudo — e o
-- servico ainda reinicia sozinho a cada ~150 requisicoes. No banco, ele
-- sobrevive a navegacao, a reinicio e ao dia seguinte.
--
-- E ELE HOJE E JOGADO FORA. O robo ja devolve, a cada leva, quais baixaram,
-- quais nao localizaram a SP, quais estavam duplicados, quais estao pendentes
-- de validacao e quais foram recusados — com o motivo escrito. Isso volta para
-- o Make.com e morre la. Estas duas tabelas sao o lugar onde isso passa a
-- ficar.
--
-- O PDF NAO ENTRA NO BANCO. Ele fica em disco enquanto e processado e e
-- apagado depois. O banco tem 1 GB e ja usa 430 MB; guardar comprovante ali
-- encheria o disco em semanas, e o comprovante ja e guardado pelo robo.
-- ---------------------------------------------------------------------------

-- Uma linha por ARQUIVO solto na tela.
CREATE TABLE IF NOT EXISTS analisesps.comprovantes_lote (
    id            SERIAL PRIMARY KEY,
    pessoa        TEXT NOT NULL DEFAULT '',      -- quem soltou (para a tela dele)
    quem          TEXT NOT NULL DEFAULT '',      -- o nome, para o historico
    arquivo       TEXT NOT NULL DEFAULT '',      -- o nome original do arquivo
    caminho       TEXT NOT NULL DEFAULT '',      -- onde o PDF esta no disco
    paginas       INTEGER NOT NULL DEFAULT 0,
    -- ESPERANDO -> RODANDO -> PRONTO | FALHOU
    situacao      TEXT NOT NULL DEFAULT 'ESPERANDO',
    levas         INTEGER NOT NULL DEFAULT 0,    -- em quantas levas foi partido
    levas_feitas  INTEGER NOT NULL DEFAULT 0,
    erro          TEXT NOT NULL DEFAULT '',
    recebido_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    terminado_em  TIMESTAMPTZ
);

-- A fila e lida por situacao a cada disparo; o historico, por data.
CREATE INDEX IF NOT EXISTS ix_comprov_situacao ON analisesps.comprovantes_lote (situacao);
CREATE INDEX IF NOT EXISTS ix_comprov_recebido ON analisesps.comprovantes_lote (recebido_em DESC);


-- Uma linha por COMPROVANTE — que e uma PAGINA, nao um arquivo. O robo trata
-- cada pagina do PDF como um comprovante separado, e e assim que o dono manda
-- hoje: um PDF de cinquenta paginas sao cinquenta comprovantes.
CREATE TABLE IF NOT EXISTS analisesps.comprovantes_item (
    id          SERIAL PRIMARY KEY,
    lote_id     INTEGER NOT NULL
                REFERENCES analisesps.comprovantes_lote (id) ON DELETE CASCADE,
    pagina      INTEGER,
    -- BAIXADO | DUPLICADO | NAO_LOCALIZADO | PENDENTE_VALIDACAO | RECUSADO | ERRO
    situacao    TEXT NOT NULL DEFAULT '',
    sp_id       TEXT NOT NULL DEFAULT '',
    valor       TEXT NOT NULL DEFAULT '',
    recebedor   TEXT NOT NULL DEFAULT '',
    -- Em portugues, e e o que a tela mostra: "por que este nao baixou".
    motivo      TEXT NOT NULL DEFAULT '',
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_comprov_item_lote ON analisesps.comprovantes_item (lote_id);
-- A pergunta que hoje nao tem resposta em lugar nenhum: "aquele comprovante da
-- semana passada que nao casou, o que deu nele?"
CREATE INDEX IF NOT EXISTS ix_comprov_item_situacao ON analisesps.comprovantes_item (situacao);
CREATE INDEX IF NOT EXISTS ix_comprov_item_sp ON analisesps.comprovantes_item (sp_id);
