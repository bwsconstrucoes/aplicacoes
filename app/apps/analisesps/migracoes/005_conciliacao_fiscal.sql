-- ---------------------------------------------------------------------------
-- 005 — A CONCILIACAO FISCAL
--
-- Pedido do dono em 11/09/2026, levantado inteiro em `CONCILIACAO_FISCAL.md`.
-- O objetivo, nas palavras dele: a conciliacao das notas emitidas contra o
-- CNPJ da BWS, "da forma mais automatica, mais com seguranca possivel".
--
-- O QUE ESTA MIGRACAO **NAO** CRIA, e e a decisao mais importante: um cadastro
-- de notas fiscais. Perguntado onde esse cadastro deveria morar, o dono
-- respondeu: *"nao existe. A gente usa o Pipefy para armazenar essas notas."*
--
-- O CARD CONTINUA SENDO O LUGAR DA NOTA. O ERP tambem esta construindo o
-- cruzamento dele (migracao 044 de la), e dois cadastros decidindo sobre as
-- mesmas notas divergiriam. Aqui nao ha segundo cadastro: ha o RELATORIO que
-- o FSist entrega, e o DIARIO do que este modulo escreveu no card.
--
-- Duas tabelas, e cada uma tem um dono diferente da verdade:
--
--   notas_fiscais   copia do relatorio do FSist. Ele manda; aqui so se guarda.
--                   Some e volta quando o relatorio e reimportado.
--   sp_fiscal_analise  o que ESTE modulo decidiu e mandou para o card. E o
--                   unico lugar onde essa memoria existe, porque a planilha
--                   SPsBD nao tem esses campos e o dono decidiu NAO mexer
--                   nela.
-- ---------------------------------------------------------------------------


-- ---------------------------------------------------------------------------
-- AS NOTAS DO RELATORIO DO FSIST
--
-- A CHAVE E A IDENTIDADE. Sao 44 digitos definidos pela Receita, unicos por
-- nota no pais inteiro. Por isso ela e a chave primaria, e nao um numero
-- sequencial nosso: reimportar o mesmo relatorio nao pode duplicar nada, e o
-- script que roda na planilha hoje ja acertava nisso.
--
-- OS DIGITOS 7 A 20 DA CHAVE SAO O CNPJ DE QUEM EMITIU. Nao e curiosidade: e
-- o que permite casar a nota com o credor do lancamento mesmo quando a coluna
-- do emitente vem suja. Fica gravado em `emitente_doc` ja extraido.
--
-- `status` importa mais do que parece: nota CANCELADA paga e problema fiscal,
-- e o relatorio novo corrige o status de uma nota que ja estava aqui.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analisesps.notas_fiscais (
    chave           TEXT PRIMARY KEY,
    emissao         DATE,
    numero          TEXT NOT NULL DEFAULT '',
    serie           TEXT NOT NULL DEFAULT '',
    tipo            TEXT NOT NULL DEFAULT '',      -- NFe, CTe...
    valor           NUMERIC(14,2),
    status          TEXT NOT NULL DEFAULT '',      -- Autorizada, Cancelada...
    emitente_doc    TEXT NOT NULL DEFAULT '',      -- so digitos
    emitente        TEXT NOT NULL DEFAULT '',
    emitente_uf     TEXT NOT NULL DEFAULT '',
    destinatario_doc TEXT NOT NULL DEFAULT '',     -- so digitos (a BWS)
    destinatario    TEXT NOT NULL DEFAULT '',
    chaves_nfe      TEXT NOT NULL DEFAULT '',      -- so no CT-e: as NFes do frete
    importada_em    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- O caminho de busca da conciliacao: por quem emitiu, e por numero da nota.
-- Sem eles, cada lancamento a conciliar varreria as notas todas.
CREATE INDEX IF NOT EXISTS ix_notas_emitente ON analisesps.notas_fiscais (emitente_doc);
CREATE INDEX IF NOT EXISTS ix_notas_numero   ON analisesps.notas_fiscais (numero);
CREATE INDEX IF NOT EXISTS ix_notas_emissao  ON analisesps.notas_fiscais (emissao);


-- ---------------------------------------------------------------------------
-- O DIARIO DA ANALISE FISCAL DE CADA SP
--
-- Uma linha por SP. Guarda o que foi DECIDIDO e o que foi ESCRITO no card —
-- duas coisas diferentes, e a diferenca e o que permite tentar de novo quando
-- o Pipefy recusa.
--
-- `situacao` e o estado da analise, e nao a categoria fiscal:
--
--   PENDENTE    ninguem olhou ainda
--   PROPOSTA    o sistema achou candidata e espera confirmacao de gente
--   CONFIRMADA  uma pessoa confirmou; ainda nao foi para o card
--   ESCRITA     foi para o card, e o card respondeu que aceitou
--   SEM_PAR     procurou-se e nao ha nota que sirva
--   FUTURA      pagamento antecipado, a nota sera emitida depois. NAO E FIM
--               DE LINHA: volta para a fila a cada relatorio novo do FSist,
--               por decisao do dono.
--   IGNORADA    nao se aplica (Fundo Fixo, Seguros, Contrato, Taxas...)
--
-- POR QUE `origem` EXISTE. Uma chave pode ter vindo de tres lugares: ja estava
-- no Pipefy antes deste modulo (importada uma vez do relatorio), foi casada
-- pela conciliacao, ou foi lida de um anexo pela IA. Quando um numero sair
-- errado la na frente, a primeira pergunta vai ser "de onde veio isso" — e a
-- resposta tem de estar gravada, nao deduzida.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analisesps.sp_fiscal_analise (
    sp_id           TEXT PRIMARY KEY,
    situacao        TEXT NOT NULL DEFAULT 'PENDENTE',
    documentacao    TEXT NOT NULL DEFAULT '',      -- a opcao do campo do Pipefy
    chave           TEXT NOT NULL DEFAULT '',
    numero_nota     TEXT NOT NULL DEFAULT '',
    gerou_nota      TEXT NOT NULL DEFAULT '',      -- Sim / Nao
    dedutivel       BOOLEAN,                       -- derivado da documentacao
    confianca       INTEGER NOT NULL DEFAULT 0,    -- 0 a 100
    origem          TEXT NOT NULL DEFAULT '',      -- PIPEFY, CONCILIACAO, IA, PESSOA
    motivo          TEXT NOT NULL DEFAULT '',      -- por que esta assim, em portugues
    link_nota       TEXT NOT NULL DEFAULT '',      -- a nota guardada no Drive
    decidida_por    TEXT NOT NULL DEFAULT '',
    decidida_em     TIMESTAMPTZ,
    escrita_em      TIMESTAMPTZ,                   -- quando o card aceitou
    erro_escrita    TEXT NOT NULL DEFAULT '',
    atualizada_em   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A tela filtra por situacao e por dedutibilidade o tempo todo; sao as duas
-- perguntas que o dono disse que precisa responder ("o que esta resolvido, o
-- que e dedutivel, o que nao e, por que esta pendente").
CREATE INDEX IF NOT EXISTS ix_fiscal_situacao  ON analisesps.sp_fiscal_analise (situacao);
CREATE INDEX IF NOT EXISTS ix_fiscal_dedutivel ON analisesps.sp_fiscal_analise (dedutivel);
CREATE INDEX IF NOT EXISTS ix_fiscal_chave     ON analisesps.sp_fiscal_analise (chave);


-- ---------------------------------------------------------------------------
-- A MESMA CHAVE EM DUAS SPs E ERRO — MAS NEM SEMPRE
--
-- O parcelamento e legitimo: uma nota de dez mil paga em tres parcelas sao
-- tres SPs com a mesma chave. O script da planilha ja distinguia isso pela
-- etiqueta "Parcela N", e a distincao se mantem no codigo.
--
-- Por isso NAO ha unicidade em `chave` aqui: o banco nao consegue julgar o
-- caso, e uma trava que precisa ser desligada metade das vezes e pior do que
-- nenhuma. Quem aponta a duplicidade suspeita e a conciliacao, na tela, com o
-- motivo escrito.
-- ---------------------------------------------------------------------------
