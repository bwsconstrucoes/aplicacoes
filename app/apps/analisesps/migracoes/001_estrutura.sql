-- ===========================================================================
-- 001 — A estrutura da Análise de SPs
--
-- Tudo vive no schema `analisesps`, dentro do MESMO Postgres do ERP. O schema
-- separado não é preciosismo: o ERP tem tabelas de nome genérico (`titulos`,
-- `categorias`, `rateios`) e a chance de colisão é real. Separados, os dois
-- convivem sem renomear nada e sem risco de uma escrita daqui encostar em dado
-- do ERP.
--
-- A base vem da planilha SPsBD (aba SPsBD, colunas A:AL) e é REGENERÁVEL: se
-- esta tabela sumisse, a sincronização a refaz. O que NÃO é regenerável é a
-- fila de escrita e o registro de alterações — por isso os dois moram aqui, e
-- não em arquivo no disco do Render, que é apagado a cada reinício.
-- ===========================================================================

CREATE SCHEMA IF NOT EXISTS analisesps;


-- ---------------------------------------------------------------------------
-- As solicitações de pagamento
--
-- As colunas de texto guardam o que a planilha manda, cru. As quatro colunas
-- terminadas em `_d` e a `valor_num` são as MESMAS informações já convertidas
-- para data e número.
--
-- Por que guardar as duas formas: a planilha escreve "6.750,00" e "31/12/2026",
-- que como texto ordenam errado ("10/01" viria antes de "9/01") e não somam. A
-- versão convertida é o que as telas filtram, ordenam e somam — e é o que
-- permite o Postgres fazer a conta, em vez de mandar 59 mil linhas para a tela.
-- A versão crua fica porque é a verdade da planilha: se a conversão errar num
-- caso esquisito, o original está aqui para conferir.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analisesps.sps (
    id                 TEXT PRIMARY KEY,
    solicitacao        TEXT,
    vencimento         TEXT,
    credor             TEXT,
    documento          TEXT,
    descricao          TEXT,
    valor              TEXT,
    centro_custo       TEXT,
    tipo_despesa       TEXT,
    forma_pagamento    TEXT,
    responsavel        TEXT,
    dt_autorizacao     TEXT,
    resp_autorizacao   TEXT,
    status_aut         TEXT,
    status_pgt         TEXT,
    codigo_integracao  TEXT,
    anexo_link         TEXT,
    card_link          TEXT,
    projeto            TEXT,
    conta              TEXT,
    carimbo            TEXT,
    data_pagamento     TEXT,
    info_pgt           TEXT,
    parcela            TEXT,
    nf                 TEXT,
    agendado           TEXT,
    pedido             TEXT,
    anuente            TEXT,
    status_anuencia    TEXT,
    comprovante        TEXT,
    validacao          TEXT,
    codigo_barras      TEXT,
    id_contrato        TEXT,
    analise_ia         TEXT,

    -- as mesmas informações, já convertidas (ver o comentário acima)
    valor_num          NUMERIC(14,2),
    solicitacao_d      DATE,
    vencimento_d       DATE,
    data_pagamento_d   DATE,
    dt_autorizacao_d   DATE,

    atualizado_em      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Os índices cobrem exatamente os filtros da tela de Solicitações. Sem eles o
-- Postgres varre as 59 mil linhas a cada clique; com eles, vai direto.
CREATE INDEX IF NOT EXISTS ix_sps_status_pgt   ON analisesps.sps(status_pgt);
CREATE INDEX IF NOT EXISTS ix_sps_vencimento   ON analisesps.sps(vencimento_d);
CREATE INDEX IF NOT EXISTS ix_sps_pagamento    ON analisesps.sps(data_pagamento_d);
CREATE INDEX IF NOT EXISTS ix_sps_solicitacao  ON analisesps.sps(solicitacao_d);
CREATE INDEX IF NOT EXISTS ix_sps_centro_custo ON analisesps.sps(centro_custo);
CREATE INDEX IF NOT EXISTS ix_sps_credor       ON analisesps.sps(credor);
CREATE INDEX IF NOT EXISTS ix_sps_carimbo      ON analisesps.sps(carimbo);
-- Busca por credor sem diferenciar maiúscula de minúscula.
CREATE INDEX IF NOT EXISTS ix_sps_credor_lower ON analisesps.sps(lower(credor));


-- ---------------------------------------------------------------------------
-- Chave/valor de controle: último carimbo lido, hora da última sincronização.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analisesps.meta (
    chave  TEXT PRIMARY KEY,
    valor  TEXT
);


-- ---------------------------------------------------------------------------
-- FILA DE ESCRITA para a planilha — a tabela que NÃO pode se perder.
--
-- Toda alteração feita na tela é aplicada aqui na hora (o operador vê o efeito
-- imediatamente) e fica nesta fila até o Google confirmar a gravação. Se a
-- internet cair, se a cota do Google estourar, se o serviço reiniciar no meio
-- — a alteração continua aqui e é reenviada sozinha.
--
-- Uma linha por CÉLULA (SP + coluna): reescrever a mesma célula substitui o
-- valor pendente, e o último vale. Sem isso, duas trocas de status seguidas
-- virariam duas gravações, e a ordem entre elas não seria garantida.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analisesps.fila (
    sp_id       TEXT NOT NULL,
    coluna      TEXT NOT NULL,
    valor       TEXT,
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    tentativas  INTEGER NOT NULL DEFAULT 0,
    ultimo_erro TEXT,
    PRIMARY KEY (sp_id, coluna)
);


-- ---------------------------------------------------------------------------
-- Registro permanente de alterações. Só cresce, nunca reescreve linha.
--
-- Guarda o PERFIL que alterou, não a pessoa — ver a explicação em `auth.py`.
-- Quando o cadastro de usuários do ERP passar a valer aqui, esta coluna vira o
-- nome de quem fez, e o histórico antigo continua legível.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analisesps.log_alteracoes (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sp_id           TEXT,
    coluna          TEXT,
    valor           TEXT,
    valor_anterior  TEXT,
    acao            TEXT,
    perfil          TEXT,
    status          TEXT NOT NULL DEFAULT 'pendente',
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    enviado_em      TIMESTAMPTZ,
    erro            TEXT
);
CREATE INDEX IF NOT EXISTS ix_log_criado ON analisesps.log_alteracoes(criado_em DESC);
CREATE INDEX IF NOT EXISTS ix_log_sp     ON analisesps.log_alteracoes(sp_id);


-- ---------------------------------------------------------------------------
-- Apoios lidos de outras planilhas.
-- ---------------------------------------------------------------------------
-- C. Diários: centro de custo -> conta de pagamento
CREATE TABLE IF NOT EXISTS analisesps.contas_diarios (
    codigo           TEXT PRIMARY KEY,
    conta_pagamento  TEXT
);

-- Lançamentos: SP -> documentação fiscal
CREATE TABLE IF NOT EXISTS analisesps.sp_fiscal (
    sp_id        TEXT PRIMARY KEY,
    doc_fiscal   TEXT
);


-- ---------------------------------------------------------------------------
-- ANDAMENTO DAS TAREFAS LONGAS — no banco desde o primeiro dia.
--
-- No painel isto só veio na migração 005, depois de doer: enquanto o andamento
-- vivia na memória do processo, uma carga de horas dizia apenas "carregando",
-- e um reinício do serviço fazia a execução sumir da tela — que então mostrava
-- a falha ANTERIOR como se fosse a atual. Aqui já nasce certo.
--
-- `visto_em` é o batimento: enquanto viva, a execução carimba a hora aqui. Se
-- parar de carimbar, morreu — e a tela consegue dizer isso, em vez de fingir
-- que ainda está rodando.
--
-- `etapas_ok` é a retomada: guarda quais etapas já terminaram, para uma carga
-- interrompida recomeçar de onde parou em vez de tudo de novo.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analisesps.execucoes (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tipo        TEXT NOT NULL,
    disparo     TEXT NOT NULL DEFAULT 'manual',
    inicio      TIMESTAMPTZ NOT NULL DEFAULT now(),
    fim         TIMESTAMPTZ,
    ok          BOOLEAN,
    mensagem    TEXT,
    etapa       TEXT,
    progresso   TEXT,
    etapas_ok   TEXT,
    linhas      INTEGER,
    visto_em    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_exec_abertas ON analisesps.execucoes(fim)
    WHERE fim IS NULL;
CREATE INDEX IF NOT EXISTS ix_exec_inicio  ON analisesps.execucoes(inicio DESC);
