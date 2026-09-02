-- ===========================================================================
-- 002 — O que as telas leem, o dicionario de categorias e a prestacao de contas
--
-- `fato` e a tabela que substitui o dados_omie.parquet. Cada linha e um pedaco
-- de titulo ja apropriado a uma obra. Antes o painel abria esse arquivo inteiro
-- na memoria: 4 MB em disco viravam 179 MB abertos, numa instancia de 2 GB
-- dividida com 14 modulos. Agora a soma e feita pelo banco e a tela recebe so o
-- numero que vai mostrar.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS painel.fato (
    id                  BIGSERIAL PRIMARY KEY,
    -- de qual titulo do OMIE esta linha veio (o titulo se divide entre obras)
    codigo_lancamento   BIGINT,
    tipo                TEXT,          -- '1. Contas a Receber' / '2. Contas a Pagar'
    analise             TEXT,          -- DRE / Fluxo de Caixa / TRF
    situacao            TEXT,          -- status do titulo no OMIE
    situacao_vencimento TEXT,          -- Quitado / Vencido / A vencer
    categoria           TEXT,
    grupo               TEXT,
    projeto             TEXT,
    departamento        TEXT,          -- a obra
    razao_social        TEXT,
    cnpj_cpf            TEXT,
    numero_documento    TEXT,
    pedido_compra       TEXT,
    conta_corrente      TEXT,
    observacao          TEXT,
    link                TEXT,
    data                DATE,          -- pagamento se quitado; senao vencimento
    ano                 INTEGER,
    mes                 INTEGER,
    pago_recebido       NUMERIC(16,2) NOT NULL DEFAULT 0,  -- executado (caixa)
    a_pagar_receber     NUMERIC(16,2) NOT NULL DEFAULT 0,  -- saldo em aberto
    juros               NUMERIC(16,2) NOT NULL DEFAULT 0,
    multa               NUMERIC(16,2) NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS ix_fato_ano     ON painel.fato(ano);
CREATE INDEX IF NOT EXISTS ix_fato_analise ON painel.fato(analise);
CREATE INDEX IF NOT EXISTS ix_fato_tipo    ON painel.fato(tipo);
CREATE INDEX IF NOT EXISTS ix_fato_proj    ON painel.fato(projeto);
CREATE INDEX IF NOT EXISTS ix_fato_dep     ON painel.fato(departamento);
CREATE INDEX IF NOT EXISTS ix_fato_grupo   ON painel.fato(grupo);
CREATE INDEX IF NOT EXISTS ix_fato_cat     ON painel.fato(categoria);
CREATE INDEX IF NOT EXISTS ix_fato_data    ON painel.fato(data);

-- Uma linha por recebimento efetivo (data e valor exatos). Alimenta a Receita
-- Analitico, que abre cada medicao nos recebimentos que a quitaram.
CREATE TABLE IF NOT EXISTS painel.fato_recebimentos (
    id                  BIGSERIAL PRIMARY KEY,
    codigo_lancamento   BIGINT,
    tipo                TEXT,
    analise             TEXT,
    tipo_receita        TEXT,
    situacao            TEXT,
    categoria           TEXT,
    grupo               TEXT,
    projeto             TEXT,
    departamento        TEXT,
    razao_social        TEXT,
    cnpj_cpf            TEXT,
    numero_documento    TEXT,
    observacao          TEXT,
    medicao             TEXT,
    conta_corrente      TEXT,
    link                TEXT,
    data                DATE,
    ano                 INTEGER,
    mes                 INTEGER,
    valor               NUMERIC(16,2) NOT NULL DEFAULT 0,
    juros               NUMERIC(16,2) NOT NULL DEFAULT 0,
    multa               NUMERIC(16,2) NOT NULL DEFAULT 0,
    desconto            NUMERIC(16,2) NOT NULL DEFAULT 0,
    valor_movimento     NUMERIC(16,2) NOT NULL DEFAULT 0,
    rateio_pct          NUMERIC(9,4)  NOT NULL DEFAULT 0,
    parcela             TEXT,
    recebimentos        INTEGER,
    total_medicao       NUMERIC(16,2) NOT NULL DEFAULT 0,
    origem              TEXT
);
CREATE INDEX IF NOT EXISTS ix_receb_ano  ON painel.fato_recebimentos(ano);
CREATE INDEX IF NOT EXISTS ix_receb_proj ON painel.fato_recebimentos(projeto);
CREATE INDEX IF NOT EXISTS ix_receb_dep  ON painel.fato_recebimentos(departamento);

-- Dicionario categoria -> (analise, grupo). Substitui o dados_log.parquet, um
-- arquivo de 14 MB cuja unica funcao era ensinar essa correspondencia. Quando o
-- proprio OMIE informa a conta do DRE, ele manda; isto aqui e a rede de seguranca
-- para categoria antiga que o OMIE nao classifica.
CREATE TABLE IF NOT EXISTS painel.categoria_de_para (
    categoria  TEXT PRIMARY KEY,   -- descricao da categoria, como aparece no OMIE
    analise    TEXT,               -- DRE / Fluxo de Caixa / TRF
    grupo      TEXT,
    origem     TEXT                -- 'log' (aprendido do arquivo antigo) / 'manual'
);

-- Estado e historico das sincronizacoes (o que a tela mostra como "dados de").
CREATE TABLE IF NOT EXISTS painel.execucoes (
    id           BIGSERIAL PRIMARY KEY,
    tipo         TEXT NOT NULL,     -- 'incremental' / 'completa' / 'so_fato'
    disparo      TEXT NOT NULL,     -- 'agendado' / 'manual'
    inicio       TIMESTAMPTZ NOT NULL DEFAULT now(),
    fim          TIMESTAMPTZ,
    ok           BOOLEAN,
    mensagem     TEXT,
    linhas_fato  INTEGER
);
CREATE INDEX IF NOT EXISTS ix_exec_inicio ON painel.execucoes(inicio DESC);

-- ------------------------------------------------------------------ prestacao
-- Configuracao digitada pelo usuario: socios, percentuais, regras de rateio e
-- ajustes manuais. Isto NAO e dado baixado do OMIE — nao da para regenerar. E a
-- razao principal de o painel usar banco e nao arquivo: o disco do Render e
-- apagado a cada reinicio, e isto sumiria junto.
CREATE TABLE IF NOT EXISTS painel.socios (
    id     BIGSERIAL PRIMARY KEY,
    nome   TEXT UNIQUE NOT NULL,
    tipo   TEXT NOT NULL DEFAULT 'Interno',   -- Interno / Externo
    ativo  INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS painel.participacoes (
    id       BIGSERIAL PRIMARY KEY,
    projeto  TEXT NOT NULL,
    socio_id BIGINT NOT NULL REFERENCES painel.socios(id) ON DELETE CASCADE,
    pct      REAL NOT NULL DEFAULT 0,
    UNIQUE (projeto, socio_id)
);

CREATE TABLE IF NOT EXISTS painel.regras (
    id          BIGSERIAL PRIMARY KEY,
    nome        TEXT NOT NULL,
    depto       TEXT NOT NULL,
    todas       INTEGER NOT NULL DEFAULT 0,
    grupos      TEXT NOT NULL DEFAULT '[]',
    categorias  TEXT NOT NULL DEFAULT '[]',
    pct         REAL NOT NULL DEFAULT 100,
    escopo      TEXT NOT NULL DEFAULT 'AMBAS',   -- AMBAS / FILIAL / MATRIZ
    mes_ini     TEXT NOT NULL DEFAULT '',
    mes_fim     TEXT NOT NULL DEFAULT '',
    ativo       INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS painel.ajustes (
    id         BIGSERIAL PRIMARY KEY,
    socio_id   BIGINT NOT NULL REFERENCES painel.socios(id) ON DELETE CASCADE,
    projeto    TEXT NOT NULL DEFAULT '',
    data       TEXT NOT NULL DEFAULT '',
    tipo       TEXT NOT NULL,
    valor      REAL NOT NULL,
    descricao  TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS painel.config (
    chave  TEXT PRIMARY KEY,
    valor  TEXT NOT NULL
);

INSERT INTO painel.config (chave, valor) VALUES
    ('projeto_matriz',      'BWSCE'),
    ('depto_admin_matriz',  'BWS Construções'),
    ('depto_admin_filial',  'BWSNE'),
    ('grupo_pessoal',       'Despesas com Pessoal'),
    ('taxa_adm_pct',        '1.5'),
    ('residual',            '1')
ON CONFLICT (chave) DO NOTHING;
