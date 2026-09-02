-- ===========================================================================
-- 001 — Espelho do OMIE
--
-- Tudo do painel vive no schema `painel`, nao no `public`. O ERP tem tabelas
-- chamadas titulos, rateios e categorias; o espelho do OMIE tem tabelas com os
-- mesmos nomes e significado completamente diferente. Schema separado e o que
-- garante que uma escrita do painel nunca alcance dado do ERP.
-- ===========================================================================
CREATE SCHEMA IF NOT EXISTS painel;

-- Titulos (contas a pagar e a receber) como o OMIE devolve.
CREATE TABLE IF NOT EXISTS painel.titulos (
    codigo_lancamento_omie    BIGINT PRIMARY KEY,
    natureza                  TEXT NOT NULL,   -- 'P' (pagar) / 'R' (receber)
    valor_documento           REAL,            -- BRUTO
    codigo_categoria          TEXT,
    codigo_cliente_fornecedor BIGINT,
    id_conta_corrente         BIGINT,
    numero_documento          TEXT,
    numero_documento_fiscal   TEXT,
    numero_pedido             TEXT,
    numero_parcela            TEXT,
    codigo_tipo_documento     TEXT,
    status_titulo             TEXT,
    observacao                TEXT,            -- campo livre do OMIE
    observacao_sync           TEXT,            -- marca que o backfill ja consultou
    data_emissao              TEXT,
    data_entrada              TEXT,
    data_registro             TEXT,
    data_previsao             TEXT,
    data_vencimento           TEXT,
    valor_ir                  REAL DEFAULT 0,
    valor_iss                 REAL DEFAULT 0,
    valor_inss                REAL DEFAULT 0,
    valor_pis                 REAL DEFAULT 0,
    valor_cofins              REAL DEFAULT 0,
    valor_csll                REAL DEFAULT 0,
    dinc                      TEXT,
    dalt                      TEXT,            -- data da ultima alteracao (incremental)
    halt                      TEXT,
    cimpapi                   TEXT,
    sync_em                   TEXT
);
CREATE INDEX IF NOT EXISTS ix_titulos_natureza  ON painel.titulos(natureza);
CREATE INDEX IF NOT EXISTS ix_titulos_status    ON painel.titulos(status_titulo);
CREATE INDEX IF NOT EXISTS ix_titulos_categoria ON painel.titulos(codigo_categoria);
CREATE INDEX IF NOT EXISTS ix_titulos_cliente   ON painel.titulos(codigo_cliente_fornecedor);
CREATE INDEX IF NOT EXISTS ix_titulos_dalt      ON painel.titulos(dalt);

-- Rateio: como o valor do titulo se divide entre departamentos (obras).
CREATE TABLE IF NOT EXISTS painel.rateio (
    codigo_lancamento_omie BIGINT NOT NULL,
    seq                    INTEGER NOT NULL,
    ccoddep                TEXT,
    cdesdep                TEXT,
    nperdep                REAL,
    nvaldep                REAL,
    PRIMARY KEY (codigo_lancamento_omie, seq)
);
CREATE INDEX IF NOT EXISTS ix_rateio_dep ON painel.rateio(ccoddep);

-- Onde cada sincronizacao parou (para o incremental nao rebaixar tudo).
CREATE TABLE IF NOT EXISTS painel.sync_state (
    entidade         TEXT PRIMARY KEY,
    ultima_dalt      TEXT,
    ultima_sync      TEXT,
    total_registros  INTEGER
);

-- Movimentos financeiros: a data de pagamento REAL e os valores realizados.
-- Um titulo pode ter varios (pagamentos parciais). O SQLite usava `rowid` como
-- chave tecnica; no Postgres isso nao existe, entao criamos uma coluna `id`.
CREATE TABLE IF NOT EXISTS painel.movimentos (
    id             BIGSERIAL PRIMARY KEY,
    ncodtitulo     BIGINT NOT NULL,
    cnatureza      TEXT,
    cgrupo         TEXT,
    cstatus        TEXT,
    ccodcateg      TEXT,
    ncodcc         BIGINT,
    ncodcliente    BIGINT,
    ddtpagamento   TEXT,           -- dd/mm/aaaa
    ddtvenc        TEXT,
    ddtemissao     TEXT,
    ddtregistro    TEXT,
    cliquidado     TEXT,           -- S / N
    nvalortitulo   REAL,
    nvalpago       REAL,
    nvalliquido    REAL,
    nvalaberto     REAL,
    njuros         REAL,
    nmulta         REAL,
    ndesconto      REAL,
    sync_em        TEXT
);
CREATE INDEX IF NOT EXISTS ix_mov_titulo ON painel.movimentos(ncodtitulo);
CREATE INDEX IF NOT EXISTS ix_mov_pgto   ON painel.movimentos(ddtpagamento);
CREATE INDEX IF NOT EXISTS ix_mov_liq    ON painel.movimentos(cliquidado);

-- Catalogo de categorias do plano financeiro do OMIE.
CREATE TABLE IF NOT EXISTS painel.cat (
    codigo             TEXT PRIMARY KEY,
    descricao          TEXT,
    categoria_superior TEXT,
    grupo              TEXT,
    natureza           TEXT,
    conta_inativa      TEXT,
    codigo_dre         TEXT,        -- vazio = nao entra no DRE
    transferencia      TEXT,        -- 'S' = transferencia entre contas
    descricao_dre      TEXT,        -- vira o Grupo do DRE
    totalizadora       TEXT,
    sync_em            TEXT
);

-- Clientes e fornecedores (vem juntos no ListarClientes do OMIE).
CREATE TABLE IF NOT EXISTS painel.clientes (
    codigo         BIGINT PRIMARY KEY,
    razao_social   TEXT,
    nome_fantasia  TEXT,
    cnpj_cpf       TEXT,
    sync_em        TEXT
);

-- Contas correntes: o titulo guarda so o codigo; o nome sai daqui.
CREATE TABLE IF NOT EXISTS painel.contas_correntes (
    codigo         BIGINT PRIMARY KEY,
    descricao      TEXT,
    tipo_conta     TEXT,
    codigo_banco   TEXT,
    agencia        TEXT,
    numero_conta   TEXT,
    inativa        TEXT,
    sync_em        TEXT
);

-- De-para departamento (obra) -> projeto, lido da planilha "C. Diarios".
CREATE TABLE IF NOT EXISTS painel.depto_projeto (
    ccoddep    TEXT PRIMARY KEY,
    projeto    TEXT,
    sync_em    TEXT
);
