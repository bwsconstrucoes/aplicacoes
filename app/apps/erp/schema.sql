-- ============================================================================
-- BWS ERP — SCHEMA DO NÚCLEO FINANCEIRO — v1.0
-- PostgreSQL 15+ (Render Managed PostgreSQL)
--
-- PRINCÍPIOS DE MODELAGEM (não violar em nenhuma evolução futura):
--   1. IMUTABILIDADE CONTÁBIL: títulos e pagamentos confirmados nunca são
--      editados nem apagados. Correção = estorno (novo registro que referencia
--      o original via estorna_titulo_id) ou reclassificação (novo rateio
--      substituindo o anterior, com trilha em eventos).
--   2. OBRIGAÇÃO ≠ CAIXA: titulos/parcelas registram O QUE se deve e QUANDO
--      (competência); pagamentos registram O QUE saiu e QUANDO (caixa).
--   3. DADOS BANCÁRIOS PERTENCEM AO CADASTRO: o título SELECIONA uma conta
--      homologada do fornecedor (fornecedor_contas).
--   4. TRILHA APPEND-ONLY: a tabela eventos só recebe INSERT.
--   5. CÓDIGOS DE INTEGRAÇÃO: colunas codigo_omie / ref_sheets / ref_pipefy
--      mantêm o vínculo com os espelhos durante a transição.
-- ============================================================================

BEGIN;

CREATE TYPE tipo_pessoa        AS ENUM ('PF', 'PJ');
CREATE TYPE regime_tributario  AS ENUM ('SIMPLES', 'LUCRO_PRESUMIDO', 'LUCRO_REAL', 'MEI', 'NAO_INFORMADO');
CREATE TYPE perfil_usuario     AS ENUM ('ADMIN', 'FINANCEIRO', 'APROVADOR', 'LANCADOR', 'CONSULTA');

CREATE TYPE tipo_titulo AS ENUM (
    'T1_MATERIAL_NFE', 'T2_SERVICO_NFSE', 'T3_FRETE_CTE', 'T4_LOCACAO',
    'T5_EMPREITEIRO', 'T6_SERVICO_PF_RPA', 'T7_FOLHA_ENCARGOS', 'T8_TRIBUTO_GUIA',
    'T9_CONCESSIONARIA', 'T10_FUNDO_FIXO', 'T11_ADIANTAMENTO', 'T12_REEMBOLSO',
    'T13_FINANCIAMENTO', 'T14_EXCECAO_SEM_NOTA'
);

CREATE TYPE forma_pagamento AS ENUM ('BOLETO', 'PIX', 'TED', 'DEBITO_AUTOMATICO', 'GUIA', 'DINHEIRO');

CREATE TYPE status_titulo AS ENUM (
    'RASCUNHO', 'EM_ANALISE', 'DEVOLVIDO', 'AGUARDANDO_APROVACAO', 'APROVADO',
    'BLOQUEADO', 'PAGO_PARCIAL', 'PAGO', 'CANCELADO', 'ESTORNADO'
);

CREATE TYPE status_parcela   AS ENUM ('ABERTA', 'AGENDADA', 'PAGA', 'CANCELADA');
CREATE TYPE status_conta     AS ENUM ('PENDENTE', 'HOMOLOGADA', 'BLOQUEADA');
CREATE TYPE tipo_doc_fiscal  AS ENUM ('NFE', 'NFSE', 'CTE', 'NFCE', 'FATURA', 'RECIBO', 'CONTRATO', 'OUTRO');
CREATE TYPE situacao_nota    AS ENUM ('AUTORIZADA', 'CANCELADA', 'DENEGADA', 'DESCONHECIDA');
CREATE TYPE tipo_retencao    AS ENUM ('INSS', 'ISS', 'IRRF', 'PCC');
CREATE TYPE destino_sync     AS ENUM ('OMIE', 'SHEETS', 'PIPEFY');
CREATE TYPE status_sync      AS ENUM ('PENDENTE', 'PROCESSANDO', 'OK', 'ERRO', 'DESCARTADO');

CREATE TABLE usuarios (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nome            TEXT        NOT NULL,
    email           TEXT        NOT NULL UNIQUE,
    senha_hash      TEXT        NOT NULL,
    perfil          perfil_usuario NOT NULL DEFAULT 'CONSULTA',
    ativo           BOOLEAN     NOT NULL DEFAULT TRUE,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE alcadas (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    categoria_id    BIGINT      NULL,
    obra_id         BIGINT      NULL,
    valor_max       NUMERIC(14,2) NOT NULL,
    perfil_minimo   perfil_usuario NOT NULL,
    ativo           BOOLEAN     NOT NULL DEFAULT TRUE,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE fornecedores (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tipo_pessoa         tipo_pessoa NOT NULL,
    cnpj_cpf            TEXT        NOT NULL UNIQUE,
    razao_social        TEXT        NOT NULL,
    nome_fantasia       TEXT,
    regime_tributario   regime_tributario NOT NULL DEFAULT 'NAO_INFORMADO',
    cnae_principal      TEXT,
    email               TEXT,
    telefone            TEXT,
    municipio           TEXT,
    uf                  CHAR(2),
    situacao_rfb        TEXT,
    situacao_rfb_em     TIMESTAMPTZ,
    data_abertura       DATE,
    codigo_omie         BIGINT      UNIQUE,
    observacoes         TEXT,
    ativo               BOOLEAN     NOT NULL DEFAULT TRUE,
    criado_em           TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_fornecedores_nome ON fornecedores (razao_social);

CREATE TABLE fornecedor_contas (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fornecedor_id   BIGINT      NOT NULL REFERENCES fornecedores(id),
    forma           forma_pagamento NOT NULL,
    pix_tipo        TEXT,
    pix_chave       TEXT,
    banco_codigo    TEXT,
    agencia         TEXT,
    conta           TEXT,
    conta_digito    TEXT,
    titular_nome    TEXT,
    titular_doc     TEXT,
    status          status_conta NOT NULL DEFAULT 'PENDENTE',
    homologada_por  BIGINT      REFERENCES usuarios(id),
    homologada_em   TIMESTAMPTZ,
    motivo_bloqueio TEXT,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_conta_dados CHECK (
        (forma = 'PIX' AND pix_tipo IS NOT NULL AND pix_chave IS NOT NULL)
        OR (forma = 'TED' AND banco_codigo IS NOT NULL AND agencia IS NOT NULL AND conta IS NOT NULL)
    )
);
CREATE INDEX idx_fornecedor_contas_forn ON fornecedor_contas (fornecedor_id, status);

CREATE TABLE obras (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    codigo              TEXT        NOT NULL UNIQUE,
    nome                TEXT        NOT NULL,
    cno                 TEXT,
    municipio           TEXT,
    uf                  CHAR(2),
    endereco            TEXT,
    codigo_omie_depto   TEXT        UNIQUE,
    ref_sheets          TEXT,
    status              TEXT        NOT NULL DEFAULT 'ATIVA',
    criado_em           TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE categorias (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    codigo              TEXT        NOT NULL UNIQUE,
    descricao           TEXT        NOT NULL,
    categoria_pai_id    BIGINT      REFERENCES categorias(id),
    codigo_omie         TEXT        UNIQUE,
    tipos_permitidos    tipo_titulo[] NOT NULL DEFAULT '{}',
    dedutivel_padrao    BOOLEAN     NOT NULL DEFAULT TRUE,
    credito_pis_cofins  BOOLEAN     NOT NULL DEFAULT FALSE,
    conta_contabil      TEXT,
    ativo               BOOLEAN     NOT NULL DEFAULT TRUE,
    criado_em           TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE contratos (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fornecedor_id       BIGINT      NOT NULL REFERENCES fornecedores(id),
    obra_id             BIGINT      REFERENCES obras(id),
    tipo                TEXT        NOT NULL,
    objeto              TEXT        NOT NULL,
    valor_total         NUMERIC(14,2),
    valor_parcela       NUMERIC(14,2),
    indice_reajuste     TEXT,
    dia_vencimento      SMALLINT,
    vigencia_inicio     DATE        NOT NULL,
    vigencia_fim        DATE,
    retencao_contratual_pct NUMERIC(5,2) DEFAULT 0,
    arquivo_anexo_id    BIGINT,
    status              TEXT        NOT NULL DEFAULT 'VIGENTE',
    criado_em           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE contas_bancarias (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    descricao       TEXT        NOT NULL,
    banco_codigo    TEXT        NOT NULL,
    agencia         TEXT        NOT NULL,
    conta           TEXT        NOT NULL,
    codigo_omie     BIGINT      UNIQUE,
    ativo           BOOLEAN     NOT NULL DEFAULT TRUE,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE documentos_fiscais (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tipo                tipo_doc_fiscal NOT NULL,
    chave_acesso        TEXT        UNIQUE,
    numero              TEXT,
    serie               TEXT,
    codigo_verificacao  TEXT,
    municipio_emissao   TEXT,
    emitente_doc        TEXT        NOT NULL,
    emitente_nome       TEXT,
    destinatario_doc    TEXT,
    valor_total         NUMERIC(14,2),
    data_emissao        DATE,
    situacao            situacao_nota NOT NULL DEFAULT 'DESCONHECIDA',
    situacao_em         TIMESTAMPTZ,
    manifestacao        TEXT,
    xml_path            TEXT,
    pdf_path            TEXT,
    dados               JSONB,
    origem              TEXT        NOT NULL DEFAULT 'UPLOAD',
    capturado_em        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_nfse UNIQUE (tipo, municipio_emissao, emitente_doc, numero)
);
CREATE INDEX idx_docfiscais_emitente ON documentos_fiscais (emitente_doc, data_emissao);
CREATE INDEX idx_docfiscais_situacao ON documentos_fiscais (situacao) WHERE situacao <> 'AUTORIZADA';

CREATE TABLE pedidos (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    numero          TEXT        NOT NULL UNIQUE,
    fornecedor_id   BIGINT      REFERENCES fornecedores(id),
    obra_id         BIGINT      REFERENCES obras(id),
    valor_total     NUMERIC(14,2),
    status          TEXT        NOT NULL DEFAULT 'ABERTO',
    dados           JSONB,
    ref_origem      TEXT,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE anexos (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entidade_tipo   TEXT        NOT NULL,
    entidade_id     BIGINT      NOT NULL,
    nome_arquivo    TEXT        NOT NULL,
    dropbox_path    TEXT        NOT NULL,
    hash_sha256     TEXT        NOT NULL,
    tamanho_bytes   BIGINT,
    enviado_por     BIGINT      REFERENCES usuarios(id),
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_anexos_entidade ON anexos (entidade_tipo, entidade_id);
CREATE INDEX idx_anexos_hash ON anexos (hash_sha256);

ALTER TABLE contratos
    ADD CONSTRAINT fk_contratos_anexo FOREIGN KEY (arquivo_anexo_id) REFERENCES anexos(id);

CREATE TABLE titulos (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    numero_sp           TEXT        NOT NULL UNIQUE,
    tipo                tipo_titulo NOT NULL,
    fornecedor_id       BIGINT      NOT NULL REFERENCES fornecedores(id),
    descricao           TEXT        NOT NULL,
    valor_bruto         NUMERIC(14,2) NOT NULL CHECK (valor_bruto > 0),
    valor_retencoes     NUMERIC(14,2) NOT NULL DEFAULT 0,
    valor_liquido       NUMERIC(14,2) NOT NULL,
    competencia         DATE        NOT NULL,
    data_emissao_doc    DATE,
    categoria_id        BIGINT      NOT NULL REFERENCES categorias(id),
    pedido_id           BIGINT      REFERENCES pedidos(id),
    contrato_id         BIGINT      REFERENCES contratos(id),
    documento_fiscal_id BIGINT      REFERENCES documentos_fiscais(id),
    forma_pagamento     forma_pagamento NOT NULL,
    fornecedor_conta_id BIGINT      REFERENCES fornecedor_contas(id),
    dedutivel           BOOLEAN     NOT NULL DEFAULT TRUE,
    justificativa_excecao TEXT,
    status              status_titulo NOT NULL DEFAULT 'RASCUNHO',
    score_risco         SMALLINT,
    solicitante_id      BIGINT      NOT NULL REFERENCES usuarios(id),
    aprovador_id        BIGINT      REFERENCES usuarios(id),
    aprovado_em         TIMESTAMPTZ,
    estorna_titulo_id   BIGINT      REFERENCES titulos(id),
    codigo_omie         BIGINT      UNIQUE,
    ref_pipefy          TEXT,
    origem              TEXT        NOT NULL DEFAULT 'SISTEMA',
    criado_em           TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_liquido CHECK (valor_liquido = valor_bruto - valor_retencoes),
    CONSTRAINT uq_titulo_docfiscal UNIQUE (documento_fiscal_id)
);
CREATE INDEX idx_titulos_status ON titulos (status);
CREATE INDEX idx_titulos_fornecedor ON titulos (fornecedor_id, competencia);
CREATE INDEX idx_titulos_competencia ON titulos (competencia);

CREATE TABLE parcelas (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    titulo_id       BIGINT      NOT NULL REFERENCES titulos(id),
    numero          SMALLINT    NOT NULL,
    vencimento      DATE        NOT NULL,
    valor           NUMERIC(14,2) NOT NULL CHECK (valor > 0),
    status          status_parcela NOT NULL DEFAULT 'ABERTA',
    linha_digitavel TEXT,
    codigo_barras   TEXT,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_parcela UNIQUE (titulo_id, numero)
);
CREATE INDEX idx_parcelas_vencimento ON parcelas (vencimento, status);
CREATE UNIQUE INDEX uq_linha_digitavel ON parcelas (linha_digitavel)
    WHERE linha_digitavel IS NOT NULL AND status <> 'CANCELADA';

CREATE TABLE rateios (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    titulo_id       BIGINT      NOT NULL REFERENCES titulos(id),
    obra_id         BIGINT      NOT NULL REFERENCES obras(id),
    valor           NUMERIC(14,2) NOT NULL CHECK (valor > 0),
    percentual      NUMERIC(7,4),
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_rateios_titulo ON rateios (titulo_id);
CREATE INDEX idx_rateios_obra ON rateios (obra_id);

CREATE TABLE retencoes (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    titulo_id       BIGINT      NOT NULL REFERENCES titulos(id),
    tipo            tipo_retencao NOT NULL,
    base_calculo    NUMERIC(14,2) NOT NULL,
    aliquota        NUMERIC(7,4) NOT NULL,
    valor           NUMERIC(14,2) NOT NULL,
    cno_obra        TEXT,
    titulo_guia_id  BIGINT      REFERENCES titulos(id),
    memoria_calculo JSONB,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_retencoes_titulo ON retencoes (titulo_id);

CREATE TABLE analises (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    titulo_id       BIGINT      NOT NULL REFERENCES titulos(id),
    motor_versao    TEXT        NOT NULL,
    resultado       TEXT        NOT NULL,
    score           SMALLINT    NOT NULL DEFAULT 0,
    criticas        JSONB       NOT NULL DEFAULT '[]',
    executada_em    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_analises_titulo ON analises (titulo_id, executada_em DESC);

CREATE TABLE pagamentos (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    parcela_id          BIGINT      NOT NULL REFERENCES parcelas(id),
    conta_bancaria_id   BIGINT      NOT NULL REFERENCES contas_bancarias(id),
    data_pagamento      DATE        NOT NULL,
    valor_pago          NUMERIC(14,2) NOT NULL CHECK (valor_pago > 0),
    meio                forma_pagamento NOT NULL,
    comprovante_anexo_id BIGINT     REFERENCES anexos(id),
    executado_por       BIGINT      REFERENCES usuarios(id),
    executado_por_robo  BOOLEAN     NOT NULL DEFAULT FALSE,
    estorna_pagamento_id BIGINT     REFERENCES pagamentos(id),
    criado_em           TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_pagamentos_parcela ON pagamentos (parcela_id);
CREATE INDEX idx_pagamentos_data ON pagamentos (data_pagamento);

CREATE TABLE extratos (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    conta_bancaria_id   BIGINT      NOT NULL REFERENCES contas_bancarias(id),
    data_lancamento     DATE        NOT NULL,
    valor               NUMERIC(14,2) NOT NULL,
    historico           TEXT,
    documento           TEXT,
    nome_contraparte    TEXT,
    hash_linha          TEXT        NOT NULL UNIQUE,
    importado_em        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_extratos_conta_data ON extratos (conta_bancaria_id, data_lancamento);

CREATE TABLE conciliacoes (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pagamento_id    BIGINT      NOT NULL REFERENCES pagamentos(id),
    extrato_id      BIGINT      NOT NULL REFERENCES extratos(id),
    metodo          TEXT        NOT NULL DEFAULT 'MANUAL',
    confianca       NUMERIC(4,3),
    conciliado_por  BIGINT      REFERENCES usuarios(id),
    conciliado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    desfeita_em     TIMESTAMPTZ,
    CONSTRAINT uq_conc_pagamento UNIQUE (pagamento_id),
    CONSTRAINT uq_conc_extrato UNIQUE (extrato_id)
);

CREATE TABLE sync_queue (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entidade_tipo   TEXT        NOT NULL,
    entidade_id     BIGINT      NOT NULL,
    destino         destino_sync NOT NULL,
    operacao        TEXT        NOT NULL,
    payload         JSONB,
    status          status_sync NOT NULL DEFAULT 'PENDENTE',
    tentativas      SMALLINT    NOT NULL DEFAULT 0,
    ultimo_erro     TEXT,
    proximo_retry   TIMESTAMPTZ,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    processado_em   TIMESTAMPTZ
);
CREATE INDEX idx_sync_pendentes ON sync_queue (destino, status, proximo_retry)
    WHERE status IN ('PENDENTE', 'ERRO');

CREATE TABLE eventos (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entidade_tipo   TEXT        NOT NULL,
    entidade_id     BIGINT      NOT NULL,
    usuario_id      BIGINT      REFERENCES usuarios(id),
    acao            TEXT        NOT NULL,
    detalhe         JSONB,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_eventos_entidade ON eventos (entidade_tipo, entidade_id, criado_em);

CREATE OR REPLACE FUNCTION fn_bloquear_mutacao() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'Tabela eventos é append-only: % não permitido', TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_eventos_imutavel
    BEFORE UPDATE OR DELETE ON eventos
    FOR EACH ROW EXECUTE FUNCTION fn_bloquear_mutacao();

CREATE OR REPLACE FUNCTION fn_touch() RETURNS trigger AS $$
BEGIN
    NEW.atualizado_em = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_touch_usuarios      BEFORE UPDATE ON usuarios      FOR EACH ROW EXECUTE FUNCTION fn_touch();
CREATE TRIGGER trg_touch_fornecedores  BEFORE UPDATE ON fornecedores  FOR EACH ROW EXECUTE FUNCTION fn_touch();
CREATE TRIGGER trg_touch_obras         BEFORE UPDATE ON obras         FOR EACH ROW EXECUTE FUNCTION fn_touch();
CREATE TRIGGER trg_touch_categorias    BEFORE UPDATE ON categorias    FOR EACH ROW EXECUTE FUNCTION fn_touch();
CREATE TRIGGER trg_touch_titulos       BEFORE UPDATE ON titulos       FOR EACH ROW EXECUTE FUNCTION fn_touch();

COMMIT;
