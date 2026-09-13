-- ===========================================================================
-- Migração 045 — a gestão de documentos da empresa.
--
-- POR QUE EXISTE
--
-- Pedido do dono em 09/09/2026, especificado inteiro em `GESTAO_DOCUMENTOS.md`.
-- Nas palavras dele: *"eu queria um ambiente onde eu pudesse simplesmente jogar
-- esse documento, ele fosse interpretado, lido, e a partir dali categorizado,
-- renomeado e salvo."*
--
-- O QUE JÁ EXISTIA, E O QUE FALTAVA
--
-- Os BYTES já têm casa: a tabela `anexos`, que desde a migração 043 sabe
-- guardar no banco ou no Google Drive. Nada disso é refeito aqui.
--
-- O que faltava é o que transforma "arquivo guardado" em "documento
-- encontrável": o TIPO dele no catálogo da empresa, a QUEM ele pertence, ATÉ
-- QUANDO vale, de QUE MÊS é, e o TEXTO dentro dele.
--
-- DUAS TABELAS
--
--   documento_tipos   o catálogo. É editável na tela porque quem sabe quais
--                     documentos a BWS usa é a BWS, não quem programa. Nasce
--                     com os tipos do §4 da especificação.
--   documentos        o documento em si, apontando para o anexo (os bytes) e
--                     para o dono.
--
-- O DONO É UM SÓ, e é o que evita a bagunça: empresa, obra, pessoa, parceiro
-- ou lançamento. A restrição abaixo garante que seja exatamente um — documento
-- pendurado em dois donos não é achado por nenhum dos dois.
--
-- A COMPETÊNCIA NÃO É DONO, É RECORTE. Folha de agosto pertence a uma obra E
-- ao mês de agosto. É esse recorte que faz o compilado fiscal funcionar.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS documento_tipos (
    codigo        TEXT PRIMARY KEY,          -- vira o nome do arquivo: CND-FEDERAL
    nome          TEXT NOT NULL,             -- o que a pessoa lê: "Certidão negativa federal"
    grupo         TEXT NOT NULL,             -- CADASTRAL, CERTIDAO, LICITACAO, OBRA, FISCAL, PESSOA, FINANCEIRO
    dono          TEXT NOT NULL,             -- EMPRESA, OBRA, PESSOA, PARCEIRO, LANCAMENTO
    vence         BOOLEAN NOT NULL DEFAULT FALSE,
    -- Quantos dias antes de vencer o sistema começa a avisar. Certidão de 30
    -- dias não pode avisar com 30 de antecedência — avisaria no dia da emissão.
    avisar_dias   INT,
    por_competencia BOOLEAN NOT NULL DEFAULT FALSE,
    -- ABERTO, RESTRITO ou PESSOAL. Folha de pagamento e documento de sócio não
    -- são para todo mundo que entra no ERP.
    sigilo        TEXT NOT NULL DEFAULT 'ABERTO',
    ativo         BOOLEAN NOT NULL DEFAULT TRUE,
    ordem         INT NOT NULL DEFAULT 100,
    criado_em     TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE documento_tipos DROP CONSTRAINT IF EXISTS ck_tipo_dono;
ALTER TABLE documento_tipos ADD CONSTRAINT ck_tipo_dono
    CHECK (dono IN ('EMPRESA', 'OBRA', 'PESSOA', 'PARCEIRO', 'LANCAMENTO'));
ALTER TABLE documento_tipos DROP CONSTRAINT IF EXISTS ck_tipo_sigilo;
ALTER TABLE documento_tipos ADD CONSTRAINT ck_tipo_sigilo
    CHECK (sigilo IN ('ABERTO', 'RESTRITO', 'PESSOAL'));
-- Só o código: A-Z, 0-9 e hífen. Ele VAI para o nome do arquivo, e portal de
-- licitação ainda engasga com acento e espaço.
ALTER TABLE documento_tipos DROP CONSTRAINT IF EXISTS ck_tipo_codigo;
ALTER TABLE documento_tipos ADD CONSTRAINT ck_tipo_codigo
    CHECK (codigo ~ '^[A-Z0-9]+(-[A-Z0-9]+)*$');


CREATE TABLE IF NOT EXISTS documentos (
    id            BIGSERIAL PRIMARY KEY,
    tipo_codigo   TEXT NOT NULL REFERENCES documento_tipos(codigo),
    anexo_id      BIGINT NOT NULL REFERENCES anexos(id) ON DELETE CASCADE,

    -- O nome padronizado (TIPO_DONO[_REFERENCIA]_DATA) e o que veio do
    -- fornecedor. Os DOIS: renomear é conveniência, não amnésia — quando
    -- alguém pergunta "recebeu o arquivo tal?", tem de dar para responder.
    nome_padronizado TEXT NOT NULL,
    nome_original    TEXT,

    -- O DONO. Exatamente um, garantido pela restrição abaixo.
    empresa_id    BIGINT REFERENCES empresas(id),
    obra_id       BIGINT REFERENCES obras(id),
    colaborador_id BIGINT REFERENCES colaboradores(id),
    fornecedor_id BIGINT REFERENCES fornecedores(id),
    lancamento_tipo TEXT,
    lancamento_id BIGINT,

    competencia   DATE,                      -- sempre dia 1; é recorte, não dono
    referencia    TEXT,                      -- "02" do aditivo, "ART-1234567"
    emissao       DATE,
    validade      DATE,

    -- O texto lido do documento, para a busca. Extraído NA ENTRADA porque a
    -- leitura já acontece de qualquer jeito; reprocessar dez mil arquivos
    -- depois é que sairia caro.
    texto         TEXT,
    resumo        TEXT,

    origem        TEXT NOT NULL DEFAULT 'TELA',
    confirmado_por BIGINT REFERENCES usuarios(id),
    confirmado_em TIMESTAMPTZ,
    observacao    TEXT,
    criado_por    BIGINT REFERENCES usuarios(id),
    criado_em     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- UM DONO, NEM ZERO NEM DOIS. Documento pendurado em dois donos não é achado
-- por nenhum dos dois; documento sem dono nenhum é o arquivo perdido de novo.
ALTER TABLE documentos DROP CONSTRAINT IF EXISTS ck_documento_um_dono;
ALTER TABLE documentos ADD CONSTRAINT ck_documento_um_dono CHECK (
    (CASE WHEN empresa_id      IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN obra_id         IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN colaborador_id  IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN fornecedor_id   IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN lancamento_id   IS NOT NULL THEN 1 ELSE 0 END) = 1
);

-- Competência é sempre o primeiro dia do mês. Sem isso, "agosto" viraria trinta
-- e um valores diferentes e o compilado fiscal não fecharia nunca.
ALTER TABLE documentos DROP CONSTRAINT IF EXISTS ck_documento_competencia;
ALTER TABLE documentos ADD CONSTRAINT ck_documento_competencia
    CHECK (competencia IS NULL OR EXTRACT(DAY FROM competencia) = 1);

-- O mesmo arquivo (mesmo anexo) é UM documento. Anexar duas vezes o mesmo PDF
-- e catalogá-lo duas vezes com tipos diferentes é como o arquivo se perde.
CREATE UNIQUE INDEX IF NOT EXISTS idx_documento_anexo ON documentos (anexo_id);

CREATE INDEX IF NOT EXISTS idx_documento_tipo ON documentos (tipo_codigo);
CREATE INDEX IF NOT EXISTS idx_documento_empresa ON documentos (empresa_id);
CREATE INDEX IF NOT EXISTS idx_documento_obra ON documentos (obra_id);
CREATE INDEX IF NOT EXISTS idx_documento_colaborador ON documentos (colaborador_id);
CREATE INDEX IF NOT EXISTS idx_documento_fornecedor ON documentos (fornecedor_id);
CREATE INDEX IF NOT EXISTS idx_documento_competencia ON documentos (competencia);
-- A pergunta mais valiosa da tela é "o que está vencendo?" — ela merece índice.
CREATE INDEX IF NOT EXISTS idx_documento_validade
    ON documentos (validade) WHERE validade IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_documento_lancamento
    ON documentos (lancamento_tipo, lancamento_id) WHERE lancamento_id IS NOT NULL;

-- BUSCA DENTRO DO DOCUMENTO. O índice é sobre o texto já extraído; o custo é
-- de espaço, não de processamento por consulta.
CREATE INDEX IF NOT EXISTS idx_documento_texto
    ON documentos USING gin (to_tsvector('portuguese',
        coalesce(nome_padronizado, '') || ' ' || coalesce(resumo, '') || ' ' ||
        coalesce(texto, '')));
