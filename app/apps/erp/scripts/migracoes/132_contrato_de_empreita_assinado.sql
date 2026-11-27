-- 132 — O CONTRATO DE EMPREITA GERADO PELO SISTEMA E ASSINADO À DISTÂNCIA (27/11/2026).
--
-- O dono: "a gente precisaria coletar uma assinatura (…) do empreiteiro (…)
-- duas pessoas assinariam. Seria o de fora e um de dentro (…) quem assina é
-- uma pessoa do cadastro de colaboradores (…) vai ser o WhatsApp mesmo.
-- WhatsApp e e-mail (…) que tenha valor jurídico."
--
-- Duas tabelas novas e NENHUMA coluna em tabela existente: o modelo de
-- `contratos_servico` não muda, então o código no ar não depende desta
-- migração para listar, medir ou aprovar contrato. Sem ela, só a assinatura
-- fica indisponível (e a tela diz isso).

-- O documento: o texto do contrato como foi montado, com o lacre (SHA-256)
-- que prova que ninguém mudou uma vírgula entre a montagem e as assinaturas.
CREATE TABLE IF NOT EXISTS contrato_documentos (
    id                  BIGSERIAL PRIMARY KEY,
    contrato_id         BIGINT NOT NULL REFERENCES contratos_servico(id),
    titulo              TEXT NOT NULL,
    texto               TEXT NOT NULL,
    lacre               TEXT NOT NULL,
    escolhas            JSONB NOT NULL DEFAULT '{}'::jsonb,
    situacao            TEXT NOT NULL DEFAULT 'RASCUNHO'
                        CHECK (situacao IN ('RASCUNHO', 'EM_ASSINATURA', 'ASSINADO', 'CANCELADO')),
    assinado_anexo_id   BIGINT,
    criado_por          BIGINT REFERENCES usuarios(id),
    criado_em           TIMESTAMPTZ NOT NULL DEFAULT now(),
    concluido_em        TIMESTAMPTZ
);
-- Um documento vivo por contrato (o cancelado não conta).
CREATE UNIQUE INDEX IF NOT EXISTS ux_contrato_documento_vivo
    ON contrato_documentos (contrato_id) WHERE situacao <> 'CANCELADO';

-- Quem assina: o empreiteiro (CONTRATADO) e quem responde pela empresa
-- (CONTRATANTE, um colaborador do cadastro). O link leva um TOKEN; o código
-- de 6 dígitos vai ao telefone/e-mail. Guardamos só o RESUMO de cada um.
CREATE TABLE IF NOT EXISTS contrato_assinaturas (
    id                  BIGSERIAL PRIMARY KEY,
    documento_id        BIGINT NOT NULL REFERENCES contrato_documentos(id),
    papel               TEXT NOT NULL CHECK (papel IN ('CONTRATADO', 'CONTRATANTE')),
    ordem               SMALLINT NOT NULL DEFAULT 1,
    nome                TEXT NOT NULL,
    documento           TEXT NOT NULL,
    telefone            TEXT,
    email               TEXT,
    colaborador_id      BIGINT REFERENCES colaboradores(id),
    fornecedor_id       BIGINT REFERENCES fornecedores(id),
    token_resumo        TEXT NOT NULL UNIQUE,
    codigo_resumo       TEXT,
    codigo_expira_em    TIMESTAMPTZ,
    codigo_canal        TEXT,
    tentativas          SMALLINT NOT NULL DEFAULT 0,
    enviado_em          TIMESTAMPTZ,
    envios              JSONB NOT NULL DEFAULT '[]'::jsonb,
    aberto_em           TIMESTAMPTZ,
    assinado_em         TIMESTAMPTZ,
    ip                  TEXT,
    navegador           TEXT,
    assinatura_png      BYTEA,
    lacre_conferido     TEXT,
    situacao            TEXT NOT NULL DEFAULT 'AGUARDANDO'
                        CHECK (situacao IN ('AGUARDANDO', 'ASSINADO', 'CANCELADO'))
);
CREATE INDEX IF NOT EXISTS idx_contrato_assinaturas_doc ON contrato_assinaturas (documento_id);
