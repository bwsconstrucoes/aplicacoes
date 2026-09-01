-- ============================================================================
-- Migração 026 — Pessoal: colaboradores e despesas com colaboradores (DC)
--
-- O que a DC resolve: pagar 20 pessoas de uma obra não pode virar 20 títulos
-- lançados um a um, porque o pagamento não sai na conta de cada um — sai por
-- ARQUIVO (BeeVale ou SomaPay). Então a DC é um lote: várias pessoas, cada uma
-- com sua verba e valor, aprovado em cadeia e virando UM título financeiro
-- rateado por obra.
--
-- Cadastro do colaborador é propositalmente enxuto: nome, CPF, função, obra,
-- diária e os auxílios de referência. O cadastro completo (143 campos do pipe)
-- fica para quando o RH entrar no sistema.
-- ============================================================================

CREATE TABLE IF NOT EXISTS funcoes (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nome            TEXT NOT NULL UNIQUE,
    valor_diaria    NUMERIC(14,2),
    ativo           BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS colaboradores (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nome                TEXT        NOT NULL,
    cpf                 TEXT        NOT NULL UNIQUE,
    matricula           TEXT,
    funcao_id           BIGINT      REFERENCES funcoes(id),
    obra_id             BIGINT      REFERENCES obras(id),
    admissao            DATE,
    demissao            DATE,
    regime              TEXT        NOT NULL DEFAULT 'CLT',   -- CLT | DIARISTA | PJ
    valor_diaria        NUMERIC(14,2),
    aux_alimentacao     NUMERIC(14,2),
    aux_transporte      NUMERIC(14,2),
    pix_chave           TEXT,
    pix_tipo            TEXT,
    banco               TEXT,
    agencia             TEXT,
    conta               TEXT,
    telefone            TEXT,
    situacao            TEXT        NOT NULL DEFAULT 'ATIVO', -- ATIVO|AFASTADO|DESLIGADO
    ref_pipefy          TEXT,
    observacoes         TEXT,
    criado_em           TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_colaboradores_obra ON colaboradores (obra_id, situacao);

CREATE TABLE IF NOT EXISTS despesas_colaborador (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    numero              TEXT        NOT NULL UNIQUE,
    obra_id             BIGINT      NOT NULL REFERENCES obras(id),
    competencia         DATE        NOT NULL,
    data_prevista       DATE,
    descricao           TEXT,
    meio_pagamento      TEXT        NOT NULL DEFAULT 'BEEVALE',  -- BEEVALE | SOMAPAY
    status              TEXT        NOT NULL DEFAULT 'RASCUNHO',
      -- RASCUNHO|AGUARDANDO_SUPERVISOR|AGUARDANDO_DP|AGUARDANDO_DIRETOR|
      -- APROVADA|FATURADA|DEVOLVIDA|CANCELADA
    valor_total         NUMERIC(14,2) NOT NULL DEFAULT 0,
    titulo_id           BIGINT      REFERENCES titulos(id),
    criado_por          BIGINT      REFERENCES usuarios(id),
    aprovado_supervisor BIGINT      REFERENCES usuarios(id),
    aprovado_supervisor_em TIMESTAMPTZ,
    aprovado_dp         BIGINT      REFERENCES usuarios(id),
    aprovado_dp_em      TIMESTAMPTZ,
    aprovado_diretor    BIGINT      REFERENCES usuarios(id),
    aprovado_diretor_em TIMESTAMPTZ,
    motivo_devolucao    TEXT,
    criado_em           TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dc_status ON despesas_colaborador (status, obra_id);

CREATE TABLE IF NOT EXISTS despesa_colaborador_itens (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    despesa_id          BIGINT      NOT NULL REFERENCES despesas_colaborador(id) ON DELETE CASCADE,
    colaborador_id      BIGINT      NOT NULL REFERENCES colaboradores(id),
    verba               TEXT        NOT NULL,
      -- PRODUCAO|DIARIA|ALIMENTACAO|TRANSPORTE|PLR|FERIAS|RESCISAO|ADIANTAMENTO|OUTRA
    quantidade          NUMERIC(14,4),
    valor_unitario      NUMERIC(14,2),
    valor               NUMERIC(14,2) NOT NULL,
    obra_id             BIGINT      REFERENCES obras(id),
    observacao          TEXT,
    criticas            JSONB       NOT NULL DEFAULT '[]'::jsonb,
    conferido_por       BIGINT      REFERENCES usuarios(id),
    conferido_em        TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_dc_itens ON despesa_colaborador_itens (despesa_id);
CREATE INDEX IF NOT EXISTS idx_dc_itens_colab ON despesa_colaborador_itens (colaborador_id);

-- perfil novo: o departamento pessoal revisa a DC depois do supervisor
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'perfil_usuario') THEN
        ALTER TYPE perfil_usuario ADD VALUE IF NOT EXISTS 'DEPARTAMENTO_PESSOAL';
    END IF;
END $$;
