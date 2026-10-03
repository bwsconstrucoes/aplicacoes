-- ===========================================================================
-- 002 — A GESTÃO DO PONTO: escalas, feriados, ocorrências, banco de horas,
-- fechamento do mês, alertas e o acesso do colaborador pelo celular.
--
-- Pedido do dono em 03/10/2026 (ver PLANO_FASE2.md): um ambiente completo de
-- gestão dentro do ERP, um "meu ponto" simples no celular, atestado e licença
-- com documento e aprovação, compensação, banco de horas só para quem pode e
-- alertas. Decisões dele no mesmo dia: horários CONFIGURÁVEIS; gestão no ERP;
-- quem aprova o quê é configurável (as seções "Ponto" do cadastro de perfis);
-- só o DP vê o atestado.
--
-- Regras que a estrutura garante, e não a tela:
--   · batida NUNCA se apaga nem muda de hora. A decisão sobre uma batida em
--     análise (validar ou rejeitar) fica em `marcacao_decisoes`, com quem e por
--     quê; o ajuste aprovado gera uma batida NOVA, AJUSTADA, que aponta para o
--     pedido. É o que a Portaria 671 chama de tratamento do registro.
--   · o mês FECHADO (`competencias`) não aceita ocorrência nem decisão nova —
--     quem reabre deixa nome, data e motivo.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- Documentos (atestado, acordo de banco de horas…). Mesmo caminho das fotos:
-- o arquivo vai para o Google Drive; aqui fica a ficha. `conteudo` é a sala de
-- espera enquanto o Drive não aceitou. `sigiloso` marca documento de saúde —
-- só quem tem "aprovar afastamento" (o DP) abre.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ponto.documentos (
    id             BIGSERIAL PRIMARY KEY,
    sha256         TEXT        NOT NULL,
    nome_original  TEXT        NOT NULL DEFAULT '',
    nome_arquivo   TEXT        NOT NULL,
    mime           TEXT        NOT NULL,
    tamanho        INTEGER     NOT NULL,
    sigiloso       BOOLEAN     NOT NULL DEFAULT FALSE,
    drive_file_id  TEXT,
    enviada_em     TIMESTAMPTZ,
    conteudo       BYTEA,
    tentativas     INTEGER     NOT NULL DEFAULT 0,
    ultimo_erro    TEXT,
    criado_em      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_ponto_documentos_fila
    ON ponto.documentos (id) WHERE drive_file_id IS NULL AND conteudo IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Escalas. SEMANAL: `semana` = {"0": [["07:00","11:00"],["12:00","17:00"]], …}
-- (0 = segunda … 6 = domingo; dia ausente = descanso; saída menor que a
-- entrada atravessa a meia-noite). CICLO_12X36: um turno, dia sim, dia não,
-- contado da data-base de cada pessoa.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ponto.escalas (
    id             BIGSERIAL PRIMARY KEY,
    nome           TEXT        NOT NULL UNIQUE,
    tipo           TEXT        NOT NULL DEFAULT 'SEMANAL'
                               CHECK (tipo IN ('SEMANAL', 'CICLO_12X36')),
    semana         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    ciclo_entrada  TEXT,
    ciclo_saida    TEXT,
    descricao      TEXT        NOT NULL DEFAULT '',
    ativo          BOOLEAN     NOT NULL DEFAULT TRUE,
    criado_em      TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A escala de cada pessoa, com vigência: trocar de escala não reescreve o
-- passado, porque o espelho de agosto tem de continuar dizendo o que dizia.
CREATE TABLE IF NOT EXISTS ponto.colaborador_escalas (
    id               BIGSERIAL PRIMARY KEY,
    colaborador_id   BIGINT      NOT NULL REFERENCES public.colaboradores (id) ON DELETE CASCADE,
    escala_id        BIGINT      NOT NULL REFERENCES ponto.escalas (id),
    vigencia_inicio  DATE        NOT NULL,
    ciclo_data_base  DATE,
    definido_por     TEXT        NOT NULL DEFAULT '',
    criado_em        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (colaborador_id, vigencia_inicio)
);

-- ---------------------------------------------------------------------------
-- Feriados. NACIONAL vale para todos; ESTADUAL pela UF da obra; MUNICIPAL pelo
-- município da obra; OBRA para uma obra só (ponto facultativo combinado, por
-- exemplo). A obra que vale para a batida é a do dia.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ponto.feriados (
    id           BIGSERIAL PRIMARY KEY,
    data         DATE        NOT NULL,
    nome         TEXT        NOT NULL,
    abrangencia  TEXT        NOT NULL DEFAULT 'NACIONAL'
                             CHECK (abrangencia IN ('NACIONAL', 'ESTADUAL', 'MUNICIPAL', 'OBRA')),
    uf           TEXT,
    municipio    TEXT,
    obra_id      BIGINT      REFERENCES public.obras (id) ON DELETE CASCADE,
    criado_em    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_ponto_feriados
    ON ponto.feriados (data, abrangencia, COALESCE(upper(uf), ''),
                       COALESCE(upper(municipio), ''), COALESCE(obra_id, 0));

-- ---------------------------------------------------------------------------
-- O que muda na pessoa: banco de horas e o PIN do celular.
--   SEM_BANCO           extra é paga no mês, débito é descontado
--   COMPENSACAO_MENSAL  compensa dentro do mesmo mês (CLT art. 59, §6º)
--   BANCO_6_MESES       acordo individual escrito (art. 59, §5º)
--   BANCO_12_MESES      acordo/convenção coletiva (art. 59, §2º)
-- O PIN fica só em hash; errar 5 vezes bloqueia por 15 minutos.
-- ---------------------------------------------------------------------------
ALTER TABLE ponto.colaborador_config
    ADD COLUMN IF NOT EXISTS regime_banco TEXT NOT NULL DEFAULT 'SEM_BANCO',
    ADD COLUMN IF NOT EXISTS banco_inicio DATE,
    ADD COLUMN IF NOT EXISTS acordo_documento_id BIGINT REFERENCES ponto.documentos (id),
    ADD COLUMN IF NOT EXISTS pin_hash TEXT,
    ADD COLUMN IF NOT EXISTS pin_criado_em TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pin_falhas INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS pin_bloqueado_ate TIMESTAMPTZ;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_ponto_regime_banco') THEN
        ALTER TABLE ponto.colaborador_config ADD CONSTRAINT ck_ponto_regime_banco
            CHECK (regime_banco IN ('SEM_BANCO', 'COMPENSACAO_MENSAL', 'BANCO_6_MESES', 'BANCO_12_MESES'));
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Ocorrências: tudo o que justifica ou muda um dia.
--
-- Fluxo (as etapas por tipo moram em `core/ocorrencias.py`):
--   AGUARDANDO_SUPERVISOR → AGUARDANDO_DP → APROVADA   (ou NEGADA / CANCELADA)
-- `cid`, `medico` e `crm` são dado de SAÚDE (LGPD, art. 11): a gestão só os
-- mostra a quem aprova afastamento.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ponto.ocorrencias (
    id                     BIGSERIAL PRIMARY KEY,
    colaborador_id         BIGINT      NOT NULL REFERENCES public.colaboradores (id),
    tipo                   TEXT        NOT NULL
                           CHECK (tipo IN ('ATESTADO', 'LICENCA', 'FERIAS', 'AFASTAMENTO', 'ABONO',
                                           'AJUSTE_BATIDA', 'COMPENSACAO', 'FOLGA_BANCO')),
    data_inicio            DATE        NOT NULL,
    data_fim               DATE        NOT NULL,
    horario                TIMESTAMPTZ,
    obra_id                BIGINT      REFERENCES public.obras (id),
    dia_trabalhado         DATE,
    minutos                INTEGER,
    descricao              TEXT        NOT NULL DEFAULT '',
    cid                    TEXT,
    medico                 TEXT,
    crm                    TEXT,
    documento_id           BIGINT      REFERENCES ponto.documentos (id),
    leitura_ia             JSONB,
    status                 TEXT        NOT NULL
                           CHECK (status IN ('AGUARDANDO_SUPERVISOR', 'AGUARDANDO_DP',
                                             'APROVADA', 'NEGADA', 'CANCELADA')),
    origem                 TEXT        NOT NULL DEFAULT 'GESTAO'
                           CHECK (origem IN ('APP', 'GESTAO', 'API')),
    solicitado_por         TEXT        NOT NULL,
    solicitado_usuario_id  BIGINT,
    supervisor_usuario_id  BIGINT,
    supervisor_nome        TEXT,
    supervisor_em          TIMESTAMPTZ,
    dp_usuario_id          BIGINT,
    dp_nome                TEXT,
    dp_em                  TIMESTAMPTZ,
    motivo_negativa        TEXT,
    marcacao_gerada_id     BIGINT      REFERENCES ponto.marcacoes (id),
    criado_em              TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (data_fim >= data_inicio)
);
CREATE INDEX IF NOT EXISTS ix_ponto_ocorrencias_pessoa
    ON ponto.ocorrencias (colaborador_id, data_inicio, data_fim);
CREATE INDEX IF NOT EXISTS ix_ponto_ocorrencias_fila
    ON ponto.ocorrencias (status, id) WHERE status IN ('AGUARDANDO_SUPERVISOR', 'AGUARDANDO_DP');

-- A decisão sobre uma batida EM_ANALISE (fora da cerca, relógio…). A batida
-- muda de status; a hora, o lugar e a corrente de hash não mudam nunca.
CREATE TABLE IF NOT EXISTS ponto.marcacao_decisoes (
    id           BIGSERIAL PRIMARY KEY,
    marcacao_id  BIGINT      NOT NULL REFERENCES ponto.marcacoes (id),
    de_status    TEXT        NOT NULL,
    para_status  TEXT        NOT NULL,
    motivo       TEXT        NOT NULL DEFAULT '',
    usuario_id   BIGINT,
    usuario_nome TEXT        NOT NULL,
    criado_em    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_ponto_marcacao_decisoes ON ponto.marcacao_decisoes (marcacao_id);

-- ---------------------------------------------------------------------------
-- Banco de horas: o saldo é a soma do que a apuração dá (extra − débito) nos
-- dias desde o início do banco, MAIS estes lançamentos à mão (saldo inicial,
-- pagamento de horas, desconto, folga aprovada). Lançamento não se apaga: o
-- engano se corrige com outro lançamento.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ponto.banco_lancamentos (
    id              BIGSERIAL PRIMARY KEY,
    colaborador_id  BIGINT      NOT NULL REFERENCES public.colaboradores (id),
    data            DATE        NOT NULL,
    minutos         INTEGER     NOT NULL CHECK (minutos <> 0),
    tipo            TEXT        NOT NULL
                    CHECK (tipo IN ('SALDO_INICIAL', 'PAGAMENTO', 'DESCONTO', 'FOLGA', 'CORRECAO')),
    descricao       TEXT        NOT NULL DEFAULT '',
    ocorrencia_id   BIGINT      REFERENCES ponto.ocorrencias (id),
    usuario_id      BIGINT,
    usuario_nome    TEXT        NOT NULL,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_ponto_banco_pessoa ON ponto.banco_lancamentos (colaborador_id, data);

-- ---------------------------------------------------------------------------
-- Competência (o mês da folha). FECHADA trava ocorrência e decisão naquele mês.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ponto.competencias (
    competencia   DATE        PRIMARY KEY CHECK (EXTRACT(DAY FROM competencia) = 1),
    situacao      TEXT        NOT NULL DEFAULT 'ABERTA' CHECK (situacao IN ('ABERTA', 'FECHADA')),
    fechada_por   TEXT,
    fechada_em    TIMESTAMPTZ,
    reaberta_por  TEXT,
    reaberta_em   TIMESTAMPTZ,
    motivo        TEXT        NOT NULL DEFAULT ''
);

-- ---------------------------------------------------------------------------
-- Alertas. `chave` impede o mesmo alerta duas vezes (ex.: FALTA:123:2026-10-02).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ponto.alertas (
    id               BIGSERIAL PRIMARY KEY,
    chave            TEXT        NOT NULL UNIQUE,
    codigo           TEXT        NOT NULL,
    gravidade        TEXT        NOT NULL DEFAULT 'ATENCAO'
                     CHECK (gravidade IN ('INFO', 'ATENCAO', 'URGENTE')),
    colaborador_id   BIGINT      REFERENCES public.colaboradores (id),
    obra_id          BIGINT      REFERENCES public.obras (id),
    data_referencia  DATE,
    mensagem         TEXT        NOT NULL,
    situacao         TEXT        NOT NULL DEFAULT 'ABERTO'
                     CHECK (situacao IN ('ABERTO', 'RESOLVIDO', 'DISPENSADO')),
    tratado_por      TEXT,
    tratado_em       TIMESTAMPTZ,
    nota             TEXT,
    criado_em        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_ponto_alertas_abertos
    ON ponto.alertas (situacao, gravidade, data_referencia) WHERE situacao = 'ABERTO';

-- O resumo diário que vai por WhatsApp: um por destinatário por dia.
CREATE TABLE IF NOT EXISTS ponto.resumos_enviados (
    id           BIGSERIAL PRIMARY KEY,
    referencia   TEXT        NOT NULL UNIQUE,
    destino      TEXT        NOT NULL,
    texto        TEXT        NOT NULL,
    resultado    TEXT        NOT NULL DEFAULT '',
    criado_em    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Código de acesso do celular, mandado por WhatsApp para criar ou trocar o PIN.
-- Só o hash; vale 15 minutos; 5 erros e ele morre.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ponto.codigos_acesso (
    id              BIGSERIAL PRIMARY KEY,
    colaborador_id  BIGINT      NOT NULL REFERENCES public.colaboradores (id) ON DELETE CASCADE,
    codigo_hash     TEXT        NOT NULL,
    expira_em       TIMESTAMPTZ NOT NULL,
    usado_em        TIMESTAMPTZ,
    tentativas      INTEGER     NOT NULL DEFAULT 0,
    ip              TEXT,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_ponto_codigos_pessoa ON ponto.codigos_acesso (colaborador_id, criado_em);

-- ---------------------------------------------------------------------------
-- Os feriados NACIONAIS fixos de 2026 e 2027 (leis 662/1949, 6.802/1980 e
-- 14.759/2023). Carnaval, Paixão de Cristo e Corpus Christi NÃO estão aqui: são
-- ponto facultativo ou feriado municipal, e quem decide é o DP, por obra.
-- ---------------------------------------------------------------------------
INSERT INTO ponto.feriados (data, nome, abrangencia)
SELECT make_date(a.ano, f.mes, f.dia), f.nome, 'NACIONAL'
  FROM (VALUES (2026), (2027)) AS a(ano)
 CROSS JOIN (VALUES
    (1, 1, 'Confraternização Universal'), (4, 21, 'Tiradentes'), (5, 1, 'Dia do Trabalho'),
    (9, 7, 'Independência do Brasil'), (10, 12, 'Nossa Senhora Aparecida'),
    (11, 2, 'Finados'), (11, 15, 'Proclamação da República'),
    (11, 20, 'Dia Nacional de Zumbi e da Consciência Negra'), (12, 25, 'Natal')
 ) AS f(mes, dia, nome)
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- Parâmetros do ponto que a gestão muda pela tela (telefones do resumo diário,
-- última vez que a rotina do dia rodou…). Texto simples, chave → valor.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ponto.parametros (
    chave          TEXT        PRIMARY KEY,
    valor          TEXT        NOT NULL DEFAULT '',
    atualizado_por TEXT        NOT NULL DEFAULT '',
    atualizado_em  TIMESTAMPTZ NOT NULL DEFAULT now()
);
