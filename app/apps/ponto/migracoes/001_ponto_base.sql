-- ===========================================================================
-- 001 — O PONTO ELETRÔNICO PRÓPRIO (REP-P), fase 1
--
-- Schema `ponto`, separado do ERP (`public`) e da Análise de SPs (`analisesps`)
-- pelo mesmo motivo que separou aquela: tabelas com nome parecido convivem sem
-- se encostar, e uma escrita daqui nunca alcança dado de outra área.
--
-- O QUE NÃO ESTÁ AQUI, DE PROPÓSITO: cadastro de obra e de pessoa. Os dois já
-- existem no ERP (`public.obras`, com latitude e longitude desde a migração 024;
-- `public.colaboradores`, CPF único, desde a 026). Criar `ponto_obras` e
-- `ponto_colaboradores` seria a segunda cópia de obras e a TERCEIRA de pessoas
-- (a Análise de SPs já espelha o Pipefy em `analisesps.colaborador`), e pessoa
-- que troca de obra no ERP não trocaria no ponto. Decisão do dono de 18/09/2026:
-- "100% da movimentação no ERP, nada de sistema paralelo". Aqui fica só o que o
-- ERP não tem: raio da cerca, tipo de jornada, obras adicionais, aparelhos,
-- marcações, ajustes e recusas.
--
-- PORTARIA 671/2021 DESDE O PRIMEIRO DIA: a marcação já nasce com NSR (número
-- sequencial de registro, sem furo) e hash encadeado com a marcação anterior.
-- Os arquivos fiscais (AFD/AEJ) e o comprovante do empregado vêm em fase
-- futura, mas o que eles precisam já é guardado — refazer tabela com 400
-- pessoas batendo ponto é o retrabalho que esta migração evita.
--
-- MARCAÇÃO NÃO SE ALTERA NEM SE APAGA. Ajuste aprovado gera marcação NOVA com
-- status AJUSTADA apontando para a original (`marcacao_origem_id`); a original
-- continua lá. É assim que a trilha fiscal se sustenta.
-- ===========================================================================

CREATE SCHEMA IF NOT EXISTS ponto;

-- ---------------------------------------------------------------------------
-- Fotos (da batida e cadastral): a FICHA fica aqui; o ARQUIVO fica no Google
-- Drive, pela mesma rotina dos anexos do ERP. Decisão do dono, 03/10/2026:
-- "no Drive o espaço é virtualmente infinito; na base de dados não".
--
-- `conteudo` é a SALA DE ESPERA, não armazenamento: só tem bytes enquanto a
-- subida ao Drive não aconteceu (rede, cota, pasta não configurada). Quando
-- sobe, vira NULL. `sha256` fica para sempre — é a prova de que a foto existiu
-- e não foi trocada. `expurgada_em` é para a rotina futura de apagar do Drive
-- pelo prazo de guarda; o hash continua.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ponto.fotos (
    id             BIGSERIAL PRIMARY KEY,
    sha256         TEXT        NOT NULL,
    tamanho        INTEGER     NOT NULL,
    mime           TEXT        NOT NULL DEFAULT 'image/jpeg',
    largura        INTEGER,
    altura         INTEGER,
    nome_arquivo   TEXT,
    drive_file_id  TEXT,
    enviada_em     TIMESTAMPTZ,
    conteudo       BYTEA,
    tentativas     INTEGER     NOT NULL DEFAULT 0,
    ultimo_erro    TEXT,
    criado_em      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expurgada_em   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_ponto_fotos_fila
    ON ponto.fotos (id) WHERE drive_file_id IS NULL AND conteudo IS NOT NULL;

-- As subpastas por mês (AAAA-MM) dentro da pasta do ponto no Drive, lembradas
-- para não perguntar ao Google a cada batida.
CREATE TABLE IF NOT EXISTS ponto.drive_pastas (
    nome       TEXT PRIMARY KEY,
    file_id    TEXT NOT NULL,
    criado_em  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- O que o ponto precisa saber de uma obra além do que o ERP já guarda.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ponto.obra_config (
    obra_id        BIGINT      PRIMARY KEY REFERENCES public.obras (id) ON DELETE CASCADE,
    raio_metros    INTEGER     NOT NULL DEFAULT 200
                               CHECK (raio_metros BETWEEN 20 AND 5000),
    centro_custo   TEXT,
    ativo          BOOLEAN     NOT NULL DEFAULT TRUE,
    criado_em      TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- O que o ponto precisa saber de uma pessoa além do que o ERP já guarda.
-- Pessoa SEM linha aqui bate ponto com os padrões (PADRAO_4, ativa): o cadastro
-- do ERP é a verdade sobre quem existe; esta tabela só afina a jornada.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ponto.colaborador_config (
    colaborador_id     BIGINT      PRIMARY KEY
                                   REFERENCES public.colaboradores (id) ON DELETE CASCADE,
    tipo_jornada       TEXT        NOT NULL DEFAULT 'PADRAO_4'
                                   CHECK (tipo_jornada IN
                                          ('PADRAO_4', 'VIGIA_DIURNO_2', 'VIGIA_NOTURNO_2')),
    centro_custo       TEXT,
    foto_cadastral_id  BIGINT      REFERENCES ponto.fotos (id),
    ativo              BOOLEAN     NOT NULL DEFAULT TRUE,
    criado_em          TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Obras ADICIONAIS onde a pessoa pode bater (a principal é `colaboradores.obra_id`).
CREATE TABLE IF NOT EXISTS ponto.colaborador_obras (
    colaborador_id  BIGINT NOT NULL REFERENCES public.colaboradores (id) ON DELETE CASCADE,
    obra_id         BIGINT NOT NULL REFERENCES public.obras (id) ON DELETE CASCADE,
    PRIMARY KEY (colaborador_id, obra_id)
);

-- ---------------------------------------------------------------------------
-- Aparelhos. O celular gera o `device_uuid` na primeira abertura e entra como
-- PENDENTE; alguém aprova, dizendo o perfil:
--   COMPARTILHADO  o tablet da obra: qualquer pessoa autorizada na obra bate nele
--   INDIVIDUAL     o celular de uma pessoa só (`colaborador_id` é o dono)
--   LISTA          um aparelho para um grupo (`dispositivo_autorizados`)
-- `token_hash` é o hash do token entregue UMA vez no registro: chave de API no
-- celular vazaria no primeiro aparelho inspecionado; token por aparelho morre
-- com o bloqueio do aparelho, e só dele.
-- `aprovado_por` é texto porque na fase 1 a aprovação vem pela API; quando a
-- tela do ERP chegar, passa a guardar também o usuário do ERP.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ponto.dispositivos (
    id               BIGSERIAL PRIMARY KEY,
    device_uuid      TEXT        NOT NULL UNIQUE,
    token_hash       TEXT        NOT NULL,
    descricao        TEXT        NOT NULL DEFAULT '',
    perfil           TEXT        NOT NULL DEFAULT 'COMPARTILHADO'
                                 CHECK (perfil IN ('COMPARTILHADO', 'INDIVIDUAL', 'LISTA')),
    colaborador_id   BIGINT      REFERENCES public.colaboradores (id),
    status           TEXT        NOT NULL DEFAULT 'PENDENTE'
                                 CHECK (status IN ('PENDENTE', 'APROVADO', 'BLOQUEADO')),
    aprovado_por     TEXT,
    aprovado_em      TIMESTAMPTZ,
    bloqueado_em     TIMESTAMPTZ,
    motivo_bloqueio  TEXT,
    user_agent       TEXT,
    ip_registro      TEXT,
    ultimo_uso_em    TIMESTAMPTZ,
    criado_em        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_ponto_dispositivos_status ON ponto.dispositivos (status, id);

CREATE TABLE IF NOT EXISTS ponto.dispositivo_autorizados (
    dispositivo_id  BIGINT NOT NULL REFERENCES ponto.dispositivos (id) ON DELETE CASCADE,
    colaborador_id  BIGINT NOT NULL REFERENCES public.colaboradores (id) ON DELETE CASCADE,
    PRIMARY KEY (dispositivo_id, colaborador_id)
);

-- Obras onde o aparelho vale. Vazio = todas.
CREATE TABLE IF NOT EXISTS ponto.dispositivo_obras (
    dispositivo_id  BIGINT NOT NULL REFERENCES ponto.dispositivos (id) ON DELETE CASCADE,
    obra_id         BIGINT NOT NULL REFERENCES public.obras (id) ON DELETE CASCADE,
    PRIMARY KEY (dispositivo_id, obra_id)
);

-- ---------------------------------------------------------------------------
-- A marcação. `timestamp_servidor` é a hora oficial; a do aparelho é guardada
-- para conferência. `data_referencia` é o dia de trabalho a que a batida
-- pertence (para o vigia noturno, batida antes das 10h é do dia anterior).
-- Horários em TIMESTAMPTZ: o banco guarda UTC e a saída converte para
-- America/Fortaleza — padrão da casa.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ponto.marcacoes (
    id                    BIGSERIAL PRIMARY KEY,
    nsr                   BIGINT      NOT NULL UNIQUE,
    colaborador_id        BIGINT      NOT NULL REFERENCES public.colaboradores (id),
    obra_id               BIGINT      NOT NULL REFERENCES public.obras (id),
    timestamp_servidor    TIMESTAMPTZ NOT NULL DEFAULT now(),
    timestamp_dispositivo TIMESTAMPTZ,
    data_referencia       DATE        NOT NULL,
    latitude              NUMERIC(10,6),
    longitude             NUMERIC(10,6),
    dentro_da_cerca       BOOLEAN,
    distancia_metros      NUMERIC(10,1),
    dispositivo_id        BIGINT      REFERENCES ponto.dispositivos (id),
    foto_id               BIGINT      REFERENCES ponto.fotos (id),
    foto_hash             TEXT,
    origem                TEXT        NOT NULL
                                      CHECK (origem IN ('PWA', 'IDFACE', 'MANUAL')),
    status                TEXT        NOT NULL DEFAULT 'VALIDA'
                                      CHECK (status IN ('VALIDA', 'EM_ANALISE', 'AJUSTADA', 'REJEITADA')),
    motivo_analise        TEXT,
    marcacao_origem_id    BIGINT      REFERENCES ponto.marcacoes (id),
    hash_encadeado        TEXT        NOT NULL,
    registrado_por        TEXT,
    criado_em             TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_ponto_marcacoes_pessoa_dia
    ON ponto.marcacoes (colaborador_id, data_referencia, timestamp_servidor);
CREATE INDEX IF NOT EXISTS ix_ponto_marcacoes_obra_dia
    ON ponto.marcacoes (obra_id, data_referencia);
CREATE INDEX IF NOT EXISTS ix_ponto_marcacoes_status
    ON ponto.marcacoes (status, data_referencia) WHERE status = 'EM_ANALISE';

-- ---------------------------------------------------------------------------
-- Pedidos de ajuste. A decisão (aprovar/negar) é fase 2, na tela; a tabela já
-- existe para a API aceitar o pedido com justificativa.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ponto.ajustes (
    id                 BIGSERIAL PRIMARY KEY,
    marcacao_id        BIGINT      REFERENCES ponto.marcacoes (id),
    colaborador_id     BIGINT      NOT NULL REFERENCES public.colaboradores (id),
    data_referencia    DATE        NOT NULL,
    tipo               TEXT        NOT NULL
                                   CHECK (tipo IN ('INCLUSAO', 'EXCLUSAO', 'ALTERACAO_HORARIO',
                                                   'ALTERACAO_OBRA', 'ABONO')),
    horario_proposto   TIMESTAMPTZ,
    obra_proposta_id   BIGINT      REFERENCES public.obras (id),
    justificativa      TEXT        NOT NULL,
    solicitado_por     TEXT        NOT NULL,
    aprovado_por       TEXT,
    status             TEXT        NOT NULL DEFAULT 'PENDENTE'
                                   CHECK (status IN ('PENDENTE', 'APROVADO', 'NEGADO')),
    decidido_em        TIMESTAMPTZ,
    marcacao_gerada_id BIGINT      REFERENCES ponto.marcacoes (id),
    criado_em          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_ponto_ajustes_status ON ponto.ajustes (status, id);

-- ---------------------------------------------------------------------------
-- Batidas RECUSADAS (aparelho não aprovado, pessoa não autorizada…). Sem isto,
-- ninguém investiga um aparelho clonado nem descobre um tablet mal configurado.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ponto.recusas (
    id             BIGSERIAL PRIMARY KEY,
    device_uuid    TEXT,
    cpf_informado  TEXT,
    obra_informada TEXT,
    origem         TEXT,
    motivo         TEXT        NOT NULL,
    detalhes       JSONB       NOT NULL DEFAULT '{}'::jsonb,
    ip             TEXT,
    criado_em      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_ponto_recusas_quando ON ponto.recusas (criado_em);
