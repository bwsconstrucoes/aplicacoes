-- ===========================================================================
-- 090 — PEDIDOS À CONTABILIDADE E O AGENTE DE DP
--
-- Desenho inteiro em `AGENTE_DP.md`, nesta pasta. Resumo do que esta migração
-- cria e por quê:
--
-- 1. `dp_pedidos` — o pedido formal que o DP manda para a contabilidade
--    (assinatura de CTPS, férias, rescisão, alteração de cargo…). Hoje isso é
--    um e-mail disparado do Pipefy, e **ninguém lembra que pediu**. A tabela
--    existe para o pedido ter prazo, cobrança e retorno esperado.
--
-- 2. `dp_pedido_eventos` — a trilha: enviado, cobrado, respondido, divergente,
--    concluído. Com AUTOR, e com a marca de quem foi o AGENTE — porque quem
--    audita precisa separar o que foi gente do que foi máquina.
--
-- 3. `dp_agente_rotinas` — as sete rotinas, cada uma com modo
--    (DESLIGADA / SUGERE / FAZ) e parâmetros. **Todas nascem DESLIGADAS**,
--    porque foi o que ele pediu com estas palavras: *"a gente precisa poder
--    ativar e desativar isso aí, que a gente não vai começar com ele ativo."*
--
-- 4. `dp_agente_sugestoes` — o que o agente FARIA, no modo SUGERE. É o modo de
--    ganhar confiança: liga, olha uma semana, e só então promove para FAZ.
-- ===========================================================================

CREATE SEQUENCE IF NOT EXISTS dp_pedido_numero_seq;

CREATE TABLE IF NOT EXISTS dp_pedidos (
    id              BIGSERIAL PRIMARY KEY,
    -- Numeração humana, como o resto do ERP: PC-0001, SS-0001, SDP-000001…
    numero          TEXT NOT NULL UNIQUE,
    template        TEXT NOT NULL,
    situacao        TEXT NOT NULL DEFAULT 'RASCUNHO',

    colaborador_id  BIGINT REFERENCES colaboradores(id),
    -- O pedido pode nascer de uma solicitação da obra (a fila do DP) — e aí os
    -- dois ficam ligados, para a obra poder acompanhar o que pediu sem ter de
    -- perguntar ao DP.
    solicitacao_id  BIGINT REFERENCES dp_solicitacoes(id),
    empresa_id      BIGINT REFERENCES empresas(id),

    -- O que foi mandado, exatamente. Guardado e não recalculado: o template
    -- muda com o tempo, e o pedido de março tem de continuar mostrando o texto
    -- de março.
    assunto         TEXT,
    corpo           TEXT,
    para            TEXT[] NOT NULL DEFAULT '{}',
    dados           JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- O RETORNO ESPERADO. Sem isto o pedido não tem como ser conferido nem
    -- cobrado: "esperar" exige saber o quê e até quando.
    documento_esperado TEXT,
    prazo           DATE,
    cobrancas       INTEGER NOT NULL DEFAULT 0,
    cobrado_em      TIMESTAMPTZ,
    escalado_em     TIMESTAMPTZ,

    -- O retorno que chegou.
    documento_id    BIGINT REFERENCES documentos(id),
    respondido_em   TIMESTAMPTZ,
    divergencias    JSONB NOT NULL DEFAULT '[]'::jsonb,
    concluido_em    TIMESTAMPTZ,
    concluido_por   BIGINT REFERENCES usuarios(id),

    aberto_por      BIGINT REFERENCES usuarios(id),
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A pergunta que a tela faz sempre: "o que está aberto, e o que venceu?".
CREATE INDEX IF NOT EXISTS ix_dp_pedidos_abertos
    ON dp_pedidos (prazo) WHERE situacao IN ('ENVIADO', 'COBRADO', 'RESPONDIDO');
CREATE INDEX IF NOT EXISTS ix_dp_pedidos_pessoa
    ON dp_pedidos (colaborador_id, criado_em DESC);

CREATE TABLE IF NOT EXISTS dp_pedido_eventos (
    id          BIGSERIAL PRIMARY KEY,
    pedido_id   BIGINT NOT NULL REFERENCES dp_pedidos(id) ON DELETE CASCADE,
    tipo        TEXT NOT NULL,
    texto       TEXT,
    dados       JSONB NOT NULL DEFAULT '{}'::jsonb,
    usuario_id  BIGINT REFERENCES usuarios(id),
    -- A marca que separa gente de máquina. Sem ela, a auditoria de um ano
    -- depois não tem como saber quem decidiu o quê.
    pelo_agente BOOLEAN NOT NULL DEFAULT FALSE,
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_dp_pedido_eventos
    ON dp_pedido_eventos (pedido_id, criado_em, id);

-- ---------------------------------------------------------------------------
-- AS ROTINAS DO AGENTE. Uma linha por rotina, com modo e parâmetros.
--
-- Por que tabela e não variável de ambiente: porque quem liga e desliga é o
-- dono, na tela, e não quem faz deploy. E porque o parâmetro de cada rotina
-- (prazo, quantas cobranças, o que conferir) é decisão de negócio que muda sem
-- código novo.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dp_agente_rotinas (
    chave       TEXT PRIMARY KEY,
    modo        TEXT NOT NULL DEFAULT 'DESLIGADA',
    parametros  JSONB NOT NULL DEFAULT '{}'::jsonb,
    rodou_em    TIMESTAMPTZ,
    alterado_por BIGINT REFERENCES usuarios(id),
    alterado_em TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT dp_agente_modo CHECK (modo IN ('DESLIGADA', 'SUGERE', 'FAZ'))
);

-- TODAS DESLIGADAS, e isto é o pedido dele, não um padrão meu.
INSERT INTO dp_agente_rotinas (chave, modo) VALUES
    ('triagem',    'DESLIGADA'),
    ('leitura',    'DESLIGADA'),
    ('despacho',   'DESLIGADA'),
    ('cobranca',   'DESLIGADA'),
    ('conferencia','DESLIGADA'),
    ('vigia',      'DESLIGADA'),
    ('relatorio',  'DESLIGADA')
ON CONFLICT (chave) DO NOTHING;

-- ---------------------------------------------------------------------------
-- O QUE O AGENTE FARIA — o modo SUGERE.
--
-- É o que permite ligar sem medo: a rotina analisa, escreve aqui o que faria, e
-- uma pessoa aprova com um clique. Uma semana disso diz se ele acerta.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dp_agente_sugestoes (
    id          BIGSERIAL PRIMARY KEY,
    rotina      TEXT NOT NULL,
    -- Sobre o que é a sugestão: 'colaborador', 'dp_solicitacao', 'dp_pedido'.
    alvo_tipo   TEXT NOT NULL,
    alvo_id     BIGINT NOT NULL,
    acao        TEXT NOT NULL,
    -- Em português, para a pessoa decidir sem ler código.
    explicacao  TEXT NOT NULL,
    dados       JSONB NOT NULL DEFAULT '{}'::jsonb,
    situacao    TEXT NOT NULL DEFAULT 'ABERTA',
    decidido_por BIGINT REFERENCES usuarios(id),
    decidido_em TIMESTAMPTZ,
    motivo      TEXT,
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT dp_sugestao_situacao
        CHECK (situacao IN ('ABERTA', 'APLICADA', 'RECUSADA', 'VENCIDA'))
);

CREATE INDEX IF NOT EXISTS ix_dp_agente_sugestoes_abertas
    ON dp_agente_sugestoes (rotina, criado_em DESC) WHERE situacao = 'ABERTA';
-- A MESMA SUGESTÃO NÃO SE REPETE: rotina que roda todo dia geraria a mesma
-- linha todo dia, e a tela viraria um monte de ruído igual.
CREATE UNIQUE INDEX IF NOT EXISTS ux_dp_agente_sugestao_aberta
    ON dp_agente_sugestoes (rotina, alvo_tipo, alvo_id, acao)
    WHERE situacao = 'ABERTA';

-- ---------------------------------------------------------------------------
-- QUEM VÊ E QUEM MEXE. A tela nova entra nas seções, como toda tela do ERP.
-- ---------------------------------------------------------------------------
INSERT INTO perfil_secoes (perfil_id, secao, nivel)
SELECT p.id, v.secao, v.nivel
  FROM perfis p
  JOIN (VALUES
    ('Administrador',          'pes_contabilidade', 'EDITAR'),
    ('Diretor financeiro',     'pes_contabilidade', 'EDITAR'),
    ('Departamento pessoal',   'pes_contabilidade', 'EDITAR'),
    -- Quem está na obra ACOMPANHA o pedido que nasceu da requisição dela — e é
    -- isso que acaba com o "e aí, saiu?" por WhatsApp. Não mexe no pedido.
    -- "Administrativo financeiro" é o nome do perfil que espelha o cargo
    -- FINANCEIRO (migração 065): sem ele aqui, o teste que compara o perfil
    -- pronto com o cargo antigo acusa divergência, e com razão.
    ('Administrativo financeiro', 'pes_contabilidade', 'LER'),
    ('Gestor de obras',        'pes_contabilidade', 'LER'),
    ('Supervisor de obras',    'pes_contabilidade', 'LER'),
    ('Administrativo de obra', 'pes_contabilidade', 'LER')
  ) AS v(perfil, secao, nivel) ON v.perfil = p.nome
ON CONFLICT (perfil_id, secao) DO NOTHING;
