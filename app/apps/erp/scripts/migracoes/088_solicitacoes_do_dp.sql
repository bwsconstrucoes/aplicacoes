-- ============================================================================
-- 088 — A FILA DE SOLICITAÇÕES DO DP
--
-- O segundo pipe do Pipefy ("SOLICITAÇÕES DP", 80.992 cards desde 2021) é uma
-- fila de pedidos com aprovação. O ponto do ERP já absorveu os pedidos de PONTO
-- (atestado, licença, férias, ajuste de batida, compensação, folga do banco);
-- o que sobra é o ADMINISTRATIVO, e é isto.
--
-- DECISÃO DO DONO, 05/11/2026: *"todos os pedidos vão para o DP, e quem tiver
-- perfil do DP que vai aprovar."* Uma etapa só, portanto — diferente do ponto,
-- onde algumas coisas passam pelo encarregado antes. Isso simplifica: não há
-- cadeia para configurar.
--
-- POR QUE UMA TABELA SÓ, com o pedido em JSONB, e não uma tabela por tipo:
-- são 13 tipos que mudam de campo conforme o que se pede (readmissão quer data
-- de admissão; desconto em folha quer valor e parcelas; dependente quer nome e
-- parentesco). Treze tabelas seriam treze telas e treze rotas para a MESMA
-- decisão de autorização — e o pipe, que fez isso com 106 campos num formulário
-- só, mostrou onde dá: metade dos campos vazia em cada card.
--
-- O QUE É COLUNA e o que é JSONB, e o critério não é gosto: vira COLUNA o que
-- alguma TELA FILTRA, SOMA ou ORDENA (tipo, situação, pessoa, obra, datas,
-- valor). Fica no JSONB o que só é lido quando o pedido está aberto na frente.
-- Filtrar por dentro do JSONB é o caminho de consulta lenta que ninguém vê
-- chegar.
-- ============================================================================

CREATE TABLE IF NOT EXISTS dp_solicitacoes (
    id              BIGSERIAL PRIMARY KEY,
    -- O NÚMERO HUMANO, como todo registro do ERP (SDP-000001). O pipe chamava
    -- "Nº SDP" e é por ele que o DP e a obra se referem ao pedido.
    numero          TEXT NOT NULL,
    tipo            TEXT NOT NULL,
    -- ABERTA → (EM_ANALISE) → ATENDIDA | RECUSADA | CANCELADA
    situacao        TEXT NOT NULL DEFAULT 'ABERTA',
    colaborador_id  BIGINT REFERENCES colaboradores(id),
    -- O PEDIDO DE CADASTRO NOVO não tem colaborador ainda: a pessoa só existe
    -- depois que o DP atende. Por isso nome e CPF ficam aqui também, soltos.
    nome_informado  TEXT,
    cpf_informado   TEXT,
    obra_id         BIGINT REFERENCES obras(id),
    -- Quem pediu, e por quem (o pipe perguntava "requisição solicitada por um
    -- terceiro?" porque o encarregado abre pedido para a equipe dele).
    solicitante_id  BIGINT REFERENCES usuarios(id),
    requerente      TEXT,
    -- O que o pedido diz, campo por campo conforme o tipo.
    dados           JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- As datas e o valor que as telas filtram e somam.
    data_referencia DATE,
    valor           NUMERIC(14,2),
    -- O atendimento.
    atendido_por    BIGINT REFERENCES usuarios(id),
    atendido_em     TIMESTAMPTZ,
    resposta        TEXT,
    -- CANCELAR SOLICITAÇÃO é um TIPO no pipe ("Cancelar Solicitação de
    -- Desligamento"), e isso obrigava a abrir um card para cancelar outro.
    -- Aqui é uma ação sobre o próprio pedido, e `cancelada_*` guarda quem e por
    -- quê — mas o pedido não desaparece: pedido apagado é a pergunta "e o que
    -- aconteceu com aquele desligamento?" sem resposta.
    cancelado_por   BIGINT REFERENCES usuarios(id),
    cancelado_em    TIMESTAMPTZ,
    motivo_cancelamento TEXT,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dp_solicitacoes_numero ON dp_solicitacoes (numero);
CREATE INDEX IF NOT EXISTS ix_dp_solicitacoes_fila
    ON dp_solicitacoes (situacao, criado_em DESC);
CREATE INDEX IF NOT EXISTS ix_dp_solicitacoes_pessoa
    ON dp_solicitacoes (colaborador_id, criado_em DESC);
CREATE INDEX IF NOT EXISTS ix_dp_solicitacoes_obra ON dp_solicitacoes (obra_id);
CREATE INDEX IF NOT EXISTS ix_dp_solicitacoes_tipo ON dp_solicitacoes (tipo, situacao);

-- A sequência do número humano, como as outras do ERP.
CREATE SEQUENCE IF NOT EXISTS dp_solicitacao_numero_seq START 1;

-- ----------------------------------------------------------------------------
-- O CHECKLIST DO ATENDIMENTO
--
-- O pipe tinha checklists longos por tipo — o de desligamento CTPS tem ONZE
-- itens ("Solic. de Aviso Prévio", "Lançamento de Faltas", "Impressão de TRCT",
-- "Desativação no Sistema de Ponto"…). Eles existem porque o atendimento é
-- trabalho de várias etapas, feito por mais de uma pessoa, ao longo de dias.
--
-- Fica em tabela própria e não no JSONB porque a pergunta "o que falta neste
-- desligamento?" é de LISTA, não de um pedido só: o DP precisa ver todos os
-- desligamentos e o que falta em cada um.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dp_solicitacao_itens (
    id              BIGSERIAL PRIMARY KEY,
    solicitacao_id  BIGINT NOT NULL REFERENCES dp_solicitacoes(id) ON DELETE CASCADE,
    ordem           SMALLINT NOT NULL DEFAULT 0,
    descricao       TEXT NOT NULL,
    feito           BOOLEAN NOT NULL DEFAULT FALSE,
    feito_por       BIGINT REFERENCES usuarios(id),
    feito_em        TIMESTAMPTZ,
    observacao      TEXT
);

CREATE INDEX IF NOT EXISTS ix_dp_itens_solicitacao
    ON dp_solicitacao_itens (solicitacao_id, ordem);

-- ----------------------------------------------------------------------------
-- A TRILHA DO PEDIDO — quem mexeu, quando, e o que escreveu
--
-- O pipe guardava isso no histórico do card, e o card vai morrer. Sem esta
-- tabela, "quem autorizou essa ajuda de custo?" fica sem resposta no dia em que
-- o Pipefy for desligado.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dp_solicitacao_eventos (
    id              BIGSERIAL PRIMARY KEY,
    solicitacao_id  BIGINT NOT NULL REFERENCES dp_solicitacoes(id) ON DELETE CASCADE,
    acao            TEXT NOT NULL,
    detalhe         JSONB,
    usuario_id      BIGINT REFERENCES usuarios(id),
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_dp_eventos_solicitacao
    ON dp_solicitacao_eventos (solicitacao_id, criado_em);

-- ----------------------------------------------------------------------------
-- A SEÇÃO DE PERFIL, para quem tem perfil cadastrado ver a fila
--
-- Duas ações, e a divisão vem do pipe: lá o formulário perguntava *"requisição
-- solicitada por um terceiro?"* porque o encarregado abre pedido para a equipe
-- dele. Logo, ABRIR pedido é de quem está na obra; ATENDER é do DP — a decisão
-- do dono em 05/11/2026: *"todos os pedidos vão para o DP, e quem tiver perfil
-- do DP que vai aprovar."*
--
-- Sem esta parte, quem usa perfil cadastrado (migração 065) não vê o menu novo,
-- e o teste `test_perfis_cadastro_banco` acusa a divergência entre o que o cargo
-- antigo dá e o que o perfil pronto dá.
-- ----------------------------------------------------------------------------
INSERT INTO perfil_secoes (perfil_id, secao, nivel)
SELECT p.id, v.secao, v.nivel
  FROM perfis p
  JOIN (VALUES
    ('Administrador',          'pes_solicitacoes', 'EDITAR'),
    ('Diretor financeiro',     'pes_solicitacoes', 'EDITAR'),
    ('Departamento pessoal',   'pes_solicitacoes', 'EDITAR'),
    -- Quem está na obra ABRE pedido e acompanha o que abriu; não atende.
    -- "Administrativo financeiro" é o nome do perfil que espelha o cargo
    -- FINANCEIRO (migração 065) — sem ele aqui, o teste que compara o perfil
    -- pronto com o cargo antigo acusa divergência, e com razão: a pessoa
    -- perderia o acesso ao trocar de cargo para perfil.
    ('Administrativo financeiro', 'pes_solicitacoes', 'LER'),
    ('Gestor de obras',        'pes_solicitacoes', 'LER'),
    ('Supervisor de obras',    'pes_solicitacoes', 'LER'),
    ('Administrativo de obra', 'pes_solicitacoes', 'LER')
  ) AS v(perfil, secao, nivel) ON v.perfil = p.nome
ON CONFLICT (perfil_id, secao) DO NOTHING;
