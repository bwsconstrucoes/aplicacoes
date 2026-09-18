-- ============================================================================
-- Migração 074 — o OFÍCIO gerado pelo sistema (Acompanhamento, pedaço 3)
--
-- Pedido do dono, junto com o módulo: *"gerar ofícios, dar entrada, lançar
-- alguma coisa que a gente protocolou"*. É o item mais barato de construir e o
-- que mais economiza tempo — o texto é quase sempre o mesmo, e o que muda são
-- os dados que o ERP já tem: obra, contrato, objeto, órgão, número do processo.
--
-- POR QUE UMA TABELA, e não só um PDF guardado no Arquivo:
--
--   · o NÚMERO precisa ser sequencial por empresa e por ano, e sequência sem
--     tabela é sequência que repete no dia em que duas pessoas geram ao mesmo
--     tempo. A restrição única abaixo é o que impede dois "OF 012/2026";
--   · o TEXTO enviado tem de ficar guardado como foi enviado. Regerar a partir
--     do modelo meses depois daria outro texto — e aí o que está no papel do
--     órgão e o que está no sistema divergiriam sem ninguém perceber.
--
-- O PDF continua no Arquivo (`documento_id`), com o escopo e a busca de sempre.
-- Aqui fica o que o Arquivo não sabe: de que processo ele saiu e que número
-- recebeu.
-- ============================================================================

CREATE TABLE IF NOT EXISTS processo_oficios (
    id            BIGSERIAL PRIMARY KEY,
    processo_id   BIGINT NOT NULL REFERENCES processos(id) ON DELETE CASCADE,
    -- A empresa que assina. É dela a numeração — duas empresas da casa têm
    -- sequências independentes, como no papel.
    empresa_id    BIGINT REFERENCES empresas(id),
    ano           INTEGER NOT NULL,
    sequencia     INTEGER NOT NULL,
    -- "OF 012/2026", montado uma vez e guardado: se o formato mudar um dia, o
    -- que já foi enviado continua citável como foi enviado.
    numero        TEXT NOT NULL,
    destinatario  TEXT,
    assunto       TEXT,
    -- O corpo COMO FOI ENVIADO, depois da edição da pessoa.
    corpo         TEXT NOT NULL,
    documento_id  BIGINT REFERENCES documentos(id),
    criado_por    BIGINT REFERENCES usuarios(id),
    criado_em     TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_oficio_sequencia UNIQUE (empresa_id, ano, sequencia)
);

CREATE INDEX IF NOT EXISTS idx_oficios_processo
    ON processo_oficios (processo_id, criado_em DESC);

COMMENT ON TABLE processo_oficios IS
    'Ofício gerado pelo ERP a partir de modelo, numerado por empresa e ano. '
    'O corpo fica guardado COMO FOI ENVIADO; o PDF vive no Arquivo.';


-- ---------------------------------------------------------------------------
-- O TIPO DE DOCUMENTO "OFÍCIO", criado aqui e não só no catálogo em código
--
-- O catálogo de tipos (`core/arquivo/catalogo.py`) é aplicado por um BOTÃO
-- próprio, em Configurações. Se o tipo dependesse só dele, o primeiro ofício
-- gerado depois da publicação sairia com número e NÃO seria arquivado —
-- calado, porque o ofício vale mesmo sem o arquivamento (perder o número por
-- causa do arquivo seria o rabo abanando o cachorro).
--
-- Silêncio é exatamente o que não se quer aqui. Criando o tipo na migração, o
-- botão que o dono já aperta ("Aplicar atualizações do banco") basta. O
-- catálogo em código continua sendo a fonte da verdade: este INSERT não
-- sobrescreve nada e não roda de novo.
-- ---------------------------------------------------------------------------
INSERT INTO documento_tipos (codigo, nome, grupo, dono, vence, avisar_dias,
                             por_competencia, sigilo, ordem)
     VALUES ('OFICIO', 'Ofício expedido', 'OBRA', 'OBRA', false, NULL,
             false, 'ABERTO', 33)
ON CONFLICT (codigo) DO NOTHING;
