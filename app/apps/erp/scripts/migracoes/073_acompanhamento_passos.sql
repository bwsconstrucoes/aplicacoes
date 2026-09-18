-- ============================================================================
-- Migração 073 — os PASSOS SUGERIDOS do processo (Acompanhamento, pedaço 2)
--
-- Cada tipo de processo tem uma lista do que costuma ter que ser feito: o
-- aditivo de prazo pede justificativa, cronograma, ofício, protocolo, parecer,
-- assinatura e publicação. Quem nunca tocou um aditivo não sabe essa lista, e
-- quem já tocou esquece um item.
--
-- É SUGESTÃO, E NÃO FLUXO, e a diferença está na ausência de colunas:
--
--   · não há "obrigatório" — nenhum passo impede fechar o processo;
--   · não há "anterior" nem "depende de" — marcar fora de ordem é permitido;
--   · não há "quem pode marcar" — quem toca o processo marca.
--
-- O dono citou o SEI como CONTRAEXEMPLO: *"não quero uma coisa travada"*. A
-- lista serve para lembrar o que costuma faltar, nunca para barrar o caminho
-- que o órgão inventou desta vez.
--
-- Os passos nascem do modelo do tipo, mas são LINHAS e não um JSON fechado:
-- assim dá para acrescentar o passo que só aquele órgão pede, renomear o que
-- tem outro nome na prefeitura, e apagar o que não se aplica.
-- ============================================================================

CREATE TABLE IF NOT EXISTS processo_passos (
    id          BIGSERIAL PRIMARY KEY,
    processo_id BIGINT NOT NULL REFERENCES processos(id) ON DELETE CASCADE,
    texto       TEXT NOT NULL,
    ordem       INTEGER NOT NULL DEFAULT 0,
    feito_em    TIMESTAMPTZ,
    feito_por   BIGINT REFERENCES usuarios(id),
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_processo_passos
    ON processo_passos (processo_id, ordem);

COMMENT ON TABLE processo_passos IS
    'O que costuma ter que ser feito neste tipo de processo. SUGESTÃO: nenhum '
    'passo impede fechar o processo, e marcar fora de ordem é permitido.';
