-- ===========================================================================
-- A OBRA DO MÊS, AUTOMÁTICA — e a transferência deixa de ser pedido ao DP
--
-- Ele, 20/11/2026: *"essa transferência de obra vamos fazer de outra forma.
-- Isso é um trabalho a mais para o DP (…) A gente vai considerar um mês
-- trabalhado (…) em qual obra, num determinado mês, aquela pessoa deve ser
-- considerada? Na obra que ela mais trabalhou. Porque isso, em relação a
-- cálculo de FGTS, INSS, é o que deve ser levado em consideração (…) E todo
-- mês isso a gente faz essa alteração. Não vamos botar nada para o DP."*
--
-- Uma linha por pessoa e por mês (competência = o mês do calendário): a obra
-- em que ela trabalhou MAIS DIAS, os dias de cada obra, e a obra que o
-- cadastro tinha antes. A rotina (`core/obra_do_mes.py`) calcula o mês que
-- acabou e troca a obra de cadastro sozinha.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS colaborador_obra_mes (
    colaborador_id   BIGINT NOT NULL REFERENCES colaboradores(id),
    competencia      DATE   NOT NULL,          -- o dia 1 do mês
    obra_id          BIGINT REFERENCES obras(id),
    dias             INTEGER NOT NULL DEFAULT 0,
    dias_por_obra    JSONB  NOT NULL DEFAULT '{}'::jsonb,
    obra_anterior_id BIGINT REFERENCES obras(id),
    mudou            BOOLEAN NOT NULL DEFAULT FALSE,
    criado_em        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (colaborador_id, competencia)
);
CREATE INDEX IF NOT EXISTS ix_colaborador_obra_mes_competencia
    ON colaborador_obra_mes (competencia) WHERE mudou;

-- As transferências que o sistema abriu sozinho no dia 20/11/2026 (a versão
-- que ele recusou) e que ninguém atendeu: canceladas, com o motivo.
UPDATE dp_solicitacoes
   SET situacao = 'CANCELADA', cancelado_em = now(), atualizado_em = now(),
       motivo_cancelamento = 'a obra passou a mudar sozinha, pelo mês trabalhado (20/11/2026)',
       resposta = 'a obra passou a mudar sozinha, pelo mês trabalhado (20/11/2026)'
 WHERE tipo = 'TRANSFERENCIA_OBRA'
   AND situacao IN ('ABERTA', 'EM_ANALISE')
   AND requerente = 'Ponto (obra atual)';
