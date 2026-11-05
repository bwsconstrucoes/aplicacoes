-- ===========================================================================
-- 089 — AS ETAPAS DO CADASTRO DE PESSOAS
--
-- As "fases" do card no Pipefy não eram enfeite: o dono explicou em
-- 05/11/2026 que elas são DIRECIONAMENTO — *"a parte de fase do card ela serve
-- de direcionamento pra entender o que precisa ser feito em relação àquele
-- cadastro."* Então elas vêm para o ERP como ETAPA, com o caminho que ele
-- descreveu:
--
--   formulário inicial  →  CADASTRO_INICIAL
--                       →  EM_CONFERENCIA  (o DP confere a coerência e completa
--                          o que está dentro dos documentos — agora por leitura
--                          de IA, para o formulário ter o MÍNIMO de campos)
--                       →  aí BIFURCA pelo vínculo:
--
--     PRESTADOR  ·  PJ                      → CONTRATO_A_ASSINAR → CONCLUIDO
--                ·  diarista / autônomo     → CONCLUIDO (resolve no DP)
--     CTPS                                  → NA_CONTABILIDADE → CONCLUIDO
--                   (a contabilidade faz o registro formal no eSocial, e o ERP
--                    cobra o retorno — ver migração 090)
--
-- E as três saídas que o pipe já tinha: AFASTADO, DESLIGADO, CANCELADO.
--
-- POR QUE A ETAPA NÃO É A SITUAÇÃO. `situacao` responde "esta pessoa está na
-- empresa?" e manda na folha. `etapa` responde "o que falta fazer neste
-- cadastro?" e manda na fila de trabalho do DP. Misturar as duas foi o que, no
-- Pipefy, fez cadastro concluído conviver com card parado em fase de análise.
-- ===========================================================================

ALTER TABLE colaboradores
    ADD COLUMN IF NOT EXISTS etapa TEXT NOT NULL DEFAULT 'CONCLUIDO';

-- QUEM JÁ ESTÁ NO BANCO NASCE CONCLUÍDO, e isso é de propósito: são as pessoas
-- que já trabalham e já recebem. Marcá-las como "em conferência" criaria, no
-- primeiro dia, uma fila de trabalho falsa com a empresa inteira dentro.
COMMENT ON COLUMN colaboradores.etapa IS
    'O que falta fazer neste cadastro (direcionamento), não se a pessoa está ativa';

-- A fila do DP pergunta "quem está parado em cada etapa?" a cada abertura de
-- tela. O índice parcial deixa de fora o CONCLUIDO, que é a esmagadora maioria.
CREATE INDEX IF NOT EXISTS ix_colaboradores_etapa
    ON colaboradores (etapa) WHERE etapa <> 'CONCLUIDO';

-- ---------------------------------------------------------------------------
-- A TRILHA DA ETAPA: de onde para onde, quando e por quem.
--
-- Sem ela, "este cadastro está há quanto tempo parado?" não tem resposta — e
-- era essa a pergunta que o quadro do Pipefy respondia de graça, só por ser um
-- quadro. É o que se perde ao trocar quadro por tela, e o que esta tabela
-- devolve.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS colaborador_etapas (
    id              BIGSERIAL PRIMARY KEY,
    colaborador_id  BIGINT NOT NULL REFERENCES colaboradores(id) ON DELETE CASCADE,
    de              TEXT,
    para            TEXT NOT NULL,
    motivo          TEXT,
    -- Quem moveu. NULL quando foi o sistema (a bifurcação automática pelo
    -- vínculo), e a tela precisa poder dizer "automático" em vez de inventar
    -- um nome.
    usuario_id      BIGINT REFERENCES usuarios(id),
    automatico      BOOLEAN NOT NULL DEFAULT FALSE,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_colaborador_etapas_pessoa
    ON colaborador_etapas (colaborador_id, criado_em DESC);
