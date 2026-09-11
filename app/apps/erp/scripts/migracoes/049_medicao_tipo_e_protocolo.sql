-- ===========================================================================
-- Migração 049 — o TIPO da medição, a CORRELAÇÃO entre elas, e o PROTOCOLO.
--
-- POR QUE O TIPO É TABELA E NÃO LISTA NO CÓDIGO
--
-- Foi o ponto mais detalhado da fala do dono, e o mais fácil de errar. O caso
-- simples é a medição 1 e a medição 1R (o reajuste dela). Mas ele descreveu
-- três desvios que acontecem de verdade:
--
--   1. Órgão que numera em SEQUÊNCIA: houve a 1, a 2, e o reajuste virou a
--      medição 3 — entrou na fila como se fosse normal.
--   2. Órgão que numera em PARALELO: o reajuste da 1 é a 1R, correlacionada.
--   3. Medições SUBSIDIÁRIAS: *"já aconteceu uma medição 1 alguma coisa e
--      outra medição 1 alguma coisa, por conta de fontes diferentes, e o órgão
--      trata dessa forma."*
--
-- QUEM MANDA NA NOMENCLATURA É O ÓRGÃO, NÃO O ERP. Por isso o tipo é catálogo
-- editável, e o número da medição continua sendo TEXTO LIVRE (já era). Impor
-- "1, 2, 3" quebraria no primeiro contrato fora do padrão — e ele já viu isso
-- acontecer.
--
-- A CORRELAÇÃO É OPCIONAL DE PROPÓSITO: no órgão que numera em sequência, o
-- reajuste é a medição 3 e mesmo assim aponta para a 1. É a ligação que permite
-- dizer, no quadro do contrato, "a medição 1 rendeu X, mais Y de reajuste".
-- Sem ela os dois valores ficam soltos e ninguém soma.
--
-- O PROTOCOLO destrava um indicador que hoje não existe em lugar nenhum:
-- quantos dias entre protocolar e receber, por obra e por órgão. Palavras dele:
-- *"isso é bom porque a gente pode gerar indicadores para saber o tempo de
-- recebimento."*
-- ===========================================================================

CREATE TABLE IF NOT EXISTS medicao_tipos (
    codigo      TEXT PRIMARY KEY,
    nome        TEXT NOT NULL,
    -- Tipo de REAJUSTE aponta para a medição que reajusta. É o que faz o
    -- sistema saber que aquele valor é acréscimo, e não medição nova.
    e_reajuste  BOOLEAN NOT NULL DEFAULT FALSE,
    ativo       BOOLEAN NOT NULL DEFAULT TRUE,
    ordem       INT NOT NULL DEFAULT 100,
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE medicao_tipos DROP CONSTRAINT IF EXISTS ck_medicao_tipo_codigo;
ALTER TABLE medicao_tipos ADD CONSTRAINT ck_medicao_tipo_codigo
    CHECK (codigo ~ '^[A-Z0-9]+(-[A-Z0-9]+)*$');

ALTER TABLE titulos ADD COLUMN IF NOT EXISTS medicao_tipo TEXT
    REFERENCES medicao_tipos(codigo);
ALTER TABLE titulos ADD COLUMN IF NOT EXISTS medicao_de_id BIGINT
    REFERENCES titulos(id);
ALTER TABLE titulos ADD COLUMN IF NOT EXISTS protocolo_numero TEXT;
ALTER TABLE titulos ADD COLUMN IF NOT EXISTS protocolo_em DATE;

-- Uma medição não pode ser reajuste DE SI MESMA. Parece bobo até alguém clicar
-- errado numa lista e o quadro do contrato somar o valor duas vezes.
ALTER TABLE titulos DROP CONSTRAINT IF EXISTS ck_medicao_nao_aponta_para_si;
ALTER TABLE titulos ADD CONSTRAINT ck_medicao_nao_aponta_para_si
    CHECK (medicao_de_id IS NULL OR medicao_de_id <> id);

CREATE INDEX IF NOT EXISTS idx_titulo_medicao_tipo ON titulos (medicao_tipo);
CREATE INDEX IF NOT EXISTS idx_titulo_medicao_de ON titulos (medicao_de_id)
    WHERE medicao_de_id IS NOT NULL;
-- A pergunta "o que está protocolado e ainda não recebi?" merece índice: é ela
-- que alimenta o indicador de tempo de recebimento.
CREATE INDEX IF NOT EXISTS idx_titulo_protocolo ON titulos (protocolo_em)
    WHERE protocolo_em IS NOT NULL;
