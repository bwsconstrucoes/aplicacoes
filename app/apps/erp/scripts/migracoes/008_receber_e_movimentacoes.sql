-- ============================================================================
-- Migração 008 — títulos A RECEBER (medições) e MOVIMENTAÇÕES entre contas
--
-- (a) O título ganha ESPÉCIE (PAGAR|RECEBER). Reaproveita parcelas, rateios,
--     pagamentos (que viram recebimentos) e a conciliação — em vez de criar um
--     universo paralelo que duplicaria regra e manutenção.
-- (b) Campos de MEDIÇÃO: número, período, contrato/obra e as notas fiscais
--     emitidas (várias por baixa — daí ser lista).
-- (c) MOVIMENTAÇÃO entre contas próprias: lançamento simples, só o essencial,
--     que gera as duas pontas para a conciliação casar.
-- ============================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'especie_titulo') THEN
        CREATE TYPE especie_titulo AS ENUM ('PAGAR', 'RECEBER');
    END IF;
END$$;

ALTER TABLE titulos
    ADD COLUMN IF NOT EXISTS especie especie_titulo NOT NULL DEFAULT 'PAGAR',
    ADD COLUMN IF NOT EXISTS numero_medicao   TEXT,
    ADD COLUMN IF NOT EXISTS periodo_inicio   DATE,
    ADD COLUMN IF NOT EXISTS periodo_fim      DATE,
    ADD COLUMN IF NOT EXISTS notas_fiscais    TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS cliente_id       BIGINT REFERENCES fornecedores(id);

CREATE INDEX IF NOT EXISTS idx_titulos_especie ON titulos (especie, status);
CREATE INDEX IF NOT EXISTS idx_titulos_medicao ON titulos (contrato_id, numero_medicao)
    WHERE numero_medicao IS NOT NULL;

-- fornecedores passam a poder ser clientes também (a construtora fatura para eles)
ALTER TABLE fornecedores
    ADD COLUMN IF NOT EXISTS e_cliente   BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS e_fornecedor BOOLEAN NOT NULL DEFAULT TRUE;

-- movimentações entre contas próprias (transferência, aplicação, resgate)
CREATE TABLE IF NOT EXISTS movimentacoes (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tipo                TEXT        NOT NULL DEFAULT 'TRANSFERENCIA',
    conta_origem_id     BIGINT      REFERENCES contas_bancarias(id),
    conta_destino_id    BIGINT      REFERENCES contas_bancarias(id),
    valor               NUMERIC(14,2) NOT NULL CHECK (valor > 0),
    data_movimento      DATE        NOT NULL,
    descricao           TEXT,
    categoria_id        BIGINT      REFERENCES categorias(id),
    obra_id             BIGINT      REFERENCES obras(id),
    comprovante_anexo_id BIGINT     REFERENCES anexos(id),
    extrato_saida_id    BIGINT      REFERENCES extratos(id),
    extrato_entrada_id  BIGINT      REFERENCES extratos(id),
    criado_por          BIGINT      REFERENCES usuarios(id),
    criado_em           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_mov_contas CHECK (
        conta_origem_id IS DISTINCT FROM conta_destino_id
        OR conta_origem_id IS NULL OR conta_destino_id IS NULL)
);

CREATE INDEX IF NOT EXISTS idx_mov_data ON movimentacoes (data_movimento);
CREATE INDEX IF NOT EXISTS idx_mov_extratos ON movimentacoes (extrato_saida_id, extrato_entrada_id);
