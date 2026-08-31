-- ============================================================================
-- Migração 018 — movimentação NEUTRA (o dinheiro que "existe mas não existe")
--
-- Casos reais: entrou na conta um valor que não era daqui e foi devolvido;
-- pagamos por engano de uma conta e outra empresa/conta ressarciu. As duas
-- pontas passam pelo extrato — a conciliação precisa casar — mas o par se
-- anula: não é receita, não é despesa, não é custo de obra e não pode
-- aparecer em nenhuma leitura gerencial.
--
-- A solução: marcar a movimentação como NEUTRA e ligá-la à contraparte. O
-- sistema passa a saber que as duas se cancelam, e sinaliza quando uma ponta
-- fica sozinha (recebeu e não devolveu, pagou e não foi ressarcido).
-- ============================================================================
ALTER TABLE movimentacoes
    ADD COLUMN IF NOT EXISTS neutra          BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS par_id          BIGINT REFERENCES movimentacoes(id),
    ADD COLUMN IF NOT EXISTS motivo_neutra   TEXT,
    ADD COLUMN IF NOT EXISTS contraparte     TEXT,   -- empresa/parceiro envolvido
    ADD COLUMN IF NOT EXISTS sentido         TEXT;   -- ENTRADA | SAIDA

CREATE INDEX IF NOT EXISTS idx_mov_neutra ON movimentacoes (neutra)
    WHERE neutra IS TRUE;
CREATE INDEX IF NOT EXISTS idx_mov_par ON movimentacoes (par_id);
