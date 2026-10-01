-- ===========================================================================
-- TRANSFERENCIA ENTRE CONTAS — 24/09/2026
--
-- Pedido do dono: *"transferencia no OMIE. Nao sei se seleciona a conta
-- origem, conta destino, entao ja aparece nas duas. Funciona assim."*
--
-- ⚠️ COMO O OMIE REPRESENTA TRANSFERENCIA, e foi o espelho do painel que
-- respondeu: ela e um PAR DE TITULOS com a categoria marcada como
-- `transferencia = S` no plano financeiro. E essa marca que a tira do DRE
-- (ver `painel/sync/fato.py`: transferencia=S vai para o balde TRF, nao para
-- o resultado). Ou seja: nao ha rota especial a inventar — e o mesmo caminho
-- dos aportes, que roda e ele ja validou.
--
-- Duas colunas novas:
--   `natureza`        no tipo: 'normal' ou 'transferencia'.
--   `omie_codigo_par` na linha: o SEGUNDO titulo, o da conta de destino.
--                     Sem ele, desfazer ou conferir so acharia metade.
-- ===========================================================================
ALTER TABLE analisesps.conciliacao_tipo
    ADD COLUMN IF NOT EXISTS natureza TEXT NOT NULL DEFAULT 'normal';

ALTER TABLE analisesps.conciliacao_extrato
    ADD COLUMN IF NOT EXISTS omie_codigo_par BIGINT;

-- A conta de destino escolhida no lancamento, para a tela contar depois o que
-- aconteceu — e para a OUTRA PONTA ser reconhecida quando o extrato dela
-- chegar.
ALTER TABLE analisesps.conciliacao_extrato
    ADD COLUMN IF NOT EXISTS conta_par_id INTEGER
        REFERENCES analisesps.conciliacao_conta(id);
