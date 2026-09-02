-- ===========================================================================
-- 004 — A medição vira coluna do fato
--
-- Os títulos a receber do OMIE vêm SEM número de documento, e cada parcela de
-- uma medição é um título separado. O que liga as parcelas é a observação, no
-- padrão "OBRA|Medição No: N". Extrair obra + número dá uma chave estável.
--
-- Essa chave era calculada em Python, na hora de montar a tela. Agora ela é
-- gravada junto com a linha: assim a tela "Receita de Obra" pode pedir ao banco
-- "some por medição" em vez de trazer as linhas e agrupar aqui — que é a coisa
-- que este painel existe para não fazer mais.
--
-- Colunas novas ficam vazias até a próxima atualização da base. A tela avisa.
-- ===========================================================================
ALTER TABLE painel.fato ADD COLUMN IF NOT EXISTS medicao         TEXT;
ALTER TABLE painel.fato ADD COLUMN IF NOT EXISTS medicao_rotulo  TEXT;

CREATE INDEX IF NOT EXISTS ix_fato_medicao ON painel.fato(medicao);
