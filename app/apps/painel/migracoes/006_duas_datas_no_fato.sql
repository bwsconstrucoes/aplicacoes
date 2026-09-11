-- ===========================================================================
-- 006 — Vencimento e pagamento viram duas colunas, em vez de uma só
--
-- A coluna `data` sempre foi as duas coisas ao mesmo tempo: a data do
-- PAGAMENTO quando o título foi quitado, e a do VENCIMENTO quando ainda está em
-- aberto. Isso é o certo para o DRE e para o fluxo de caixa — é sempre "quando
-- o dinheiro andou, ou vai andar" — mas na tela de lançamentos vira uma coluna
-- que ninguém sabe ler. Foi a primeira coisa que o dono não entendeu ao usar o
-- Despesas Analítico.
--
-- Com as duas separadas dá para filtrar por qualquer uma delas e, sobretudo,
-- para medir ATRASO: quantos dias entre o vencimento e o pagamento de fato.
-- Esse número não existia em lugar nenhum do painel.
--
-- A coluna `data` CONTINUA como estava e continua sendo a que o DRE, o fluxo e
-- todas as outras telas usam. Estas duas são acréscimo, não substituição —
-- trocar o significado de `data` mexeria em nove telas conferidas contra o
-- Streamlit, e não é isso que está sendo pedido.
--
-- Ficam VAZIAS até a base ser recalculada. A marca REFAZER-O-FATO abaixo diz
-- isso ao aplicador de migrações, que dispara o recálculo sozinho assim que
-- termina de aplicar — sem baixar nada do OMIE. Antes desta marca existir, o
-- dono precisava descobrir por conta própria que tinha de apertar "Só refazer
-- os números", e reclamou com razão.
-- REFAZER-O-FATO
-- ===========================================================================
ALTER TABLE painel.fato ADD COLUMN IF NOT EXISTS data_vencimento DATE;
ALTER TABLE painel.fato ADD COLUMN IF NOT EXISTS data_pagamento  DATE;

-- os dois filtros de faixa de data do Analítico passam por aqui
CREATE INDEX IF NOT EXISTS ix_fato_dvenc ON painel.fato(data_vencimento);
CREATE INDEX IF NOT EXISTS ix_fato_dpago ON painel.fato(data_pagamento);
