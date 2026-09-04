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
-- Ficam VAZIAS até a próxima atualização da base. Toda atualização refaz o fato
-- (`fato.reconstruir`, em qualquer um dos quatro modos), então a carga da
-- madrugada preenche sozinha; para ver hoje, Configurações › "Só refazer os
-- números". A tela avisa enquanto estiverem vazias.
-- ===========================================================================
ALTER TABLE painel.fato ADD COLUMN IF NOT EXISTS data_vencimento DATE;
ALTER TABLE painel.fato ADD COLUMN IF NOT EXISTS data_pagamento  DATE;

-- os dois filtros de faixa de data do Analítico passam por aqui
CREATE INDEX IF NOT EXISTS ix_fato_dvenc ON painel.fato(data_vencimento);
CREATE INDEX IF NOT EXISTS ix_fato_dpago ON painel.fato(data_pagamento);
