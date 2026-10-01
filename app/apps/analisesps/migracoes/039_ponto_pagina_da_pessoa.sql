-- ===========================================================================
-- 039 — EM QUE PÁGINA DO MOBPONTO CADA PESSOA VEIO
--
-- Pedido do dono em 30/09/2026: "é possível eu baixar só o ponto de um
-- funcionário específico? (…) eu fiz uma atualização no ponto do funcionário, e
-- quero que reflita só aquele trecho, para a folha ser recalculada."
--
-- A API do Mobponto não filtra por pessoa: ela entrega o mês em páginas. Para
-- trazer uma pessoa só é preciso saber em QUAL página ela está. Esta coluna
-- guarda isso a cada carga — e na próxima vez a busca vai direto à página
-- certa, em vez de adivinhar pela ordem do nome.
--
-- NULL em tudo o que já foi baixado antes desta atualização: aí a busca
-- adivinha pela ordem alfabética e procura nas páginas vizinhas.
-- ===========================================================================
ALTER TABLE analisesps.ponto_dia
    ADD COLUMN IF NOT EXISTS pagina INTEGER;
