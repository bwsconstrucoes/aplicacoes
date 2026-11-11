-- Cada conta da matriz em TRÊS PARTES (11/11/2026).
--
-- O dono explicou a regra com exemplos: "Categoria X: ratear 100% entre todas.
-- Categoria Y: jogar 20% para BWSNE e ratear 80%. Categoria Z: jogar 100% para
-- BWSNE" — e "de 20.000, 10.000 não entram na conta de rateio e ficam em
-- BWSCE; os 10.000 que sobram são rateados entre BWSCE e a filial".
--
-- Até aqui a linha guardava "quanto entra" (`pct`) e "quanto do que entra vai
-- para o principal" (`pct_principal`) — o que não entrava sumia, e o resto ia
-- só para os demais. Agora guarda as duas partes com destino certo; a
-- terceira (o que se rateia entre todas as obras) é o que falta para 100:
--
--   pct_fica    — % que fica no projeto principal (BWSCE)
--   pct_filial  — % que é da filial (vai só para as demais obras)
--   rateia      — 100 − pct_fica − pct_filial, entre TODAS as obras
--
-- `pct` e `pct_principal` continuam na tabela (o código antigo ainda as lê
-- enquanto esta migração não for aplicada), mas deixam de mandar.

ALTER TABLE painel.cenario_peso_periodo ADD COLUMN IF NOT EXISTS pct_fica   NUMERIC(6,3);
ALTER TABLE painel.cenario_peso_periodo ADD COLUMN IF NOT EXISTS pct_filial NUMERIC(6,3);

-- O que já estava gravado, lido pelo que o dono disse que queria: o que não
-- entrava FICA no principal (em vez de sumir); a parte "para o principal"
-- também fica nele; e o resto se RATEIA entre todas (em vez de ir só para os
-- demais). É a mesma conta que o código faz para linha sem estas colunas.
UPDATE painel.cenario_peso_periodo
   SET pct_fica   = (100 - pct) + pct * COALESCE(pct_principal, 0) / 100,
       pct_filial = 0
 WHERE pct_fica IS NULL;
