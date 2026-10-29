-- ===========================================================================
-- 020 — A DECLARAÇÃO DE COMPARECIMENTO ABONADA POR UMA QUANTIDADE DE HORAS
--
-- Pedido do dono, 30/10/2026: "a declaração fala de um determinado horário, 6h20 às 9h45. A gente
-- precisa ter a opção de aprovar na totalidade, ou o período, ou uma determinada quantidade de horas —
-- para a pessoa ficar devendo ou levar falta."
--
-- `minutos` continua guardando o período escrito na declaração (é o que a tela mostra); a quantidade
-- que quem aprovou decidiu abonar vai aqui. Só vale com o subtipo DECL_HORAS.
-- ===========================================================================

ALTER TABLE ponto.ocorrencias ADD COLUMN IF NOT EXISTS abono_minutos INTEGER;
