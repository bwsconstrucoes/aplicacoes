-- ============================================================================
-- Migração 012 — aval (dupla confirmação) e diretor financeiro
--
-- Nenhum título vai a pagamento com a assinatura de uma pessoa só. Quem lança
-- é a primeira; o AVAL é a segunda, dado por quem responde pela obra
-- (supervisor da obra, gestor) ou pelo diretor financeiro. Lançamento do
-- escritório (administrativo financeiro) exige aval do diretor.
--
-- A confirmação é registrada como assinatura: quem, quando, de onde (IP),
-- com que dispositivo e um resumo do título no momento do aval — se o título
-- for alterado depois, dá para provar o que foi assinado.
-- ============================================================================

-- Parte 1: só os valores do enum. O PostgreSQL exige que um valor novo
-- esteja COMMITADO antes de ser usado em índice ou consulta — por isso a
-- criação das tabelas fica na migração seguinte.
ALTER TYPE perfil_usuario ADD VALUE IF NOT EXISTS 'DIRETOR_FINANCEIRO';
ALTER TYPE status_titulo  ADD VALUE IF NOT EXISTS 'AGUARDANDO_AVAL';
