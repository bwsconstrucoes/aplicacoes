-- ===========================================================================
-- 008 — QUEM PODE O QUÊ NO APLICATIVO: o administrativo de obra e quem faz
--       pedido pelo próprio celular
--
-- Pedido do dono, 08/10/2026:
--   · "Se eu configurar a pessoa como administrativo de obra, ele estando
--     naquela obra (...) vai conseguir ver o dele da mesma forma. (...) O que
--     vai dar poderes para ele é ele ser considerado administrativo de obra. E
--     o celular de obra vai ter os mesmos poderes."
--   · "Para o colaborador individual (...) a princípio o cara só bate o seu
--     ponto e vê seu ponto. Se a gente entender (...) também faz solicitações
--     — na configuração dele a gente faria essa marcação."
--
-- As duas nascem FALSAS. Ninguém ganha poder com a atualização; quem bate no
-- próprio celular (exceção de 05/10) e fazia pedido por ele passa a precisar
-- da marcação "faz pedidos pelo celular" — é a regra nova do dono.
-- ===========================================================================

ALTER TABLE ponto.colaborador_config
    ADD COLUMN IF NOT EXISTS pede_no_celular BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE ponto.colaborador_config
    ADD COLUMN IF NOT EXISTS administrativo_obra BOOLEAN NOT NULL DEFAULT FALSE;

-- O pedido que o responsável ou o administrativo da obra faz POR OUTRA PESSOA,
-- pelo aplicativo (na consulta do ponto dela), ganha origem própria.
ALTER TABLE ponto.ocorrencias DROP CONSTRAINT IF EXISTS ocorrencias_origem_check;
ALTER TABLE ponto.ocorrencias ADD CONSTRAINT ocorrencias_origem_check
    CHECK (origem IN ('APP', 'GESTAO', 'API', 'APARELHO', 'RESPONSAVEL'));
