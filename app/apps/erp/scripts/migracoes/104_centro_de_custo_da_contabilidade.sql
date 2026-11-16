-- ---------------------------------------------------------------------------
-- 104 — O CENTRO DE CUSTO DA CONTABILIDADE, NA OBRA
--
-- Ele, em 16/11/2026, decidindo como a ficha do Fortes acha a obra:
--
--   *"Em relação a setor, eu criei, está na planilha C. Diários, tem a
--   informação, tem a coluna Centro de Custo da Contabilidade. E é exatamente
--   esse setor aí. Ou seja, nós vamos continuar com a nossa nomenclatura e a
--   contabilidade tem a dela também, que também é importante. A gente manter
--   as duas."*
--
-- ⚑ SÃO DUAS NOMENCLATURAS PARA A MESMA OBRA, E AS DUAS FICAM. O ERP chama a
-- obra de "094"; a contabilidade chama de "164.01". A ficha de registro do
-- Fortes traz o número DELES ("Setor: 164.01 - OBRA ESCVARZEA..."), e sem esta
-- coluna não há como saber de que obra a ficha fala — restaria casar pelo
-- NOME escrito à mão, que acerta quase sempre e erra calado em obra de nome
-- parecido.
--
-- A coluna já EXISTE na planilha C. Diários, e até hoje era lida e descartada
-- por não ter onde morar. Agora ela entra no cadastro da obra, e passa a ser a
-- ponte entre o que a BWS chama e o que a contabilidade chama.
--
-- NÃO é única de propósito: planilha em construção tem repetição e branco, e
-- recusar a importação inteira por causa disso seria trocar um problema de
-- cadastro por um de importação. A importação AVISA quando dois centros de
-- custo se repetem — e aí quem arruma é gente.
-- ---------------------------------------------------------------------------
ALTER TABLE obras
    ADD COLUMN IF NOT EXISTS centro_custo_contabil TEXT;

COMMENT ON COLUMN obras.centro_custo_contabil IS
    'Como a contabilidade chama esta obra (ex.: 164.01). Vem da coluna '
    '"Centro de Custo da Contabilidade" da planilha C. Diários, e é por ela '
    'que a ficha de registro do Fortes acha a obra.';

-- Achar a obra pelo número da contabilidade é a consulta que a importação das
-- fichas faz uma vez por pessoa — 575 vezes na carga inicial.
CREATE INDEX IF NOT EXISTS idx_obra_centro_custo_contabil
    ON obras (centro_custo_contabil)
 WHERE centro_custo_contabil IS NOT NULL;
