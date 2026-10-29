-- ===========================================================================
-- 018 — "NÃO BATE PONTO", COM O PORQUÊ
--
-- Pedido do dono, 29/10/2026: "eu queria poder selecionar pessoas e dentro do cadastro dela dizer que
-- aquela pessoa não bate ponto." A marcação já existia (`colaborador_config.ativo`, escondida no
-- "Jornada e obras"); agora fica à vista na janela da pessoa e em lote na tela Pessoas — e guarda o
-- motivo, quem marcou e quando: a pessoa some do Quadro, dos relatórios e do aplicativo, e quem
-- procurar por ela precisa saber por quê.
-- ===========================================================================

ALTER TABLE ponto.colaborador_config ADD COLUMN IF NOT EXISTS inativo_motivo TEXT;
ALTER TABLE ponto.colaborador_config ADD COLUMN IF NOT EXISTS inativo_por    TEXT;
ALTER TABLE ponto.colaborador_config ADD COLUMN IF NOT EXISTS inativo_em     TIMESTAMPTZ;
