-- ===========================================================================
-- 025 — BATE PONTO POR EXCEÇÃO (passa por cima da planilha de colaboradores)
--
-- O dono, 31/10/2026: "preciso poder ativar alguém pra bater ponto mesmo que fora das regras até então definidas
-- (…) são exatamente as regras da planilha que eu quero flexibilizar". Até aqui, "a planilha manda": PJ, pró-labore
-- e a obra sem ponto (ESCRUSSAS) ficavam fora, sem jeito de liberar. Agora o DP marca a exceção na janela da pessoa,
-- com o motivo, quem e quando. O desligado e o afastado continuam fora. Colunas novas, nada mexido.
-- ===========================================================================

ALTER TABLE ponto.colaborador_config ADD COLUMN IF NOT EXISTS excecao_planilha BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE ponto.colaborador_config ADD COLUMN IF NOT EXISTS excecao_motivo TEXT;
ALTER TABLE ponto.colaborador_config ADD COLUMN IF NOT EXISTS excecao_por TEXT;
ALTER TABLE ponto.colaborador_config ADD COLUMN IF NOT EXISTS excecao_em TIMESTAMPTZ;
