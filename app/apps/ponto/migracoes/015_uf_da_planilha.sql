-- ===========================================================================
-- 015 — A UF (E O MUNICÍPIO) DA OBRA PELA PLANILHA C. DIÁRIOS
--
-- Pedido do dono, 28/10/2026, depois de cadastrar um feriado estadual de PE: "enquanto a gente
-- não está usando o ERP por completo e a gente está usando a C. Diários, vamos utilizar como UF a
-- coluna AC, lá tem a UF". O feriado estadual e o municipal procuram a UF e o município da obra;
-- até aqui vinham só do cadastro de obras do ERP — em branco em muitas obras, e o feriado não
-- valia (o dia virava falta). Como a coordenada, o que está na planilha vence o ERP.
-- ===========================================================================

ALTER TABLE ponto.obras_planilha ADD COLUMN IF NOT EXISTS uf TEXT;
