-- 024 — O percentual de uma conta da matriz pode mudar com o tempo
--
-- 09/11/2026, o dono: "o rateio vai variar no tempo. De janeiro a dezembro de
-- 2021, tantos por cento; no período seguinte pode ser outro (…) 50-50 em
-- 2021, 60-40 em 2022. Não em todas as contas — e fácil de executar."
--
-- Cada linha diz "A PARTIR DE tal mês, tal percentual" e vale até o mês em que
-- começa a seguinte. Não há data de fim: assim não existe buraco nem
-- sobreposição entre períodos para alguém acertar à mão. Antes do primeiro
-- período, vale o percentual da linha (cenario_peso) — ou, sem ele, o do nível
-- de cima.

CREATE TABLE IF NOT EXISTS painel.cenario_peso_periodo (
    cenario_id  BIGINT NOT NULL REFERENCES painel.cenario(id) ON DELETE CASCADE,
    nivel       TEXT NOT NULL,            -- grupo / categoria / lancamento
    chave       TEXT NOT NULL,
    desde       TEXT NOT NULL,            -- AAAA-MM
    pct         NUMERIC(6,3) NOT NULL DEFAULT 0,
    PRIMARY KEY (cenario_id, nivel, chave, desde)
);

-- E a parte do PROJETO PRINCIPAL (09/11/2026, na mesma conversa): "num
-- determinado período, 40% ficou para a BWS CE, os demais 60% para os outros
-- projetos que recebem". Em branco: a conta se divide entre todas as obras
-- pela régua, sem separar os dois lados.
ALTER TABLE painel.cenario_peso_periodo ADD COLUMN IF NOT EXISTS pct_principal NUMERIC(6,3);

-- Quem RECEBE o custo da matriz, por projeto. O dono: "alguns projetos não
-- entram nessa distribuição, e outros entram até determinada data". Projeto sem
-- linha aqui recebe sempre. `papel`:
--   principal  — o projeto principal (o lado "BWS CE"); um por cenário;
--   nao        — não recebe nada;
--   periodo    — recebe só entre `desde` e `ate` (AAAA-MM, vazio = sem limite).
-- Não é o mesmo que "fora da análise": o projeto continua no resultado e nas
-- quotas; só não recebe a estrutura da matriz.
CREATE TABLE IF NOT EXISTS painel.cenario_projeto (
    cenario_id  BIGINT NOT NULL REFERENCES painel.cenario(id) ON DELETE CASCADE,
    projeto     TEXT NOT NULL,
    papel       TEXT NOT NULL,
    desde       TEXT NOT NULL DEFAULT '',
    ate         TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (cenario_id, projeto)
);
