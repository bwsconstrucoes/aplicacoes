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
