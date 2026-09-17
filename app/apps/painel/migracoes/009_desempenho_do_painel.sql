-- Desempenho: tirar das consultas o que dá para o banco calcular uma vez só.
--
-- 17/09/2026. O dono: "tô achando extremamente lento… nesse exato momento tá
-- inviável". Medido numa base do tamanho da dele (144 mil linhas): 99% do tempo
-- de cada tela estava no banco, com POUCAS consultas — ou seja, não era
-- quantidade, eram consultas caras.
--
-- O culpado principal: a regra de "foi pago" era uma EXPRESSÃO avaliada linha a
-- linha, em toda consulta de toda tela:
--
--     situacao ~* '(pago|recebido|conciliado)'
--
-- Medido, na mesma consulta do ano do DRE:
--     com a expressão ................ 112 ms
--     com comparação simples ......... 32 ms
--     só a varredura, sem predicado .. 26 ms
--
-- A expressão sozinha custava 80 dos 112 ms — 71% do tempo.
--
-- A COLUNA ABAIXO TEM EXATAMENTE A MESMA REGRA. Quem a calcula é o próprio
-- Postgres, na gravação, uma vez por linha. Nenhum número muda: é a mesma
-- comparação, no mesmo texto, feita em outro momento. Por isso esta migração
-- NÃO pede reconstrução do fato — o banco preenche as linhas que já existem no
-- instante em que a coluna nasce.
--
-- (A divergência conhecida entre esta regra e a da carga — que também aceita
-- "liquidado" na baixa do OMIE, e que vale R$ 96.750,00 em 2 títulos — segue
-- em aberto, esperando decisão do dono. Ela é outra conversa: aqui o objetivo
-- é velocidade sem mexer em número nenhum.)

ALTER TABLE painel.fato
    ADD COLUMN IF NOT EXISTS pago BOOLEAN
    GENERATED ALWAYS AS (situacao ~* '(pago|recebido|conciliado)') STORED;

CREATE INDEX IF NOT EXISTS ix_fato_pago ON painel.fato(pago);

-- As telas filtram quase sempre por "foi pago" JUNTO com ano, obra ou análise.
-- Índice composto evita a varredura inteira quando há filtro na barra lateral.
CREATE INDEX IF NOT EXISTS ix_fato_pago_ano  ON painel.fato(pago, ano);
CREATE INDEX IF NOT EXISTS ix_fato_pago_data ON painel.fato(pago, data);

-- A conferência da base crua cruza titulos com fato pelo código do lançamento.
-- Sem isto ela varre as duas tabelas inteiras.
CREATE INDEX IF NOT EXISTS ix_fato_codigo ON painel.fato(codigo_lancamento);
