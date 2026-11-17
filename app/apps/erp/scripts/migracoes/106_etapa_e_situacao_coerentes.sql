-- ===========================================================================
-- ARRUMA AS PESSOAS COM ETAPA E SITUAÇÃO QUE SE CONTRADIZEM
--
-- Achado por ele na tela, em 17/11/2026:
--
--   "Olha a outra incoerência. Se eu clico na etapa Concluído + Situação Só
--    desligados aparece gente. Se eu coloco Concluído + Só Desligados aparece
--    25. Tá errado."
--
-- Estava errado mesmo. "Etapa" e "situação" são campos independentes DE
-- PROPÓSITO — etapa responde "o que falta FAZER neste cadastro?" e manda na
-- fila do DP; situação responde "esta pessoa está na empresa?" e manda na
-- folha e no ponto. Mas independentes não quer dizer livres: cadastro
-- "Concluído" de gente que já saiu da empresa são duas afirmações que não
-- podem ser verdade juntas, e quem abre a fila do DP vê trabalho que não
-- existe.
--
-- Como chegaram ali: cada importação mexia num dos dois campos sem olhar o
-- outro. A importação pela planilha, até hoje, nem trazia a etapa — ela lia a
-- coluna "Fase Atual" só para decidir a data de saída.
--
-- Daqui em diante o `salvar` do cadastro não deixa mais a contradição nascer
-- (ver `core/cadastros/pessoas.py`). Esta migração arruma quem já estava
-- assim, e roda UMA vez.
--
-- A SITUAÇÃO É QUE MANDA, nos três casos — é ela que decide folha e ponto.
-- "Cancelado" fica intocado: é cadastro que nunca virou gente na empresa, e a
-- situação dele é desligado por consequência, não por ter saído.
-- ===========================================================================

-- 1) Fora da empresa: a etapa é "Desligado". Não há mais o que fazer.
UPDATE colaboradores
   SET etapa = 'DESLIGADO'
 WHERE UPPER(COALESCE(situacao, 'ATIVO')) = 'DESLIGADO'
   AND UPPER(COALESCE(etapa, 'CONCLUIDO')) NOT IN ('DESLIGADO', 'CANCELADO');

-- 2) Afastada: a etapa é "Afastado" — o DP acompanha, a pessoa segue cadastrada.
UPDATE colaboradores
   SET etapa = 'AFASTADO'
 WHERE UPPER(COALESCE(situacao, 'ATIVO')) = 'AFASTADO'
   AND UPPER(COALESCE(etapa, 'CONCLUIDO')) NOT IN ('AFASTADO', 'CANCELADO');

-- 3) Na ativa com etapa de quem saiu: volta para a conferência do DP, que é o
--    mesmo caminho da readmissão. Não vai direto para "concluído" — alguém
--    tem de olhar o que mudou nessa ficha.
UPDATE colaboradores
   SET etapa = 'EM_CONFERENCIA'
 WHERE UPPER(COALESCE(situacao, 'ATIVO')) = 'ATIVO'
   AND UPPER(COALESCE(etapa, 'CONCLUIDO')) IN ('DESLIGADO', 'AFASTADO');
