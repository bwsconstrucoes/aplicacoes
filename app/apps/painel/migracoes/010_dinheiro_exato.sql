-- ===========================================================================
-- 010 — DINHEIRO PASSA A SER GUARDADO EXATO
--
-- O espelho do OMIE nasceu com as colunas de dinheiro em REAL (float de 4
-- bytes). REAL guarda cerca de 7 algarismos significativos: acima de
-- R$ 131.072,00 ele JA NAO CABE os centavos e arredonda para o valor mais
-- proximo que consegue representar.
--
-- Foi assim que uma devolucao de R$ 784.647,07 virou R$ 784.647,06 na base. O
-- dono procurou esse lancamento por SETE DIAS, em 13 a 20/09/2026: ele estava
-- la o tempo todo, so que com um centavo a menos, e por isso a busca por valor
-- exato nunca o achava.
--
-- A tabela `fato` ja era NUMERIC(16,2) — mas ela e CALCULADA a partir destas
-- colunas, entao guardava com precisao o numero errado.
--
-- NUMERIC guarda o numero decimal como ele e escrito, sem aproximacao. E o
-- unico tipo em que dinheiro nao perde centavo.
--
-- ATENCAO, E ISTO E O QUE MAIS IMPORTA:
-- trocar o tipo NAO devolve o centavo que ja se perdeu. O 784.647,06 que esta
-- gravado vira NUMERIC 784.647,06 — o ,07 original nao esta mais em lugar
-- nenhum aqui, so no OMIE. Depois desta migracao e PRECISO rodar a
-- "Primeira carga — baixa toda a base do OMIE" para os valores voltarem
-- certos. Uma atualizacao do dia nao serve: ela so rebaixa o que mudou.
--
-- Percentuais (nperdep, pct) ficam em REAL de proposito: valem no maximo 100,
-- nunca chegam perto do limite de precisao, e a conta que eles fazem termina
-- arredondada a dois decimais de qualquer jeito.
-- ===========================================================================

ALTER TABLE painel.titulos
    ALTER COLUMN valor_documento TYPE NUMERIC(16,2),
    ALTER COLUMN valor_ir        TYPE NUMERIC(16,2),
    ALTER COLUMN valor_iss       TYPE NUMERIC(16,2),
    ALTER COLUMN valor_inss      TYPE NUMERIC(16,2),
    ALTER COLUMN valor_pis       TYPE NUMERIC(16,2),
    ALTER COLUMN valor_cofins    TYPE NUMERIC(16,2),
    ALTER COLUMN valor_csll      TYPE NUMERIC(16,2);

ALTER TABLE painel.rateio
    ALTER COLUMN nvaldep TYPE NUMERIC(16,2);

ALTER TABLE painel.movimentos
    ALTER COLUMN nvalortitulo TYPE NUMERIC(16,2),
    ALTER COLUMN nvalpago     TYPE NUMERIC(16,2),
    ALTER COLUMN nvalliquido  TYPE NUMERIC(16,2),
    ALTER COLUMN nvalaberto   TYPE NUMERIC(16,2),
    ALTER COLUMN njuros       TYPE NUMERIC(16,2),
    ALTER COLUMN nmulta       TYPE NUMERIC(16,2),
    ALTER COLUMN ndesconto    TYPE NUMERIC(16,2);

ALTER TABLE painel.ajustes
    ALTER COLUMN valor TYPE NUMERIC(16,2);
