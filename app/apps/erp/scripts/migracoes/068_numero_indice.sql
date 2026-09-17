-- ============================================================================
-- Migração 068 — o NÚMERO-ÍNDICE, ao lado da variação mensal
--
-- O PEDIDO, do dono, em 14/09/2026:
--
--   "Você puxou e colocou a variação de um mês pro outro, mas a gente precisa
--   do índice mesmo. Não só variação, porque normalmente a gente calcula
--   através do índice, que é um vírgula zero alguma coisa, um vírgula quatorze,
--   uns quebrados, tem inclusive várias casas decimais. (…) Caso a gente queira
--   saber qual índice inicial, qual índice final, a gente precisa visualizar
--   eles dessa forma. (…) Pode manter a coluna de percentual, contanto que
--   tenha também a de índice."
--
-- O QUE ENTRA, e de onde vem o número
--
-- O Banco Central republica o INCC-DI como VARIAÇÃO mensal (série 192) — é o
-- que a tabela tinha. O número-índice desta coluna é **acumulado pelo próprio
-- sistema**, mês a mês, a partir dessas variações:
--
--     índice do mês = índice do mês anterior × (1 + variação/100)
--
-- com **base 100 no mês mais antigo guardado** da série.
--
-- ⚠️ O QUE ISSO SIGNIFICA NA PRÁTICA, e precisa estar escrito: o NÚMERO em si
-- não é o mesmo que o boletim da FGV publica, porque a base é outra. **A RAZÃO
-- entre dois meses é idêntica** — e é ela que faz o reajuste:
--
--     fator = índice final ÷ índice inicial
--
-- Ou seja: serve exatamente para o que o dono pediu (ver o índice inicial e o
-- final e dividir um pelo outro), e o resultado bate com o da planilha dele.
-- Reproduzir o número da FGV exigiria a série do número-índice deles, que é
-- licenciada — o Banco Central só republica a variação.
-- ============================================================================

ALTER TABLE indices_economicos
    ADD COLUMN IF NOT EXISTS numero_indice NUMERIC(18, 6);

COMMENT ON COLUMN indices_economicos.numero_indice IS
    'Número-índice acumulado pelo sistema a partir da variação mensal, base '
    '100 no mês mais antigo da série. A RAZÃO entre dois meses é o fator de '
    'reajuste; o número absoluto depende da base e não é o da FGV.';

-- ---------------------------------------------------------------------------
-- PREENCHE O QUE JÁ ESTÁ GUARDADO, para a coluna não nascer vazia.
--
-- Mesma conta do sistema, escrita em SQL: o índice do mês é o do mês anterior
-- vezes (1 + variação/100), com 100 no mês mais antigo de cada série. Aqui ela
-- sai de um somatório de logaritmos — é como o Postgres faz produto acumulado.
--
-- Subtrair o acumulado do PRIMEIRO mês é o que põe exatamente 100 nele: a
-- variação do mês da base não entra, porque a base é o ponto zero.
--
-- A partir da próxima coleta quem manda é o cálculo do sistema, em precisão
-- decimal. A diferença entre os dois fica muito abaixo da sexta casa; se
-- alguma linha mudar de um dígito no último decimal depois da primeira busca,
-- é isso — e não perda de dado.
-- ---------------------------------------------------------------------------
WITH acumulado AS (
    SELECT codigo, competencia,
           SUM(LN(1 + variacao_pct / 100))
               OVER (PARTITION BY codigo ORDER BY competencia) AS ate_aqui
      FROM indices_economicos
),
base AS (
    SELECT a.codigo, a.ate_aqui AS no_primeiro
      FROM acumulado a
      JOIN (SELECT codigo, MIN(competencia) AS primeiro
              FROM indices_economicos GROUP BY codigo) p
        ON p.codigo = a.codigo AND p.primeiro = a.competencia
)
UPDATE indices_economicos e
   SET numero_indice = ROUND((100 * EXP(a.ate_aqui - b.no_primeiro))::numeric, 6)
  FROM acumulado a, base b
 WHERE a.codigo = e.codigo
   AND a.competencia = e.competencia
   AND b.codigo = e.codigo
   AND e.numero_indice IS NULL;
