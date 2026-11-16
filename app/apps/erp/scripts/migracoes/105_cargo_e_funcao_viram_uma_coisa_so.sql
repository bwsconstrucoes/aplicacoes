-- ===========================================================================
-- 105 — CARGO E FUNÇÃO VIRAM UMA COISA SÓ
--
-- Decisão do dono em 16/11/2026, depois de ele mesmo perguntar se eram a
-- mesma coisa: *"em relação a cargo e função, vamos chamar só de cargo e
-- acabou."*
--
-- O QUE HAVIA. Duas tabelas respondendo "o que a pessoa faz":
--   `funcoes` — nome + DIÁRIA de referência. Usada pelo diário de obra
--               (efetivo do dia), pelo portal e pelo ponto.
--   `cargos`  — nome + CBO, descrição, riscos, EPIs, ligado à convenção.
--               Usado pela folha e pela contabilidade.
-- Dava para alguém ser "Pedreiro" na função e "Servente" no cargo sem
-- ninguém notar, e as duas listas cresciam em paralelo.
--
-- ⚑ O QUE ESTA MIGRAÇÃO NÃO FAZ, DE PROPÓSITO: ela NÃO apaga a coluna
-- `colaboradores.funcao_id` nem a tabela `funcoes`. Quatro consultas do PONTO
-- e uma do PORTAL fazem `LEFT JOIN funcoes f ON f.id = c.funcao_id` para
-- mostrar o nome, e o ponto é aparelho homologado — mexer nele a partir daqui
-- seria arriscar o que não precisa ser arriscado.
--
-- Em vez disso:
--   * `funcoes` vira uma VISÃO sobre `cargos` (mesmos ids, mesmos nomes);
--   * `funcao_id` vira uma CÓPIA AUTOMÁTICA de `cargo_id`, mantida pelo
--     próprio banco.
-- Resultado: toda consulta que já existia continua funcionando palavra por
-- palavra, e passa a mostrar o CARGO. Quem escreve, escreve só em cargo.
-- ===========================================================================

-- 1. O cargo passa a carregar a diária, que era a única coisa que só a função
--    tinha.
ALTER TABLE cargos ADD COLUMN IF NOT EXISTS valor_diaria NUMERIC(14,2);
COMMENT ON COLUMN cargos.valor_diaria IS
  'Diária de referência. Veio de funcoes.valor_diaria na migração 105.';

-- 2. Cada função vira cargo. Mesmo nome (sem caixa e sem espaço sobrando) é a
--    MESMA coisa: a diária entra no cargo que já existe.
UPDATE cargos c
   SET valor_diaria = COALESCE(c.valor_diaria, f.valor_diaria)
  FROM funcoes f
 WHERE upper(btrim(c.nome)) = upper(btrim(f.nome));

INSERT INTO cargos (nome, valor_diaria, ativo)
SELECT btrim(f.nome), f.valor_diaria, f.ativo
  FROM funcoes f
 WHERE NOT EXISTS (SELECT 1 FROM cargos c
                    WHERE upper(btrim(c.nome)) = upper(btrim(f.nome)));

-- 3. Quem tinha função e não tinha cargo passa a ter o cargo equivalente.
--    Quem já tinha cargo fica como está: o cargo é o do registro, e ele manda.
UPDATE colaboradores col
   SET cargo_id = c.id
  FROM funcoes f
  JOIN cargos c ON upper(btrim(c.nome)) = upper(btrim(f.nome))
 WHERE col.funcao_id = f.id
   AND col.cargo_id IS NULL;

-- 4. `funcao_id` deixa de ser um dado próprio e passa a ser o espelho de
--    `cargo_id`, mantido pelo banco. Quem lia continua lendo; ninguém mais
--    escreve nele (o ERP passou a escrever só `cargo_id`).
ALTER TABLE colaboradores DROP CONSTRAINT IF EXISTS colaboradores_funcao_id_fkey;
ALTER TABLE colaboradores DROP COLUMN IF EXISTS funcao_id;
ALTER TABLE colaboradores
  ADD COLUMN funcao_id BIGINT GENERATED ALWAYS AS (cargo_id) STORED;
COMMENT ON COLUMN colaboradores.funcao_id IS
  'Cópia automática de cargo_id (migração 105). Só para as consultas antigas '
  'do ponto e do portal continuarem valendo — não escreva aqui.';

-- 5. `funcoes` vira visão de `cargos`. As colunas são as mesmas de antes, na
--    mesma ordem, para nenhuma consulta existente precisar mudar.
DROP TABLE IF EXISTS funcoes CASCADE;
CREATE OR REPLACE VIEW funcoes AS
  SELECT id, nome, valor_diaria, ativo FROM cargos;
COMMENT ON VIEW funcoes IS
  'Visão sobre cargos (migração 105): cargo e função viraram a mesma coisa. '
  'Existe para as consultas do ponto e do portal continuarem valendo.';
