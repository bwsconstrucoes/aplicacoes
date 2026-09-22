-- ============================================================================
-- Migração 080 — A EMPRESA DONA DA CONTA BANCÁRIA, E A EMPRESA DO TÍTULO
--
-- PEDIDO DO DONO, 22/09/2026, olhando a tela de contas: *"uma coisa que acho
-- que precisa fazer, ou não? Associar banco a uma empresa."*
--
-- ELE ESTÁ CERTO, E O BURACO É MAIOR DO QUE PARECE. A conta bancária era uma
-- lista solta: descrição, banco, agência, conta. Não pertencia a empresa
-- nenhuma. Quem tem empresa é a OBRA; a conta, não. Três consequências, todas
-- reais:
--
--   1. Em toda tela que escolhe conta -- pagar, importar extrato, apontar a
--      conta da obra -- apareciam as contas de TODAS as empresas misturadas,
--      sem nada dizendo de quem era cada uma. A única defesa era a pessoa
--      reconhecer pela descrição que alguém digitou.
--   2. Nada impedia pagar um título de uma empresa pela conta de outra. Esse
--      erro não se conserta no sistema: o dinheiro saiu do CNPJ errado, vira
--      acerto entre empresas e passa por movimentação bancária de verdade.
--   3. "Quanto tem em caixa nesta empresa" não tinha resposta.
--
-- E TEM O OUTRO LADO, que é o que torna a conta-com-empresa útil: o TÍTULO
-- também não sabia de que empresa era. A empresa só aparecia indiretamente,
-- pela obra do rateio. Sem os dois lados, não há o que comparar na hora de
-- pagar -- por isso as duas colunas nascem na mesma migração.
--
-- POR QUE AS DUAS COLUNAS ACEITAM NULO, sendo "obrigatórias":
--
--   `contas_bancarias.empresa_id` fica NULO nas contas que já existem, DE
--   PROPÓSITO. Atribuir sozinho a empresa padrão seria adivinhar em cima de
--   dado bancário -- exatamente onde adivinhar sai caro. A tela exige a
--   empresa em conta nova e marca as antigas que faltam; são poucas, e quem
--   sabe de quem é cada uma é o dono.
--
--   `titulos.empresa_id` é diferente: aqui NÃO é adivinhação. A obra sabe a
--   empresa dela, e o ERP já recusa, desde 09/09/2026, título rateado entre
--   obras de CONTAS diferentes ("como é que eu vou pagar um boleto de duas
--   contas bancárias? É impossível"). Com o dono confirmando em 22/09/2026 que
--   obra é sempre de UMA empresa só, a empresa do título é dedução, não
--   chute -- e por isso o histórico é preenchido aqui embaixo, pela MESMA
--   regra que o código vai usar daqui para a frente.
-- ============================================================================

ALTER TABLE contas_bancarias
    ADD COLUMN IF NOT EXISTS empresa_id BIGINT REFERENCES empresas(id);

COMMENT ON COLUMN contas_bancarias.empresa_id IS
    'A empresa dona da conta. Nulo só nas contas anteriores à migração 080 — '
    'a tela cobra o preenchimento e lista o que falta.';

CREATE INDEX IF NOT EXISTS idx_contas_empresa
    ON contas_bancarias (empresa_id);

ALTER TABLE titulos
    ADD COLUMN IF NOT EXISTS empresa_id BIGINT REFERENCES empresas(id);

COMMENT ON COLUMN titulos.empresa_id IS
    'A empresa que paga este título. Vem da obra do rateio (obra é sempre de '
    'uma empresa só) e pode ser corrigida na tela.';

CREATE INDEX IF NOT EXISTS idx_titulos_empresa
    ON titulos (empresa_id);

-- ---------------------------------------------------------------------------
-- O HISTÓRICO DOS TÍTULOS, preenchido pela obra do rateio.
--
-- A condição é estrita de propósito: só recebe empresa o título cujas obras de
-- rateio apontam TODAS para a MESMA empresa, e nenhuma delas está sem empresa
-- no cadastro. Título que não se encaixa fica nulo e aparece na tela para
-- alguém decidir -- deixar em branco é honesto, preencher com "a primeira que
-- apareceu" seria um número errado com cara de certo.
-- ---------------------------------------------------------------------------
UPDATE titulos t
   SET empresa_id = origem.empresa_id
  FROM (
        SELECT r.titulo_id,
               MIN(o.empresa_id) AS empresa_id
          FROM rateios r
          JOIN obras o ON o.id = r.obra_id
         GROUP BY r.titulo_id
        HAVING COUNT(DISTINCT o.empresa_id) = 1
           AND bool_and(o.empresa_id IS NOT NULL)
       ) AS origem
 WHERE origem.titulo_id = t.id
   AND t.empresa_id IS NULL;
