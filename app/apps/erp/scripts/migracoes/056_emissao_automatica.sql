-- ===========================================================================
-- Migração 056 — o que faltava para a nota SAIR SOZINHA.
--
-- Item 6 de `MEDICOES_E_NOTAS.md`. A emissão automática estava presa a
-- Petrolina; o dono tirou Petrolina da conta em 10/09/2026 e o que sobrou —
-- a BWS no Eusébio — já tem inscrição, token (migração 047), controle de
-- numeração (048) e certificado digital (053). Faltavam duas coisas.
--
-- 1. O ENDEREÇO DO TOMADOR
--
-- A declaração que vai para a prefeitura (a DPS, no padrão nacional) exige o
-- endereço de quem RECEBE o serviço: CEP, logradouro, número, bairro e o
-- código IBGE do município. O cadastro do cliente só tinha município e UF —
-- o suficiente para pagar, não para emitir.
--
-- Ficam no cadastro do FORNECEDOR porque é lá que o cliente mora neste
-- sistema (o mesmo cadastro serve quem vende e quem compra, marcado por
-- `e_cliente`). E servem para mais do que a nota: é o endereço de entrega em
-- Suprimentos, e o que vai no contrato.
--
-- A consulta de CNPJ na Receita, que o ERP já faz ao cadastrar, passa a
-- trazer esses campos preenchidos — ninguém vai digitar endereço à mão.
--
-- 2. O QUE A PREFEITURA DEVOLVE
--
-- Emitir gera DOIS números: o da declaração, que é NOSSO (`numero_dps`), e o
-- da nota, que é dela (`numero_nota`). Além deles vem um identificador de
-- processamento (o idDPS) e a resposta inteira do serviço.
--
-- Guardar a resposta inteira não é preciosismo: quando a prefeitura recusa,
-- o motivo vem ali dentro, em código — e é isso que se manda para o suporte
-- deles. Sem guardar, a informação morre no log e some no próximo reinício.
-- ===========================================================================

-- ---- 1. endereço do tomador -----------------------------------------------
ALTER TABLE fornecedores ADD COLUMN IF NOT EXISTS cep         TEXT;
ALTER TABLE fornecedores ADD COLUMN IF NOT EXISTS logradouro  TEXT;
ALTER TABLE fornecedores ADD COLUMN IF NOT EXISTS numero      TEXT;
ALTER TABLE fornecedores ADD COLUMN IF NOT EXISTS complemento TEXT;
ALTER TABLE fornecedores ADD COLUMN IF NOT EXISTS bairro      TEXT;
-- Código IBGE de 7 dígitos do município. TEXT e não número: zero à esquerda
-- não existe aqui, mas código de município é identificador, não quantidade —
-- e ninguém vai somar dois deles.
ALTER TABLE fornecedores ADD COLUMN IF NOT EXISTS codigo_ibge TEXT;

-- ---- 2. o retorno da prefeitura -------------------------------------------
ALTER TABLE notas_emitidas ADD COLUMN IF NOT EXISTS id_dps  TEXT;
ALTER TABLE notas_emitidas ADD COLUMN IF NOT EXISTS retorno JSONB;

-- Consultar depois pelo identificador de processamento (é o que a prefeitura
-- pede quando a resposta demora ou se perde).
CREATE INDEX IF NOT EXISTS idx_notas_emitidas_id_dps
    ON notas_emitidas (id_dps) WHERE id_dps IS NOT NULL;
