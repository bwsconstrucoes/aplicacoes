-- ============================================================================
-- Migração 078 — o nome OFICIAL do fornecedor, para normalizar o que está aqui
--
-- Pedido do dono em 21/09/2026: *"preciso normalizar o nome do fornecedor
-- através de consulta CNPJ"*.
--
-- O QUE FALTAVA, e por que uma coluna resolve:
--
-- A consulta à Receita já existia ("Acertar o cadastro pela Receita") e já
-- comparava o nome. Mas o que ela descobria morria no relatório do trabalho:
-- *"47 têm o nome DIFERENTE do da Receita"*, e quando o aviso saía da tela
-- ninguém sabia mais QUAIS eram. Não havia como filtrar, nem como aplicar.
--
-- Guardar o nome oficial no próprio fornecedor muda isso: a divergência passa
-- a ser um ESTADO do cadastro, não um número num relatório que passou. Dá para
-- filtrar por ela, mostrar os dois nomes lado a lado e trocar com um clique —
-- um a um ou todos de uma vez.
--
-- POR QUE NÃO SOBRESCREVER DIRETO, já que o da Receita é o oficial: porque
-- "MADEIREIRA SÃO JOSÉ" no ERP e "J. G. DA SILVA COMERCIO DE MADEIRAS EIRELI"
-- na Receita são a mesma empresa, e o comprador reconhece a primeira. Trocar
-- calado deixaria a lista de cotação cheia de nomes que ninguém reconhece. O
-- sistema mostra os dois e quem decide é gente — mas agora decide de verdade,
-- porque tem onde clicar.
-- ============================================================================

ALTER TABLE fornecedores
    ADD COLUMN IF NOT EXISTS razao_social_rfb TEXT;

COMMENT ON COLUMN fornecedores.razao_social_rfb IS
    'A razão social como está na Receita, quando DIFERENTE da cadastrada. '
    'Vazio quando bate, quando nunca foi consultado, ou depois de a pessoa '
    'adotar o nome oficial.';

-- A tela filtra por "nome diferente da Receita". Sem índice parcial, isso
-- varre os 1.700 fornecedores a cada abertura.
CREATE INDEX IF NOT EXISTS idx_fornecedor_nome_rfb
    ON fornecedores (id)
    WHERE razao_social_rfb IS NOT NULL;
