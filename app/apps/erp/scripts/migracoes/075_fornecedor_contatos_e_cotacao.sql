-- ============================================================================
-- Migração 075 — o contato com OBSERVAÇÃO, e o fornecedor na cotação automática
--
-- Pedido do dono em 18/09/2026, ao olhar a planilha de 1.867 fornecedores e
-- entender por que há tanto CNPJ repetido:
--
--   "Um comprador cadastrou, aí depois um segundo comprador cadastrou de novo.
--   Mas veja que tem contatos diferentes — tem fornecedores que têm mais de uma
--   pessoa que atende. Um atende entregas num determinado estado, outro
--   entrega outro (…) abre um campo para, na hora de adicionar o contato,
--   botar o nome do vendedor, o e-mail, o telefone e alguma observação a
--   respeito daquele vendedor, para a gente saber do que é que ele trata."
--
-- Ou seja: o CNPJ repetido NÃO era só sujeira. Era um cadastro que não tinha
-- onde guardar o segundo vendedor, e a pessoa resolveu criando a empresa de
-- novo. A tabela de contatos já existia; o que faltava era a OBSERVAÇÃO — o
-- "do que é que ele trata" — e a tela deixar mexer nela.
--
-- Junto vem `cotacao_automatica`, também pedido dele: *"de repente eu tenho um
-- fornecedor pequeno que eu não costumo mandar para cotar, ou foi uma compra
-- única"*. Nasce LIGADO para todo mundo: desligar é decisão consciente de quem
-- conhece o fornecedor, e nascer desligado faria o disparo automático começar
-- vazio sem ninguém entender por quê.
-- ============================================================================

ALTER TABLE fornecedor_contatos
    ADD COLUMN IF NOT EXISTS observacao TEXT;

COMMENT ON COLUMN fornecedor_contatos.observacao IS
    'Do que este vendedor trata: região que atende, linha de produto, quem o '
    'cadastrou. É o que diferencia dois contatos do mesmo fornecedor.';

ALTER TABLE fornecedores
    ADD COLUMN IF NOT EXISTS cotacao_automatica BOOLEAN NOT NULL DEFAULT true;

COMMENT ON COLUMN fornecedores.cotacao_automatica IS
    'Entra na sugestão do disparo automático de cotação. Desligar é para o '
    'fornecedor de compra única, que não se quer cotar toda vez.';

-- Os contatos ESCOLHIDOS no disparo daquela cotação. Sem isto, quem recebe é
-- sempre "todos os contatos marcados como recebe_cotacao" — e um fornecedor
-- com três vendedores, cada um de uma região, receberia três vezes a cotação
-- de uma obra que é da região de um só.
ALTER TABLE cotacao_fornecedores
    ADD COLUMN IF NOT EXISTS contatos_ids BIGINT[] NOT NULL DEFAULT '{}';

COMMENT ON COLUMN cotacao_fornecedores.contatos_ids IS
    'Quem recebe ESTA cotação neste fornecedor. Vazio = todos os contatos '
    'marcados para receber cotação, que é o comportamento de antes.';
