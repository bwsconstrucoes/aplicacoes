-- ============================================================================
-- Migração 079 — A REGIÃO QUE O FORNECEDOR ATENDE, de um jeito que dá para
-- cruzar com o município da obra
--
-- PEDIDO DO DONO, 21/09/2026, e ele achou um buraco de verdade:
--
--   "Na cotação automática, eu acho que você não se atentou à importância
--   disso. Os fornecedores, a gente não pode colocar para disparar uma cotação
--   com qualquer fornecedor, tem que ter uma lógica. O fornecedor tem a região
--   que atende, e existe o local da obra. Se não é um fornecedor que atenda a
--   nível nacional, eu tenho que buscar na região da obra. Da forma que está,
--   a gente simplesmente escreve de qualquer jeito, sem padronização. Como
--   vamos cruzar obra x fornecedor?"
--
-- ELE ESTÁ CERTO, E O DADO PROVA. Na planilha de 1.772 fornecedores, a coluna
-- "Região de Atuação" tem 27 grafias diferentes, misturando quatro níveis que
-- não se comparam entre si:
--
--     país          BR (395 vezes)
--     macrorregião  NE (256)
--     estado        PE (263), CE (142), SP (87), PB, MT
--     metropolitana RMF (204)
--     microrregião  CARIRI (17)
--     cidade        SÃO PAULO (396), TAUÁ CE, GRANJA CE, BARBALHA - CE,
--                   JUAZEIRO DO NORTE - CE, SOBRAL, TEJUCUOCA, ITAITINGA…
--
-- Com isso NÃO DÁ para responder "quem atende a obra de Barbalha?": "CE" e
-- "BARBALHA - CE" e "CARIRI" são todos verdade e nenhum se compara com o
-- outro por igualdade de texto. Era por isso que o disparo automático não
-- filtrava por região nenhuma — ele só PONTUAVA quem era da mesma cidade, e
-- deixava entrar fornecedor de São Paulo numa obra do Cariri.
--
-- O QUE ESTA MIGRAÇÃO CRIA
--
--   `abrangencia`            até onde ele vende: NACIONAL, ESTADUAL, REGIONAL,
--                            LOCAL — ou NAO_INFORMADA, que é o estado de quem
--                            ainda não foi padronizado;
--   `ufs_atendidas`          as siglas, para ESTADUAL;
--   `municipios_atendidos`   os municípios, para REGIONAL e LOCAL, já sem
--                            acento e em maiúsculas, que é como o cruzamento
--                            compara.
--
-- O texto original NÃO é jogado fora: `regioes_atuacao` continua com o que a
-- pessoa escreveu. É o que permite conferir uma conversão suspeita depois, e
-- refazer o de-para sem ter perdido a informação de origem.
--
-- A CONVERSÃO NÃO ESTÁ AQUI, e é de propósito: ela vive em
-- `core/suprimentos/regioes.py`, roda pelo botão "Padronizar as regiões" e
-- pelo importador, e relata o que NÃO reconheceu. Uma tabela de de-para dentro
-- de um .sql seria intocável depois da migração ter rodado — e ela vai precisar
-- de ajuste, porque município novo aparece.
-- ============================================================================

ALTER TABLE fornecedores
    ADD COLUMN IF NOT EXISTS abrangencia TEXT NOT NULL DEFAULT 'NAO_INFORMADA',
    ADD COLUMN IF NOT EXISTS ufs_atendidas TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS municipios_atendidos TEXT[] NOT NULL DEFAULT '{}';

COMMENT ON COLUMN fornecedores.abrangencia IS
    'Até onde o fornecedor vende: NACIONAL, ESTADUAL, REGIONAL, LOCAL ou '
    'NAO_INFORMADA (ainda não padronizado a partir do texto livre).';
COMMENT ON COLUMN fornecedores.municipios_atendidos IS
    'Municípios sem acento e em maiúsculas — é assim que o cruzamento com a '
    'obra compara.';

ALTER TABLE fornecedores
    DROP CONSTRAINT IF EXISTS ck_fornecedor_abrangencia;
ALTER TABLE fornecedores
    ADD CONSTRAINT ck_fornecedor_abrangencia CHECK (
        abrangencia IN ('NACIONAL', 'ESTADUAL', 'REGIONAL', 'LOCAL',
                        'NAO_INFORMADA'));

-- A coerência entre o nível e a lista: ESTADUAL sem UF nenhuma, ou REGIONAL
-- sem município nenhum, é um cadastro que diz atender e não atende lugar
-- algum — e o disparo automático simplesmente nunca o escolheria, calado.
ALTER TABLE fornecedores
    DROP CONSTRAINT IF EXISTS ck_fornecedor_abrangencia_coerente;
ALTER TABLE fornecedores
    ADD CONSTRAINT ck_fornecedor_abrangencia_coerente CHECK (
        abrangencia <> 'ESTADUAL' OR cardinality(ufs_atendidas) > 0);
ALTER TABLE fornecedores
    DROP CONSTRAINT IF EXISTS ck_fornecedor_municipios_coerente;
ALTER TABLE fornecedores
    ADD CONSTRAINT ck_fornecedor_municipios_coerente CHECK (
        abrangencia NOT IN ('REGIONAL', 'LOCAL')
        OR cardinality(municipios_atendidos) > 0);

-- O disparo automático pergunta "quem atende esta UF?" e "quem atende este
-- município?" a cada bloco. Sem índice, isso varre os 1.700 fornecedores por
-- bloco sugerido.
CREATE INDEX IF NOT EXISTS idx_fornecedor_ufs
    ON fornecedores USING GIN (ufs_atendidas);
CREATE INDEX IF NOT EXISTS idx_fornecedor_municipios
    ON fornecedores USING GIN (municipios_atendidos);
