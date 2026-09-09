-- ===========================================================================
-- 007 — O código da categoria chega na tabela que as telas leem
--
-- A `categoria` do fato é a DESCRIÇÃO ("Serviços de Terceiros"). Para alterar a
-- categoria de um título no OMIE é preciso o CÓDIGO dela, que já existe no
-- espelho (`titulos.codigo_categoria`) mas parava ali.
--
-- Sem esta coluna, a tela de saneamento teria de voltar ao espelho a cada
-- linha mostrada — uma consulta por linha, na tela que existe justamente para
-- varrer milhares delas.
--
-- Nasce VAZIA e é preenchida pela reconstrução do fato, que a marca abaixo
-- dispara sozinha ao fim da aplicação.
-- REFAZER-O-FATO
-- ===========================================================================
ALTER TABLE painel.fato ADD COLUMN IF NOT EXISTS codigo_categoria TEXT;

-- o saneamento procura por código de categoria e por título sem apropriação
CREATE INDEX IF NOT EXISTS ix_fato_cod_categoria ON painel.fato(codigo_categoria);
