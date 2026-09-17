-- ---------------------------------------------------------------------------
-- DE ONDE VEIO CADA NOTA — 15/09/2026
--
-- Pergunta do dono, no dia em que a busca na Receita funcionou pela primeira
-- vez: *"como e que eu sei que eu estou visualizando essas notas que foram
-- baixadas? (...) Eu so nao sei pra onde e que elas estao indo. E se estao
-- indo pra algum canto que eu nao estou enxergando direito."*
--
-- Ele esta certo, e o buraco e de desenho: a nota chega por DUAS portas — o
-- relatorio do FSist, colado na aba, e a busca automatica na Receita — e as
-- duas gravam na mesma tabela, sem deixar dito qual delas trouxe a linha.
-- Olhando a tela, as 147 notas que a Receita trouxe hoje sao indistinguiveis
-- das que ja estavam ali desde o relatorio. Ou seja: a busca podia estar
-- funcionando perfeitamente e ele continuaria sem ter como saber.
--
-- TRES VALORES POSSIVEIS, e o terceiro e o mais comum com o tempo:
--   'receita'         so a busca automatica trouxe
--   'fsist'           so o relatorio trouxe
--   'receita+fsist'   as duas portas, o que e o esperado para nota antiga
--
-- A nota que ja estava aqui antes desta coluna fica com o valor vazio ('') ate
-- ser tocada de novo por uma das duas portas. Vazio quer dizer "nao sei", e
-- dizer "nao sei" e honesto — chutar 'fsist' para o passado seria inventar.
-- ---------------------------------------------------------------------------
ALTER TABLE analisesps.notas_fiscais
    ADD COLUMN IF NOT EXISTS origem TEXT NOT NULL DEFAULT '';

-- O caminho da pergunta que ele faz: "o que a Receita trouxe, do mais novo
-- para o mais velho". Sem o indice, e uma varredura da tabela inteira a cada
-- abertura da tela.
CREATE INDEX IF NOT EXISTS ix_notas_origem_entrada
    ON analisesps.notas_fiscais (origem, importada_em DESC);
