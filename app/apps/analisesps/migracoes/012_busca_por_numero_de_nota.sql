-- ---------------------------------------------------------------------------
-- FILTRAR AS NOTAS PELA QUALIDADE DO PAR — pedido do dono em 13/09/2026:
-- "deveria poder tambem filtrar pela nota de associacao: 1-4, 2-4, ou pelo
-- percentual tambem".
--
-- O recorte pergunta, para CADA nota, se existe SP do mesmo CNPJ cujo "N. NF"
-- escrito no card seja o numero daquela nota.
--
-- O QUE O BANCO ESTAVA FAZENDO, medido com EXPLAIN ANALYZE (59.000 SPs e
-- 4.000 notas): o planejador transforma o EXISTS num Hash Semi Join sobre a
-- tabela inteira e aplica a comparacao como filtro — **262.497 pares**, e em
-- cada um deles um `regexp_replace` sobre o campo `nf`. Deu **1,6 s** nesta
-- maquina; no banco do Render, que tem UM DECIMO de um nucleo, isso e a mesma
-- historia da tela que nao abria.
--
-- A CORRECAO E GUARDAR O NUMERO JA NORMALIZADO, em vez de recalcula-lo. A
-- coluna e GERADA pelo proprio banco: nao ha codigo que possa esquecer de
-- preenche-la, e ela nunca discorda do campo `nf` — que e o defeito classico
-- de guardar a mesma coisa duas vezes.
--
-- `ltrim(..., '0')` faz parte da regra porque a planilha traz "0001430" e
-- "1430" para o mesmo numero, e e assim que o codigo compara.
-- ---------------------------------------------------------------------------
ALTER TABLE analisesps.sps
    ADD COLUMN IF NOT EXISTS nf_num TEXT
    GENERATED ALWAYS AS (
        ltrim(regexp_replace(coalesce(nf, ''), '\D', '', 'g'), '0')
    ) STORED;

-- COMPOSTO (raiz do CNPJ + numero) de proposito: o recorte filtra pelos dois
-- juntos, e assim o banco vai direto ao punhado de linhas certas.
CREATE INDEX IF NOT EXISTS ix_sps_raiz_numero_nf
    ON analisesps.sps (
        left(regexp_replace(coalesce(documento, ''), '\D', '', 'g'), 8),
        nf_num
    );

-- A data tambem entra na conta do par (vencimento contra a emissao da nota).
CREATE INDEX IF NOT EXISTS ix_sps_raiz_vencimento
    ON analisesps.sps (
        left(regexp_replace(coalesce(documento, ''), '\D', '', 'g'), 8),
        vencimento_d
    );
