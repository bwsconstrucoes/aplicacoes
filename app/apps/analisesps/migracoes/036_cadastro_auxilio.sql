-- ===========================================================================
-- 036 — O CÓDIGO DA OBRA E A OBSERVAÇÃO DOS AUXÍLIOS
--
-- Correções do dono em 28/09/2026, depois de usar a tela de alimentação e
-- transporte pela primeira vez. Três coisas, e as três vinham da MESMA causa:
-- eu não sabia o nome de cinco colunas do cadastro e chutei.
--
-- 1. *"Em obra tem que colocar o CÓDIGO da obra e não a obra por extenso.
--     Todo mundo tem código da obra. Aqui tem vários vazios, então já está
--     errado aqui também."*
--
--    A carga lia "Objeto Obra [ ]", que é a obra por extenso. O código é outra
--    coluna, e é ele que casa com a conta de pagamento (aba "C. Diários") e com
--    o rateio. Por isso `obra_codigo` entra separado de `obra_cadastro`: os dois
--    existem na planilha e servem para coisas diferentes.
--
-- 2. *"Existe aqui na coluna BM (…) Categoria Auxílio Alimentação. Aí na coluna
--     seguinte tem Valor Auxílio Alimentação. E o transporte, mesma coisa (…)
--     BO, Categoria Auxílio Transporte."*
--
--    ⚠️ ERA "CATEGORIA", NÃO "MODALIDADE" — e é por isso que a tela não
--    calculava NADA: sem a modalidade, toda pessoa caía em "o cadastro não diz a
--    modalidade", com zero dias e zero valor. O nome certo não muda o schema
--    (a coluna `modo_alimentacao` continua a mesma), mas fica registrado aqui
--    porque foi o que custou a tela inteira.
--
-- 3. *"E ainda tem a coluna BQ, que é observação. Isso aqui é interessante a
--     gente poder clicar na pessoa e visualizar."*
-- ===========================================================================

ALTER TABLE analisesps.colaborador
    ADD COLUMN IF NOT EXISTS obra_codigo TEXT NOT NULL DEFAULT '';

ALTER TABLE analisesps.colaborador
    ADD COLUMN IF NOT EXISTS observacao_auxilio TEXT NOT NULL DEFAULT '';

-- A pergunta da tela de auxílio com filtro por obra: "quem é desta obra?".
CREATE INDEX IF NOT EXISTS ix_analisesps_colaborador_obra_codigo
    ON analisesps.colaborador (obra_codigo) WHERE obra_codigo <> '';

-- ---------------------------------------------------------------------------
-- AS DUAS DATAS QUE DECIDEM DIARISTA × CTPS
--
-- *"E cadê os diaristas? Não entrou diaristas."* (dono, 28/09/2026)
--
-- A regra já existia e estava testada (`folha_vinculo.py`, tradução fiel da
-- coluna AH da aba Mobponto): o vínculo é decidido POR DIA, comparando o dia de
-- ponto com a **Data de Início** e a **Data de Admissão**. A MESMA pessoa tem dias
-- de diária e dias de CTPS no mês em que foi registrada.
--
-- O que faltava era o cadastro TRAZER essas duas datas. Sem elas, todo mundo caía
-- em "falta data para decidir" — e a tela de diaristas não tinha o que mostrar.
-- ---------------------------------------------------------------------------
ALTER TABLE analisesps.colaborador
    ADD COLUMN IF NOT EXISTS data_inicio DATE;

ALTER TABLE analisesps.colaborador
    ADD COLUMN IF NOT EXISTS data_admissao DATE;
