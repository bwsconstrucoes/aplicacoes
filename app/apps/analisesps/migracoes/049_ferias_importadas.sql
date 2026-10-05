-- ===========================================================================
-- 049 — FÉRIAS IMPORTADAS DA "LISTAGEM DE FÉRIAS" DO FORTES
--
-- O dono, 05/10/2026: a contabilidade manda a Listagem de Férias do mês (e ele
-- vai pedir a do mês anterior e a do atual); importando, o auxílio alimentação
-- e o transporte descontam as férias proporcionalmente, e a folha da
-- contabilidade fica explicada. Reimportar o mesmo arquivo não duplica, e a
-- mudança de datas de alguém aparece antes de gravar.
--
-- Os períodos continuam na tabela de férias (032) — a mesma que o cálculo do
-- auxílio já lê. Estas colunas dizem de onde o período veio e o que o arquivo
-- trazia. Lançamento à mão fica com `origem` vazia.
--
-- O código não lê nada daqui antes de este arquivo rodar (`tem_coluna`).
-- ===========================================================================
ALTER TABLE analisesps.ferias
    ADD COLUMN IF NOT EXISTS origem TEXT NOT NULL DEFAULT '';
ALTER TABLE analisesps.ferias
    ADD COLUMN IF NOT EXISTS codigo_fortes TEXT NOT NULL DEFAULT '';
ALTER TABLE analisesps.ferias
    ADD COLUMN IF NOT EXISTS aquisitivo TEXT NOT NULL DEFAULT '';
ALTER TABLE analisesps.ferias
    ADD COLUMN IF NOT EXISTS retorno DATE;
ALTER TABLE analisesps.ferias
    ADD COLUMN IF NOT EXISTS abono_dias INTEGER NOT NULL DEFAULT 0;
ALTER TABLE analisesps.ferias
    ADD COLUMN IF NOT EXISTS liquido NUMERIC(12, 2);
-- As verbas do arquivo (remuneração de férias, 1/3, abono, INSS…), em JSON.
ALTER TABLE analisesps.ferias
    ADD COLUMN IF NOT EXISTS eventos TEXT NOT NULL DEFAULT '';
