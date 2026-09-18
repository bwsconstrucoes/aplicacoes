-- ===========================================================================
-- 071 — a origem BOLETIM na tabela de índices
--
-- O dono passou a colar a tabela do boletim inteira (18/09/2026), em vez de
-- digitar mês a mês. Essa origem é diferente das duas que existiam:
--
--   BCB-SGS  o Banco Central republicou;
--   MANUAL   alguém digitou UM mês, à mão, na tela;
--   BOLETIM  veio da tabela oficial colada, com o número-índice junto.
--
-- Guardar as três separadas não é preciosismo: quando um número for
-- contestado, a primeira pergunta é "de onde veio", e "MANUAL" para uma
-- tabela conferida contra o papel apagaria justamente a origem mais confiável
-- das três.
-- ===========================================================================
ALTER TABLE indices_economicos DROP CONSTRAINT IF EXISTS ck_indice_fonte;
ALTER TABLE indices_economicos ADD CONSTRAINT ck_indice_fonte
    CHECK (fonte IN ('BCB-SGS', 'MANUAL', 'BOLETIM'));
