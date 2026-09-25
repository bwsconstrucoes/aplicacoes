-- ===========================================================================
-- 026 — A ADOÇÃO DE UMA LINHA DA PLANILHA PASSA A TER VOLTA
--
-- Relato do dono em 25/09/2026:
--
--   "Ainda tá tendo alguma falha na detecção. Está se tentando colocar
--    registro que já estão lançados. (…) BD 50024 · Li 1006 lançamento(s):
--    0 já estavam aqui e 1006 são novos."
--
-- E o mecanismo é este. Quando um OFX reconhece uma linha que já veio da
-- planilha, ele não cria outra: ADOTA a que existe — grava nela a identidade
-- do banco (`impressao`) e o FITID. Isso é certo, e está assim desde 24/09.
--
-- ⚠️ O QUE FALTAVA ERA O CAMINHO DE VOLTA. A adoção apagava a identidade
-- ORIGINAL da linha da planilha, e não deixava anotado QUAL arquivo a adotou.
-- Consequência, em duas partes, e as duas são graves:
--
--   1. O "Desfazer" de um extrato apaga só as linhas de origem 'ofx'. A linha
--      adotada tem origem 'planilha', então ela FICAVA — mas ficava carregando
--      o FITID e a identidade de um arquivo que acabou de ser apagado. Um
--      fantasma: uma linha cuja identidade aponta para nada.
--
--   2. E, com o FITID preenchido, ela deixava de ser adotável para sempre (a
--      regra da adoção exige FITID vazio). Da próxima vez que o mesmo período
--      fosse importado, ela não era reconhecida nem pela identidade (que é de
--      um arquivo que não existe mais) nem pela adoção (bloqueada pelo FITID)
--      — e o extrato entrava DE NOVO, duplicando o lançamento.
--
-- Esta coluna guarda a identidade que a linha tinha ANTES de ser adotada. Com
-- ela, desfazer devolve a linha ao estado de planilha, e a próxima importação
-- volta a reconhecê-la.
--
-- Vazio ('') significa "esta linha nunca foi adotada" — é o estado de quase
-- toda linha, e por isso é o padrão.
-- ===========================================================================
ALTER TABLE analisesps.conciliacao_extrato
    ADD COLUMN IF NOT EXISTS impressao_planilha TEXT NOT NULL DEFAULT '';

-- Para o desfazer achar depressa as linhas adotadas por um arquivo. Sem ele, a
-- volta varreria a conta inteira a cada desfazer.
CREATE INDEX IF NOT EXISTS ix_analisesps_extrato_adotadas
    ON analisesps.conciliacao_extrato (arquivo_id)
 WHERE impressao_planilha <> '';
