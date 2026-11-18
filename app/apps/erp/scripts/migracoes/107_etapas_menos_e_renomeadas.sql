-- ===========================================================================
-- MENOS ETAPAS: "Conferência" morre, fundida na "Inicial"
--
-- Ele, em 17/11/2026, olhando a tela:
--
--   "Era por isso que eu queria menos fase, porque fica fase demais. Ó,
--    inicial e conferência: não precisa ter duas, isso aqui é uma só.
--    Conferência inicial é para ser a mesma fase."
--
-- Tinha razão: as duas queriam dizer "o DP ainda está mexendo neste
-- cadastro", e a fronteira entre elas era invenção minha — ninguém sabia
-- dizer quando uma virava a outra, e o Kanban ficaria com duas colunas que
-- significam o mesmo.
--
-- A lista passou a ser, na ordem das colunas do quadro:
--   Inicial · Assinar · Concluído · Solicitações · Afastado · Desligado ·
--   Cancelado
--
-- ⚠️ OS CÓDIGOS NÃO MUDAM, só os rótulos. "NA_CONTABILIDADE" continua sendo o
-- código do que a tela chama de "Solicitações", e "CONTRATO_A_ASSINAR" do que
-- ela chama de "Assinar". Renomear código custaria migração e histórico para
-- ganhar zero: ninguém vê o código, só o rótulo.
--
-- O que esta migração faz é UMA coisa: tirar de circulação o
-- "EM_CONFERENCIA", passando quem está nele para "CADASTRO_INICIAL".
-- ===========================================================================

UPDATE colaboradores
   SET etapa = 'CADASTRO_INICIAL'
 WHERE UPPER(COALESCE(etapa, '')) = 'EM_CONFERENCIA';

-- O HISTÓRICO NÃO É REESCRITO, e isso é de propósito: ele conta o que
-- aconteceu de verdade, e naquele dia a etapa se chamava "Conferência".
-- Reescrevê-lo seria apagar o passado para ele combinar com o presente. O
-- mapa de rótulos (`ETAPA_ROTULOS`) continua traduzindo o código antigo, para
-- a trilha não aparecer com o nome cru.
