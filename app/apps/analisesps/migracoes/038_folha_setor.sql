-- ===========================================================================
-- 038 — O SETOR DE CADA PESSOA NA FOLHA DA CONTABILIDADE
--
-- Pedido do dono em 30/09/2026: "vamos guardar essa informação e expor ela em
-- tela." A Folha Sintética do Fortes separa as pessoas por FILIAL ("001 -
-- CONSTRUTORA") e, dentro dela, por SETOR ("001.01 - CONSTRUTORA/ESCRITORIO",
-- "001.08 - CONSTRUTORA/AFASTADO INSS", "001.09 - CONSTRUTORA/DESATIVAR"). O
-- setor diz coisas que o resto do sistema não sabe — quem está afastado pelo
-- INSS, quem a contabilidade marcou para desativar — e até aqui era jogado fora.
--
-- Vazio ('') quer dizer "a folha foi importada antes desta atualização" ou "o
-- arquivo não tinha setor para esta pessoa". Para preencher uma folha antiga, é
-- importar o mesmo arquivo de novo: as decisões (quem entra, obra ajustada) são
-- guardadas por competência e CPF, e continuam.
-- ===========================================================================
ALTER TABLE analisesps.folha_linha
    ADD COLUMN IF NOT EXISTS setor_codigo TEXT NOT NULL DEFAULT '';
ALTER TABLE analisesps.folha_linha
    ADD COLUMN IF NOT EXISTS setor_nome TEXT NOT NULL DEFAULT '';
