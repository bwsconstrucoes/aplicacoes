-- ===========================================================================
-- 051 — ACESSO PRESO A UMA CONTA BANCÁRIA
--
-- O dono, 06/10/2026: *"quero poder criar um usuario e definir às informacoes
-- que ele tem acesso por conta bancaria. No caso quero criar um usuario que vai
-- poder acessar somente Solicitações de uma conta especifica"*.
--
-- Uma linha por conta liberada para a pessoa — o mesmo desenho das telas
-- (`usuario_telas`, migração 023). A conta é guardada como aparece na coluna
-- "Conta" das SPs.
--
-- ⚠️ AQUI, LISTA VAZIA QUER DIZER TODAS AS CONTAS — o contrário das telas, e de
-- propósito: quem já tem cadastro hoje vê todas, e esta migração não pode
-- tirar nada de ninguém. Prender a uma conta é uma marcação consciente.
--
-- E quem fica preso a uma conta alcança SÓ as telas que respeitam esse recorte
-- (hoje, Solicitações). As outras somam todas as contas juntas, e abrir uma
-- delas vazaria o que o recorte esconde — o código fecha isso
-- (`auth.TELAS_COM_RECORTE_DE_CONTA`).
--
-- O código não lê nada daqui antes de este arquivo rodar (`tem_coluna`).
-- ===========================================================================
CREATE TABLE IF NOT EXISTS analisesps.usuario_contas (
    usuario_id BIGINT NOT NULL REFERENCES analisesps.usuarios(id) ON DELETE CASCADE,
    conta      TEXT   NOT NULL,
    PRIMARY KEY (usuario_id, conta)
);
