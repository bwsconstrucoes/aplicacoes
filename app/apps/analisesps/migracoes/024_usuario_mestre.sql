-- ===========================================================================
-- 024 — O MESTRE PASSA A SER UMA MARCAÇÃO NO CADASTRO
--
-- Pedido do dono em 25/09/2026, logo depois de ver o cadastro funcionando:
-- *"Elimine do login o login via Nomes na lista da entrada. Vamos ficar
-- somente com os cadastrados. Como ajustar o acesso master?"*
--
-- Até aqui o mestre era quem digitava a senha do Render, e a pessoa escolhia o
-- nome numa lista ao lado da senha. Some a lista, some o caminho: quem entra,
-- entra com usuário e senha próprios. E o mestre vira o que sempre deveria ter
-- sido — uma marcação na pessoa, como "pode alterar".
--
-- QUEM É MESTRE VÊ TUDO: todas as telas, Configurações, o certificado digital,
-- os aportes no OMIE e o cadastro de acesso. As telas marcadas para ele não
-- importam — ele alcança todas, por definição.
--
-- ⚠️ A SENHA DO RENDER CONTINUA EXISTINDO, mas agora só como PORTA DE
-- EMERGÊNCIA: entra-se por ela deixando o campo de usuário em branco. Ela não
-- é mais o caminho do dia a dia, e é isso que o dono pediu. Mas tirá-la por
-- completo criaria um jeito de trancar todo mundo para fora sem volta — se o
-- último cadastro de mestre se perder, não há e-mail de recuperação, não há
-- outro administrador, não há nada. A porta fica, e está escrita na tela.
-- ===========================================================================

ALTER TABLE analisesps.usuarios
    ADD COLUMN IF NOT EXISTS mestre BOOLEAN NOT NULL DEFAULT FALSE;
