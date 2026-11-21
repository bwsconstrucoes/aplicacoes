-- ===========================================================================
-- O AGENTE PASSA A COBRAR E A LER O E-MAIL SOZINHO (21/11/2026)
--
-- Ele, depois de entender o que as duas rotinas fazem: *"sim, vamos deixar as
-- rotinas automáticas."*
--
--   · COBRANÇA (FAZ): pedido à contabilidade vencido recebe e-mail de cobrança
--     pela conta do DP (até o teto do tipo de pedido; passou, escala para o
--     DP); documento que a obra tem de devolver e venceu recebe WhatsApp. Não
--     cobra no fim de semana.
--   · CAIXA (FAZ): lê os e-mails não lidos do DP, liga a resposta da
--     contabilidade ao pedido, arquiva o anexo no cadastro e marca o pedido
--     como respondido. Não apaga, não responde, não conclui; e-mail sem dono
--     vira aviso para o DP.
--
-- Rodam na rodada diária do agente. A caixa precisa da conta de e-mail do DP
-- configurada no ERP; sem ela, avisa e não faz nada.
-- ===========================================================================
UPDATE dp_agente_rotinas SET modo = 'FAZ', alterado_em = now()
 WHERE chave IN ('cobranca', 'caixa');
