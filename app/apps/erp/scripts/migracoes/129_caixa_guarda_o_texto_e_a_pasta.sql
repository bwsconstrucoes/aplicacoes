-- 129 — A caixa de entrada guarda o TEXTO da resposta e para onde a mensagem foi (26/11/2026).
--
-- O dono: "a gente está aqui na parte de pedidos. A gente clica (…) não vê o
-- que é que a contabilidade respondeu, não vê o que é que ela anexou." E sobre
-- a leitura: o que o agente identifica vai para a pasta "Agente" (lido); o que
-- ele não identifica fica na caixa de entrada, NÃO LIDO, para o DP.
--
-- Só acrescenta colunas; nada é apagado nem reescrito.
ALTER TABLE email_entrada_mensagens ADD COLUMN IF NOT EXISTS corpo TEXT;
ALTER TABLE email_entrada_mensagens ADD COLUMN IF NOT EXISTS movida_para TEXT;
