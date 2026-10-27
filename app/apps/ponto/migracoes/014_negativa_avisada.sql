-- ===========================================================================
-- 014 — A NEGATIVA AVISADA A QUEM PEDIU
--
-- Pedido do dono, 28/10/2026: "quando eu nego uma validação, o ideal era que a
-- pessoa recebesse, quem solicitou recebesse, a negativa (…) com algum motivo.
-- Para a pessoa saber que foi negado, ou então entender que foi pedido errado e
-- precisa ser pedido novamente." E, para o WhatsApp: "o padrão é para enviar,
-- mas (…) tem uma opção de não enviar".
--
-- Até aqui o pedido guardava só o NOME de quem pediu (texto); para avisar quem
-- pediu por outra pessoa (o responsável, o administrativo, quem estava no ponto
-- da obra), o pedido passa a guardar a PESSOA. Vale para os pedidos novos; nos
-- antigos, só o da própria pessoa pelo celular dá para recuperar.
-- ===========================================================================

ALTER TABLE ponto.ocorrencias ADD COLUMN IF NOT EXISTS solicitado_colaborador_id BIGINT
    REFERENCES public.colaboradores (id) ON DELETE SET NULL;

UPDATE ponto.ocorrencias SET solicitado_colaborador_id = colaborador_id
 WHERE origem = 'APP' AND solicitado_colaborador_id IS NULL;

-- Quem negou escolheu avisar pelo WhatsApp? (NULL nos negados antes desta migração)
ALTER TABLE ponto.ocorrencias ADD COLUMN IF NOT EXISTS negativa_whatsapp BOOLEAN;

-- O aviso na tela inicial do aplicativo some quando é tocado: um para a pessoa,
-- outro para quem pediu por ela.
ALTER TABLE ponto.ocorrencias ADD COLUMN IF NOT EXISTS negativa_vista_em TIMESTAMPTZ;
ALTER TABLE ponto.ocorrencias ADD COLUMN IF NOT EXISTS negativa_vista_solicitante_em TIMESTAMPTZ;

-- Os negados de mais de 7 dias antes desta migração já são história: não viram aviso de uma vez
-- na tela de todo mundo. Os da última semana, sim — a pessoa provavelmente ainda não sabe.
UPDATE ponto.ocorrencias SET negativa_vista_em = now(), negativa_vista_solicitante_em = now()
 WHERE status = 'NEGADA' AND atualizado_em < now() - interval '7 days';

-- Os negados que ainda não foram vistos, por pessoa — o que a tela inicial procura.
CREATE INDEX IF NOT EXISTS ix_ponto_ocorrencias_negativa_pessoa
    ON ponto.ocorrencias (colaborador_id) WHERE status = 'NEGADA' AND negativa_vista_em IS NULL;
CREATE INDEX IF NOT EXISTS ix_ponto_ocorrencias_negativa_solicitante
    ON ponto.ocorrencias (solicitado_colaborador_id)
    WHERE status = 'NEGADA' AND negativa_vista_solicitante_em IS NULL;

-- A fila do WhatsApp ganha o tipo da negativa.
ALTER TABLE ponto.envios DROP CONSTRAINT IF EXISTS envios_tipo_check;
ALTER TABLE ponto.envios ADD CONSTRAINT envios_tipo_check
    CHECK (tipo IN ('QR', 'MOSAICO', 'AVISO_FOTO', 'NEGATIVA'));
