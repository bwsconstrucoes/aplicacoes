-- ===========================================================================
-- Migração 057 — a agenda passa a avisar do BLOCO INCOMPLETO.
--
-- O que já existia avisava sobre documento que VAI VENCER. Faltava o outro
-- lado, que é o que faz perder licitação de verdade: o documento que NUNCA
-- FOI ARQUIVADO.
--
-- A diferença importa. Certidão vencida pelo menos existe, e o sistema sabe
-- de quando é. O documento que nunca entrou é silêncio: ninguém repara na
-- ausência dele até o dia em que o cliente pede a documentação da medição e
-- a pasta sai pela metade — e aí a medição atrasa, ou o pagamento atrasa.
--
-- A conferência do bloco já sabia responder isso desde a migração 046. O que
-- faltava era alguém PERGUNTAR sem esperar que uma pessoa clicasse. Agora
-- quem pergunta é o recálculo da agenda, que desde a migração 055 roda em
-- segundo plano — e por isso pode se dar ao luxo de percorrer obra por obra.
-- ===========================================================================

ALTER TABLE agenda_eventos DROP CONSTRAINT IF EXISTS ck_agenda_origem;
ALTER TABLE agenda_eventos ADD CONSTRAINT ck_agenda_origem
    CHECK (origem IN ('REAJUSTE', 'CERTIDAO', 'LOCACAO', 'CONTRATO',
                      'CERTIFICADO', 'DOCUMENTO', 'MANUAL'));
