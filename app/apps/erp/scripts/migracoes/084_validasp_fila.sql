-- ============================================================================
-- Migração 084 — a FILA DO VALIDASP: validação de SP que não se perde
--
-- Pedido do dono em 17/10/2026: "se a pessoa validou, ela vai receber a
-- mensagem de validação, e a gente não pode permitir que essa validação se
-- perca". Até aqui o "Sim" na coluna AH (Validação) da SPsBD era gravado por
-- uma tarefa solta na memória do serviço: duas tentativas de 90 s e, falhando,
-- uma linha na aba FalhaProcessar — que é do MESMO Google que acabou de falhar.
-- Uma publicação ou um reinício no meio dos 3 minutos apagava a tarefa sem
-- rastro, e a tela já tinha dito "base em atualização".
--
-- Agora cada validação vira uma linha aqui ANTES da resposta. O vigia do
-- validasp (app/apps/validasp/fila.py) insiste na planilha até o "Sim" entrar
-- e ser CONFERIDO por leitura — e só então marca a linha como feita.
--
-- O código chega ao Render antes de alguém apertar o botão: sem esta tabela,
-- a validação fica guardada num arquivo do próprio serviço e entra aqui sozinha
-- quando a tabela existir.
--
-- situacao: pendente → feito | desistiu (7 dias sem achar a SP na SPsBD)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS validasp;

CREATE TABLE IF NOT EXISTS validasp.pendencias (
    id                 SERIAL PRIMARY KEY,
    sp_id              VARCHAR(40)   NOT NULL,
    situacao           VARCHAR(12)   NOT NULL DEFAULT 'pendente',
    tentativas         INTEGER       NOT NULL DEFAULT 0,
    proxima_tentativa  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    ultimo_erro        VARCHAR(1000) NOT NULL DEFAULT '',
    linha_planilha     INTEGER,
    criado_em          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    feito_em           TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_validasp_pendencias_fila
    ON validasp.pendencias (situacao, proxima_tentativa);
CREATE INDEX IF NOT EXISTS ix_validasp_pendencias_sp
    ON validasp.pendencias (sp_id);
