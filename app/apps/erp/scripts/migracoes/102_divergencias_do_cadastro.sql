-- ---------------------------------------------------------------------------
-- 102 — O DADO QUE NÃO BATE VIRA PERGUNTA, E ALGUÉM RESPONDE
--
-- Ele, em 13/11/2026: *"em relação a IA errar a leitura, eu acho que pode
-- fazer o seguinte: quando houver divergência entre o que a IA lê e o que está
-- cadastrado, aí você questiona. E o humano decide."*
--
-- Antes disto, a importação já comparava as três fontes (o cadastro do ERP, o
-- card do Pipefy e o documento lido pela IA) e MOSTRAVA a divergência no
-- relatório. Só que o relatório é de uma rodada: fechou a tela, perdeu-se. Com
-- 575 pessoas, "anote num papel o que não bateu" não é um plano.
--
-- ⚑ A DIVERGÊNCIA É TRABALHO, ENTÃO ELA PRECISA DE FILA. Guardada, ela vira
-- uma lista do que falta decidir — e, decidida, vira histórico de por que o
-- cadastro está do jeito que está.
--
-- POR QUE NÃO DEIXAR A IA GRAVAR SOZINHA: quem lê o documento erra em
-- silêncio. Trocar o nome de alguém na folha por causa de uma leitura torta é
-- estrago que só aparece quando o pagamento volta. O documento tem mais força
-- — por isso vem sugerido —, mas quem decide é gente.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cadastro_divergencias (
    id              BIGSERIAL PRIMARY KEY,
    colaborador_id  BIGINT NOT NULL REFERENCES colaboradores(id) ON DELETE CASCADE,
    campo           TEXT   NOT NULL,
    no_cadastro     TEXT,
    no_card         TEXT,
    no_documento    TEXT,
    mais_forte      TEXT   NOT NULL DEFAULT 'documento',
    origem          TEXT   NOT NULL DEFAULT 'PIPEFY',
    referencia      TEXT,
    situacao        TEXT   NOT NULL DEFAULT 'ABERTA',
    escolhido       TEXT,
    decidido_por    BIGINT REFERENCES usuarios(id),
    decidido_em     TIMESTAMPTZ,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ⚑ UMA PERGUNTA ABERTA POR PESSOA E CAMPO. Reimportar a mesma gente é
-- normal (e ele vai fazer isso na carga inicial): sem esta trava, a terceira
-- rodada apresentaria a mesma dúvida três vezes, e a lista do que falta
-- decidir cresceria sem ninguém ter decidido nada.
--
-- Parcial, só sobre as ABERTAS: a pergunta já respondida fica no histórico, e
-- uma divergência nova no mesmo campo (o card mudou) pode ser perguntada de
-- novo.
CREATE UNIQUE INDEX IF NOT EXISTS idx_divergencia_aberta_unica
    ON cadastro_divergencias (colaborador_id, campo)
 WHERE situacao = 'ABERTA';

CREATE INDEX IF NOT EXISTS idx_divergencia_pessoa
    ON cadastro_divergencias (colaborador_id);

ALTER TABLE cadastro_divergencias DROP CONSTRAINT IF EXISTS ck_divergencia_situacao;
ALTER TABLE cadastro_divergencias ADD CONSTRAINT ck_divergencia_situacao
    CHECK (situacao IN ('ABERTA', 'RESOLVIDA', 'IGNORADA'));

ALTER TABLE cadastro_divergencias DROP CONSTRAINT IF EXISTS ck_divergencia_forte;
ALTER TABLE cadastro_divergencias ADD CONSTRAINT ck_divergencia_forte
    CHECK (mais_forte IN ('cadastro', 'card', 'documento'));

COMMENT ON TABLE cadastro_divergencias IS
    'O que não bate entre o cadastro do ERP, o card do Pipefy e o documento '
    'lido — para uma pessoa decidir (13/11/2026).';
