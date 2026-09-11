-- ===========================================================================
-- Migração 051 — a AGENDA do ERP: o calendário de obrigações.
--
-- POR QUE ELA EXISTE, E POR QUE AGORA
--
-- Quatro coisas construídas antes dela ficaram esperando um lugar para avisar:
--
--   1. o aniversário do reajuste da obra (migração 050);
--   2. a conferência mensal dos equipamentos locados (039);
--   3. o vencimento das certidões (045, o tipo já tem `vence` e `avisar_dias`);
--   4. o fim da vigência do contrato.
--
-- Cada uma sabia calcular a própria data. Nenhuma tinha onde AVISAR — e um
-- alerta que mora dentro da tela que a pessoa só abre quando lembra do assunto
-- não é alerta, é enfeite. O que faltava era um lugar único que a pessoa abre
-- de manhã.
--
-- A DECISÃO CENTRAL: O EVENTO GERADO É RECALCULADO, NÃO ACUMULADO
--
-- Os eventos que o sistema deduz têm uma CHAVE estável (por exemplo
-- "REAJUSTE:contrato=12:2026-01"). A sincronização insere o que falta e APAGA
-- o que deixou de valer — certidão que foi renovada, contrato que encerrou.
-- Sem isso a agenda viraria um depósito de avisos velhos, e uma agenda que
-- ninguém confia é pior que agenda nenhuma: a pessoa para de abrir.
--
-- O QUE NUNCA SE APAGA: evento RESOLVIDO ou DISPENSADO, e evento MANUAL. O
-- primeiro é histórico ("isso foi tratado, por fulano, no dia tal"); o segundo
-- ninguém deduziu, então ninguém pode deduzir que sumiu.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS agenda_eventos (
    id            BIGSERIAL PRIMARY KEY,
    -- A identidade do evento. É ela que faz a sincronização ser idempotente:
    -- rodar dez vezes no mesmo dia não cria dez avisos iguais.
    chave         TEXT NOT NULL UNIQUE,
    origem        TEXT NOT NULL,
    titulo        TEXT NOT NULL,
    detalhe       TEXT,
    -- QUANDO a coisa acontece, e A PARTIR DE QUANDO ela aparece na tela. São
    -- datas diferentes de propósito: certidão que vence em 90 dias não pode
    -- ocupar a agenda de hoje, e reajuste avisado no próprio dia já é tarde.
    quando        DATE NOT NULL,
    avisar_em     DATE NOT NULL,
    obra_id       BIGINT REFERENCES obras(id),
    empresa_id    BIGINT REFERENCES empresas(id),
    -- Para onde a tela manda a pessoa resolver. Aviso sem caminho vira
    -- pergunta ("e agora, onde eu faço isso?").
    link          TEXT,
    situacao      TEXT NOT NULL DEFAULT 'ABERTO',
    resolvido_por BIGINT REFERENCES usuarios(id),
    resolvido_em  TIMESTAMPTZ,
    observacao    TEXT,
    criado_por    BIGINT REFERENCES usuarios(id),
    criado_em     TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE agenda_eventos DROP CONSTRAINT IF EXISTS ck_agenda_situacao;
ALTER TABLE agenda_eventos ADD CONSTRAINT ck_agenda_situacao
    CHECK (situacao IN ('ABERTO', 'RESOLVIDO', 'DISPENSADO'));

ALTER TABLE agenda_eventos DROP CONSTRAINT IF EXISTS ck_agenda_origem;
ALTER TABLE agenda_eventos ADD CONSTRAINT ck_agenda_origem
    CHECK (origem IN ('REAJUSTE', 'CERTIDAO', 'LOCACAO', 'CONTRATO', 'MANUAL'));

-- Resolvido tem de dizer QUEM e QUANDO. "Alguém resolveu" não responde nada
-- três meses depois, que é quando a pergunta aparece.
ALTER TABLE agenda_eventos DROP CONSTRAINT IF EXISTS ck_agenda_resolvido_tem_dono;
ALTER TABLE agenda_eventos ADD CONSTRAINT ck_agenda_resolvido_tem_dono
    CHECK (situacao = 'ABERTO' OR resolvido_em IS NOT NULL);

CREATE INDEX IF NOT EXISTS idx_agenda_aberto
    ON agenda_eventos (avisar_em) WHERE situacao = 'ABERTO';
CREATE INDEX IF NOT EXISTS idx_agenda_obra ON agenda_eventos (obra_id);
CREATE INDEX IF NOT EXISTS idx_agenda_origem ON agenda_eventos (origem, quando);
