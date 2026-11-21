-- ===========================================================================
-- O DOCUMENTO QUE O COLABORADOR ASSINA, O CONTROLE DAS FÉRIAS E O AGENTE LIGADO
-- (20/11/2026)
--
-- 1. dp_termos — o documento que a obra apresenta e o colaborador assina, no
--    celular, como a folha de ponto (PIN da própria pessoa + o traço do dedo +
--    o lacre do texto). Nasce para o AVISO PRÉVIO. Ele: *"eu acho que a gente
--    tem que ver uma forma de o administrativo de obra já coletar o aviso
--    prévio das pessoas, formalizar e coletar a assinatura de forma legal,
--    parecido com a forma que a gente está coletando a assinatura do ponto."*
--    Serve depois para a autorização de desconto, a advertência e o termo de
--    prorrogação da experiência — qualquer papel que a pessoa tenha de assinar.
--
-- 2. ferias_controle — até quando as férias de cada pessoa estão quitadas. Sem
--    isto não se sabe quem está com o período vencendo (férias vencidas são
--    pagas em dobro — CLT art. 137). Quem informa é o DP (ou a contabilidade);
--    as férias confirmadas no sistema avançam o controle sozinhas.
--
-- 3. ferias_programadas.periodo_aquisitivo_inicio — a que período cada férias
--    pertence.
--
-- 4. O agente: ele disse *"Vamos botar para funcionar"* sobre as sugestões.
--    Liga, em modo SUGERE, as rotinas que NÃO falam com ninguém de fora
--    (triagem, despacho, vigia, relatório). A cobrança (manda e-mail à
--    contabilidade e WhatsApp à obra) e a leitura da caixa continuam como
--    estão: ligar é um clique na tela do agente, e é decisão dele.
-- ===========================================================================
CREATE SEQUENCE IF NOT EXISTS dp_termo_numero_seq;

CREATE TABLE IF NOT EXISTS dp_termos (
    id                  BIGSERIAL PRIMARY KEY,
    numero              TEXT NOT NULL UNIQUE,
    solicitacao_id      BIGINT REFERENCES dp_solicitacoes(id),
    colaborador_id      BIGINT NOT NULL REFERENCES colaboradores(id),
    obra_id             BIGINT REFERENCES obras(id),
    -- O tipo do documento no Arquivo (AVISO-PREVIO, PEDIDO-DEMISSAO…).
    tipo                TEXT NOT NULL,
    titulo              TEXT NOT NULL,
    texto               TEXT NOT NULL,
    -- SHA-256 do texto como foi mostrado. Texto mudou, lacre muda.
    lacre               TEXT NOT NULL,
    situacao            TEXT NOT NULL DEFAULT 'ABERTO',
    prazo               DATE,
    -- Como foi assinado: NA_TELA (PIN + traço), PAPEL (foto do papel
    -- assinado, o DP valida) ou RECUSA (a pessoa se recusou; duas testemunhas).
    forma               TEXT,
    assinatura_png      BYTEA,
    documento_id        BIGINT REFERENCES documentos(id),
    testemunhas         JSONB,
    motivo_recusa       TEXT,
    conduzido_por_colaborador BIGINT REFERENCES colaboradores(id),
    conduzido_por_nome  TEXT,
    latitude            NUMERIC(10, 7),
    longitude           NUMERIC(10, 7),
    ip                  TEXT,
    assinado_em         TIMESTAMPTZ,
    validado_por        BIGINT REFERENCES usuarios(id),
    validado_em         TIMESTAMPTZ,
    criado_por          BIGINT REFERENCES usuarios(id),
    criado_em           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT dp_termo_situacao CHECK (situacao IN (
        'ABERTO', 'ASSINADO', 'RECUSADO', 'CANCELADO')),
    CONSTRAINT dp_termo_forma CHECK (forma IS NULL OR forma IN (
        'NA_TELA', 'PAPEL', 'RECUSA'))
);
CREATE INDEX IF NOT EXISTS ix_dp_termos_abertos
    ON dp_termos (obra_id) WHERE situacao = 'ABERTO';
CREATE INDEX IF NOT EXISTS ix_dp_termos_solicitacao ON dp_termos (solicitacao_id);

INSERT INTO documento_tipos (codigo, nome, grupo, dono, sigilo) VALUES
    ('AVISO-PREVIO', 'Aviso prévio assinado', 'PESSOA', 'PESSOA', 'PESSOAL'),
    ('PEDIDO-DEMISSAO', 'Pedido de demissão assinado', 'PESSOA', 'PESSOA', 'PESSOAL'),
    ('AUTORIZACAO-DESCONTO', 'Autorização de desconto em folha', 'PESSOA', 'PESSOA', 'PESSOAL')
ON CONFLICT (codigo) DO NOTHING;

CREATE TABLE IF NOT EXISTS ferias_controle (
    colaborador_id      BIGINT PRIMARY KEY REFERENCES colaboradores(id),
    -- O último dia do último período aquisitivo cujas férias já foram
    -- concedidas. NULL = nenhum período quitado desde a admissão.
    quitado_ate         DATE,
    origem              TEXT NOT NULL DEFAULT 'DP',
    informado_por       BIGINT REFERENCES usuarios(id),
    informado_em        TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE ferias_programadas
    ADD COLUMN IF NOT EXISTS periodo_aquisitivo_inicio DATE;

UPDATE dp_agente_rotinas SET modo = 'SUGERE', alterado_em = now()
 WHERE chave IN ('triagem', 'despacho', 'vigia', 'relatorio')
   AND modo = 'DESLIGADA';
