-- ===========================================================================
-- 003 — QR CODE PESSOAL, FILA DE ENVIOS, MOSAICO DE FOTOS E SINAIS DE FRAUDE
--
-- Pedido do dono em 03/10/2026, depois de decidir COMO a pessoa se identifica
-- no tablet da obra: "deixar só o CPF e QR Code". Sem crachá impresso (crachá
-- se empresta: 20 crachás na mão de uma pessoa só batem o ponto de 20); o QR
-- vai para o WhatsApp da pessoa, ela mostra na tela do PRÓPRIO celular, e ele
-- MUDA de tempos em tempos — sorteado por pessoa, em dias e horas diferentes,
-- porque a API de WhatsApp da casa não é a oficial e disparo em massa bloqueia
-- o número.
--
-- E o mosaico: as fotos do dia de cada obra lado a lado, para um olho humano
-- ver em segundos o rosto que não bate. Opcional por obra; quando OBRIGATÓRIO,
-- tem um responsável que recebe o aviso e precisa confirmar — sem a
-- confirmação, o ponto acusa "falta validação".
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- QR Code pessoal. O banco guarda só o HASH do código: quem lê esta tabela não
-- consegue gerar o QR de ninguém. O código em claro existe só na mensagem que
-- foi para o WhatsApp da pessoa.
--
-- Troca sem susto: o QR antigo continua valendo até o NOVO ser usado pela
-- primeira vez, ou por 3 dias — quem não abriu o WhatsApp não fica sem bater.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ponto.qr_codigos (
    id               BIGSERIAL PRIMARY KEY,
    colaborador_id   BIGINT      NOT NULL REFERENCES public.colaboradores (id) ON DELETE CASCADE,
    token_hash       TEXT        NOT NULL UNIQUE,
    motivo           TEXT        NOT NULL DEFAULT 'ROTACAO'
                                 CHECK (motivo IN ('INICIAL', 'ROTACAO', 'PEDIDO', 'GESTAO')),
    criado_em        TIMESTAMPTZ NOT NULL DEFAULT now(),
    primeiro_uso_em  TIMESTAMPTZ,
    ultimo_uso_em    TIMESTAMPTZ,
    usos             INTEGER     NOT NULL DEFAULT 0,
    substituido_em   TIMESTAMPTZ,
    valido_ate       TIMESTAMPTZ,
    revogado_em      TIMESTAMPTZ,
    revogado_por     TEXT
);
CREATE INDEX IF NOT EXISTS ix_ponto_qr_pessoa ON ponto.qr_codigos (colaborador_id, id);

-- Quando o QR da pessoa troca de novo — sorteado a cada envio (7 a 14 dias).
ALTER TABLE ponto.colaborador_config
    ADD COLUMN IF NOT EXISTS qr_proxima_troca TIMESTAMPTZ;

-- ---------------------------------------------------------------------------
-- A fila de mensagens do ponto por WhatsApp. Tudo o que o ponto manda sem
-- alguém estar esperando a resposta na tela passa por aqui, com ritmo:
-- espaçamento sorteado entre uma e outra, teto por hora e por dia, e janela de
-- horário. `referencia` impede a mesma mensagem duas vezes.
--   QR          o QR Code pessoal (o código é gerado NA HORA do envio)
--   MOSAICO     o aviso ao responsável da obra de que o mosaico do dia espera
--   AVISO_FOTO  o aviso à pessoa de que a batida ficou sem foto
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ponto.envios (
    id              BIGSERIAL PRIMARY KEY,
    tipo            TEXT        NOT NULL CHECK (tipo IN ('QR', 'MOSAICO', 'AVISO_FOTO')),
    motivo          TEXT        NOT NULL DEFAULT '',
    prioridade      SMALLINT    NOT NULL DEFAULT 5,
    referencia      TEXT        NOT NULL UNIQUE,
    colaborador_id  BIGINT      REFERENCES public.colaboradores (id) ON DELETE CASCADE,
    usuario_id      BIGINT,
    telefone        TEXT        NOT NULL,
    texto           TEXT        NOT NULL DEFAULT '',
    status          TEXT        NOT NULL DEFAULT 'PENDENTE'
                                CHECK (status IN ('PENDENTE', 'ENVIANDO', 'ENVIADO', 'FALHOU',
                                                  'CANCELADO')),
    agendado_para   TIMESTAMPTZ NOT NULL DEFAULT now(),
    tentativas      INTEGER     NOT NULL DEFAULT 0,
    resultado       TEXT,
    pedido_por      TEXT        NOT NULL DEFAULT '',
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    pego_em         TIMESTAMPTZ,
    enviado_em      TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_ponto_envios_fila
    ON ponto.envios (prioridade, agendado_para) WHERE status = 'PENDENTE';
CREATE INDEX IF NOT EXISTS ix_ponto_envios_enviados ON ponto.envios (enviado_em)
    WHERE status = 'ENVIADO';

-- ---------------------------------------------------------------------------
-- Como a batida identificou a pessoa. Fica na batida porque é prova: "bateu
-- com o QR que estava no celular dela" é diferente de "alguém digitou o CPF".
--   SESSAO       no celular da própria pessoa (CPF + PIN)
--   CPF          CPF digitado no tablet da obra
--   QR_WHATSAPP  o QR pessoal que foi para o WhatsApp
--   QR_APP       o QR que muda a cada 30 s, no "Meu ponto" do celular dela
--   CHAVE        sistema (iDFace, lançamento manual, importação)
-- ---------------------------------------------------------------------------
ALTER TABLE ponto.marcacoes
    ADD COLUMN IF NOT EXISTS identificacao TEXT;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_ponto_marcacao_identificacao') THEN
        ALTER TABLE ponto.marcacoes ADD CONSTRAINT ck_ponto_marcacao_identificacao
            CHECK (identificacao IS NULL OR identificacao IN
                   ('SESSAO', 'CPF', 'QR_WHATSAPP', 'QR_APP', 'CHAVE'));
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Sinais medidos na foto, NA HORA da batida (sem IA, sem custo):
--   luminancia  brilho médio, 0 a 255 — perto de zero é câmera tampada
--   contraste   desvio do brilho — perto de zero é imagem lisa (parede, dedo)
--   dhash       impressão da imagem (256 bits); duas fotos com impressão quase
--               igual são a MESMA foto mandada de novo
-- ---------------------------------------------------------------------------
ALTER TABLE ponto.fotos
    ADD COLUMN IF NOT EXISTS luminancia SMALLINT,
    ADD COLUMN IF NOT EXISTS contraste SMALLINT,
    ADD COLUMN IF NOT EXISTS dhash TEXT;
CREATE INDEX IF NOT EXISTS ix_ponto_fotos_sha ON ponto.fotos (sha256);

-- ---------------------------------------------------------------------------
-- Mosaico por obra. `mosaico_obrigatorio` liga a obrigação de conferir;
-- `mosaico_responsavel_id` é o usuário do ERP que recebe o aviso.
-- ---------------------------------------------------------------------------
ALTER TABLE ponto.obra_config
    ADD COLUMN IF NOT EXISTS mosaico_obrigatorio BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS mosaico_responsavel_id BIGINT
        REFERENCES public.usuarios (id) ON DELETE SET NULL;

-- A conferência de um dia de uma obra. Nasce PENDENTE na manhã seguinte
-- (obra obrigatória) ou quando alguém confere por conta própria (opcional).
CREATE TABLE IF NOT EXISTS ponto.mosaicos (
    id                    BIGSERIAL PRIMARY KEY,
    obra_id               BIGINT      NOT NULL REFERENCES public.obras (id) ON DELETE CASCADE,
    data                  DATE        NOT NULL,
    situacao              TEXT        NOT NULL DEFAULT 'PENDENTE'
                                      CHECK (situacao IN ('PENDENTE', 'CONFERIDO')),
    obrigatorio           BOOLEAN     NOT NULL DEFAULT FALSE,
    responsavel_id        BIGINT,
    avisado_em            TIMESTAMPTZ,
    batidas               INTEGER     NOT NULL DEFAULT 0,
    suspeitas             INTEGER     NOT NULL DEFAULT 0,
    conferido_por         TEXT,
    conferido_usuario_id  BIGINT,
    conferido_em          TIMESTAMPTZ,
    nota                  TEXT,
    criado_em             TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (obra_id, data)
);
CREATE INDEX IF NOT EXISTS ix_ponto_mosaicos_pendentes
    ON ponto.mosaicos (data) WHERE situacao = 'PENDENTE';

-- ---------------------------------------------------------------------------
-- A CERCA PASSA A BLOQUEAR (decisão do dono, 04/10/2026: "não queremos
-- permitir que a pessoa bata ponto fora das áreas de obra"). Por obra, porque
-- há obra que precisa do contrário (estrada, rede, serviço espalhado):
--   BLOQUEAR  fora da cerca, a batida é recusada e registrada em `recusas`
--   ANALISAR  fora da cerca, a batida entra e vai para conferência (o jeito de
--             antes)
-- ---------------------------------------------------------------------------
ALTER TABLE ponto.obra_config
    ADD COLUMN IF NOT EXISTS fora_da_cerca TEXT NOT NULL DEFAULT 'BLOQUEAR';
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_ponto_obra_fora_da_cerca') THEN
        ALTER TABLE ponto.obra_config ADD CONSTRAINT ck_ponto_obra_fora_da_cerca
            CHECK (fora_da_cerca IN ('BLOQUEAR', 'ANALISAR'));
    END IF;
END $$;
