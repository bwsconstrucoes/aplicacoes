-- ===========================================================================
-- Migração 053 — o CERTIFICADO DIGITAL (A1) de cada empresa.
--
-- POR QUE ELE PRECISA EXISTIR AQUI
--
-- Duas coisas dependem dele, e as duas já estão construídas esperando:
--
--   1. a EMISSÃO AUTOMÁTICA da nota de serviço — no padrão nacional a DPS vai
--      ASSINADA, e sem certificado não há assinatura;
--   2. a AGENDA — foi o quarto aviso que ficou de fora quando ela nasceu,
--      justamente porque o certificado não tinha onde morar.
--
-- Hoje o arquivo .pfx vive no computador de alguém, com a senha num papel. Aí
-- ele vence num sábado, ninguém sabe, e a obra para de faturar na segunda.
--
-- COMO O ARQUIVO É GUARDADO, E POR QUE ASSIM
--
-- O .pfx e a senha vão CIFRADOS, com a mesma chave que já protege a senha de
-- e-mail (`ERP_CHAVE_SEGREDOS`, que mora na Environment do Render e nunca no
-- banco). Quem tiver uma cópia do banco não tem o certificado — e certificado
-- digital é a assinatura da empresa: quem o tem, assina no nome dela.
--
-- SEM A CHAVE, NÃO SE GRAVA. A alternativa — guardar em claro "só desta vez" —
-- é como uma assinatura de empresa vaza sem ninguém perceber.
--
-- A VALIDADE NÃO É DIGITADA: é lida de dentro do próprio arquivo. Campo de
-- data que a pessoa preenche é campo que ela erra ou esquece de atualizar — e
-- aqui o erro só apareceria no dia em que a nota não sai.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS empresa_certificados (
    id             BIGSERIAL PRIMARY KEY,
    empresa_id     BIGINT NOT NULL REFERENCES empresas(id) ON DELETE CASCADE,

    -- O arquivo e a senha, os dois cifrados. Nunca em claro, nem "só para
    -- testar": o teste que fica é o que vaza.
    arquivo_cifrado TEXT NOT NULL,
    senha_cifrada   TEXT NOT NULL,
    nome_arquivo    TEXT,

    -- Lidos DE DENTRO do certificado, não digitados.
    titular         TEXT,
    documento       TEXT,
    emissor         TEXT,
    numero_serie    TEXT,
    valido_de       DATE,
    valido_ate      DATE NOT NULL,

    situacao        TEXT NOT NULL DEFAULT 'ATIVO',
    observacao      TEXT,
    enviado_por     BIGINT REFERENCES usuarios(id),
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE empresa_certificados DROP CONSTRAINT IF EXISTS ck_certificado_situacao;
ALTER TABLE empresa_certificados ADD CONSTRAINT ck_certificado_situacao
    CHECK (situacao IN ('ATIVO', 'SUBSTITUIDO'));

ALTER TABLE empresa_certificados DROP CONSTRAINT IF EXISTS ck_certificado_periodo;
ALTER TABLE empresa_certificados ADD CONSTRAINT ck_certificado_periodo
    CHECK (valido_de IS NULL OR valido_ate >= valido_de);

-- UM ATIVO POR EMPRESA. O anterior não é apagado — vira SUBSTITUIDO e fica no
-- histórico, porque a nota assinada em março foi assinada com AQUELE
-- certificado, e um dia alguém vai perguntar com qual.
CREATE UNIQUE INDEX IF NOT EXISTS idx_certificado_ativo_por_empresa
    ON empresa_certificados (empresa_id) WHERE situacao = 'ATIVO';

CREATE INDEX IF NOT EXISTS idx_certificado_validade
    ON empresa_certificados (valido_ate) WHERE situacao = 'ATIVO';

-- ---------------------------------------------------------------------------
-- A agenda passa a conhecer o certificado
--
-- Era o quarto aviso que ela prometia e não tinha de onde tirar. A restrição
-- da migração 051 listava as origens que existiam então; aqui ela cresce.
-- ---------------------------------------------------------------------------
ALTER TABLE agenda_eventos DROP CONSTRAINT IF EXISTS ck_agenda_origem;
ALTER TABLE agenda_eventos ADD CONSTRAINT ck_agenda_origem
    CHECK (origem IN ('REAJUSTE', 'CERTIDAO', 'LOCACAO', 'CONTRATO',
                      'CERTIFICADO', 'MANUAL'));
