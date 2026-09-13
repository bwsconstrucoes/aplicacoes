-- ===========================================================================
-- Migração 047 — a chave Pix da conta, e os dados de emissão de nota POR EMPRESA.
--
-- DUAS COISAS PEDIDAS PELO DONO EM 09/09/2026, e a segunda mudou de prioridade
-- depois que ele respondeu uma pergunta.
--
-- 1) A CHAVE PIX DA CONTA BANCÁRIA
--
-- O motivo é o uso real, nas palavras dele: *"eventualmente a gente precisa
-- consultar, e tendo esse cadastro das contas é o local mais fácil da gente
-- consultar."* Não é para pagar por ali — é para COPIAR E MANDAR quando alguém
-- pede os dados da empresa.
--
-- Uma conta pode ter várias chaves (CNPJ, e-mail, telefone, aleatória), por
-- isso é tabela e não coluna.
--
-- 2) OS DADOS DE EMISSÃO, POR EMPRESA
--
-- Isto ia ficar para o fim. Mudou quando ele respondeu: *"a BWS não tem
-- inscrição municipal em Petrolina, mas outra empresa que vamos operar sim"*, e
-- *"uma por API e outra manual"*.
--
-- Ou seja: emitir em mais de um município **não é planejamento para depois, é
-- requisito do primeiro dia**. Hoje o município, o endereço do serviço e o
-- código IBGE estão FIXOS no código do `emissaonf` (Eusébio/CE). Enquanto
-- estiverem, a segunda empresa simplesmente não emite.
--
-- O TOKEN VAI CIFRADO, pelo mesmo caminho da senha de e-mail (migração 038):
-- sem a `ERP_CHAVE_SEGREDOS` o sistema RECUSA gravar, em vez de guardar aberto.
-- Token de emissão assina em nome da empresa.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 1. Chaves Pix
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS conta_chaves_pix (
    id          BIGSERIAL PRIMARY KEY,
    conta_id    BIGINT NOT NULL REFERENCES contas_bancarias(id) ON DELETE CASCADE,
    tipo        TEXT NOT NULL,
    chave       TEXT NOT NULL,
    descricao   TEXT,
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE conta_chaves_pix DROP CONSTRAINT IF EXISTS ck_pix_tipo;
ALTER TABLE conta_chaves_pix ADD CONSTRAINT ck_pix_tipo
    CHECK (tipo IN ('CNPJ', 'CPF', 'EMAIL', 'TELEFONE', 'ALEATORIA'));

-- A mesma chave não entra duas vezes na mesma conta. Duas linhas iguais só
-- fariam quem copia hesitar sobre qual é a boa.
CREATE UNIQUE INDEX IF NOT EXISTS idx_pix_conta_chave
    ON conta_chaves_pix (conta_id, chave);
CREATE INDEX IF NOT EXISTS idx_pix_conta ON conta_chaves_pix (conta_id);

-- ---------------------------------------------------------------------------
-- 2. Emissão de nota por empresa
-- ---------------------------------------------------------------------------
ALTER TABLE empresas ADD COLUMN IF NOT EXISTS emissao_modo TEXT NOT NULL DEFAULT 'MANUAL';
ALTER TABLE empresas ADD COLUMN IF NOT EXISTS emissao_municipio TEXT;
ALTER TABLE empresas ADD COLUMN IF NOT EXISTS emissao_codigo_ibge TEXT;
ALTER TABLE empresas ADD COLUMN IF NOT EXISTS emissao_url_base TEXT;
ALTER TABLE empresas ADD COLUMN IF NOT EXISTS emissao_canal TEXT NOT NULL DEFAULT 'NACIONAL';
ALTER TABLE empresas ADD COLUMN IF NOT EXISTS emissao_token_cifrado TEXT;
ALTER TABLE empresas ADD COLUMN IF NOT EXISTS emissao_serie TEXT;
ALTER TABLE empresas ADD COLUMN IF NOT EXISTS emissao_aliquota_iss NUMERIC(6,4);
ALTER TABLE empresas ADD COLUMN IF NOT EXISTS emissao_codigo_servico TEXT;
ALTER TABLE empresas ADD COLUMN IF NOT EXISTS emissao_ambiente TEXT NOT NULL DEFAULT 'HOMOLOGACAO';

-- MANUAL é o padrão de propósito: empresa recém-cadastrada não sai emitindo
-- nota fiscal sozinha porque alguém esqueceu de configurar. Ligar a emissão
-- automática é ato consciente.
ALTER TABLE empresas DROP CONSTRAINT IF EXISTS ck_empresa_emissao_modo;
ALTER TABLE empresas ADD CONSTRAINT ck_empresa_emissao_modo
    CHECK (emissao_modo IN ('MANUAL', 'API'));

-- O canal NACIONAL é o padrão porque a LC 214/2025 tornou o padrão nacional
-- obrigatório e o ABRASF 2.04 está sendo encerrado ao longo de 2026. ABRASF
-- fica disponível para o município que ainda não migrou.
ALTER TABLE empresas DROP CONSTRAINT IF EXISTS ck_empresa_emissao_canal;
ALTER TABLE empresas ADD CONSTRAINT ck_empresa_emissao_canal
    CHECK (emissao_canal IN ('NACIONAL', 'ABRASF'));

-- HOMOLOGAÇÃO é o padrão porque emitir nota é ato irreversível: cada emissão em
-- produção gera documento fiscal de verdade. Passar para produção é decisão
-- consciente, tomada depois de conferir uma nota contra outra que se sabe certa.
ALTER TABLE empresas DROP CONSTRAINT IF EXISTS ck_empresa_emissao_ambiente;
ALTER TABLE empresas ADD CONSTRAINT ck_empresa_emissao_ambiente
    CHECK (emissao_ambiente IN ('HOMOLOGACAO', 'PRODUCAO'));

-- Empresa que emite por API TEM de saber para onde mandar. Sem isto, ligar o
-- modo automático deixaria a nota falhando no meio do fechamento do mês.
ALTER TABLE empresas DROP CONSTRAINT IF EXISTS ck_empresa_api_tem_destino;
ALTER TABLE empresas ADD CONSTRAINT ck_empresa_api_tem_destino CHECK (
    emissao_modo <> 'API'
    OR (nullif(btrim(emissao_url_base), '') IS NOT NULL
        AND nullif(btrim(emissao_codigo_ibge), '') IS NOT NULL)
);
