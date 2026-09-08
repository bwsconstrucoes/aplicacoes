-- ===========================================================================
-- Migração 038 — a EMPRESA que executa a obra, e o registro de cada e-mail
-- que sai do sistema.
--
-- POR QUE EXISTE
--
-- A BWS vai operar com mais de um CNPJ. Até aqui o ERP tinha só "a empresa",
-- implícita, e a obra não dizia por qual CNPJ ela corre. Isso trava três
-- coisas concretas:
--
--   1. a cotação sai de um e-mail — e tem de ser o e-mail da empresa que vai
--      comprar, senão o fornecedor responde para a caixa errada;
--   2. o relatório mandado ao fornecedor leva a logo e os dados de quem
--      compra, e são outros a cada CNPJ;
--   3. amanhã, nota fiscal e obrigação acessória são por CNPJ.
--
-- A ligação obra → empresa nasce OPCIONAL de propósito: as obras que já
-- existem não têm empresa, e obrigar agora derrubaria as telas antes de
-- alguém ter tempo de preencher. O disparo da cotação é que exige.
--
-- A CONTA DE ENVIO fica na empresa, não no sistema: é assim que "Construtora
-- A" manda de compras@a e "Construtora B" de compras@b, sem trocar
-- configuração no Render.
--
-- A SENHA NÃO É GUARDADA EM CLARO. `smtp_senha_cifrada` guarda o texto
-- cifrado por `core/comum/segredos.py`, com a chave em ERP_CHAVE_SEGREDOS
-- (Environment do Render). Sem a chave, o ERP recusa GRAVAR a senha e diz o
-- que falta — em vez de guardar em claro e ninguém perceber.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS empresas (
    id                  BIGSERIAL PRIMARY KEY,
    razao_social        TEXT    NOT NULL,
    nome_fantasia       TEXT,
    cnpj                TEXT    NOT NULL UNIQUE,
    inscricao_estadual  TEXT,
    inscricao_municipal TEXT,

    -- endereço: entra no cabeçalho do relatório e no rodapé do e-mail
    cep                 TEXT,
    logradouro          TEXT,
    numero              TEXT,
    complemento         TEXT,
    bairro              TEXT,
    municipio           TEXT,
    uf                  TEXT,

    telefone            TEXT,
    email               TEXT,          -- o endereço que aparece para quem recebe
    site                TEXT,

    -- Logo para os relatórios. Fica no banco, como os demais anexos do ERP
    -- (ver a decisão em CONTEXTO.md): um arquivo pequeno, lido junto com a
    -- empresa, sem depender de serviço de arquivos.
    logo                BYTEA,
    logo_mime           TEXT,
    logo_nome           TEXT,

    -- Conta de envio desta empresa
    smtp_servidor       TEXT,
    smtp_porta          INT,
    smtp_usuario        TEXT,
    smtp_senha_cifrada  TEXT,
    smtp_seguranca      TEXT NOT NULL DEFAULT 'STARTTLS'
                        CHECK (smtp_seguranca IN ('STARTTLS', 'SSL', 'NENHUMA')),
    smtp_remetente      TEXT,          -- "BWS Construções <compras@bws...>"
    smtp_responder_para TEXT,          -- para onde o fornecedor responde
    smtp_conferido_em   TIMESTAMPTZ,   -- último teste de envio que deu certo

    ativo               BOOLEAN NOT NULL DEFAULT TRUE,
    padrao              BOOLEAN NOT NULL DEFAULT FALSE,
    criado_em           TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Só uma empresa padrão. É ela que o sistema usa quando a obra não diz.
CREATE UNIQUE INDEX IF NOT EXISTS idx_empresas_padrao
    ON empresas (padrao) WHERE padrao;

ALTER TABLE obras
    ADD COLUMN IF NOT EXISTS empresa_id BIGINT REFERENCES empresas(id);

CREATE INDEX IF NOT EXISTS idx_obras_empresa ON obras (empresa_id);

-- ---------------------------------------------------------------------------
-- O registro de cada e-mail que sai
--
-- Existe para responder à pergunta do comprador: "isso foi mandado mesmo?".
-- Guarda o que foi mandado, para quem, por quem, quando e o que o servidor
-- respondeu. Guardar o CORPO é de propósito: seis meses depois, "o que foi
-- que a gente pediu?" é uma pergunta real, e a resposta não pode depender da
-- caixa de e-mail de ninguém.
--
-- ATENÇÃO ao que este registro NÃO prova: "ENVIADO" quer dizer que o servidor
-- de saída aceitou a mensagem. Não quer dizer entregue, e muito menos lido.
-- Quem quiser essa certeza precisa de um serviço de entrega com retorno, e
-- isso é outra decisão. A tela diz isso com estas palavras.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS envios_email (
    id              BIGSERIAL PRIMARY KEY,
    empresa_id      BIGINT REFERENCES empresas(id),

    -- a que o envio se refere: ('cotacao', 12) hoje; pedido e relatório depois
    entidade_tipo   TEXT    NOT NULL,
    entidade_id     BIGINT  NOT NULL,
    -- desdobramento dentro da entidade (o fornecedor, no caso da cotação)
    destinatario_tipo TEXT,
    destinatario_id   BIGINT,

    para            TEXT[]  NOT NULL DEFAULT '{}',
    copia           TEXT[]  NOT NULL DEFAULT '{}',
    responder_para  TEXT,
    assunto         TEXT    NOT NULL,
    corpo           TEXT    NOT NULL,
    anexos          JSONB   NOT NULL DEFAULT '[]',

    situacao        TEXT    NOT NULL
                    CHECK (situacao IN ('ENVIADO', 'FALHOU')),
    erro            TEXT,
    enviado_por     BIGINT REFERENCES usuarios(id),
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_envios_entidade
    ON envios_email (entidade_tipo, entidade_id);
CREATE INDEX IF NOT EXISTS idx_envios_destinatario
    ON envios_email (destinatario_tipo, destinatario_id);
