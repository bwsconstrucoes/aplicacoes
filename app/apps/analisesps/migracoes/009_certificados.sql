-- ---------------------------------------------------------------------------
-- 009 — OS CERTIFICADOS DIGITAIS, GUARDADOS PELA TELA
--
-- Pedido do dono em 12/09/2026: *"nao daria pra adicionar o certificado a
-- partir da tela de configuracoes, inserir o arquivo, e adicionar la? Que
-- facilitaria uma troca ou a inclusao de outros certificados de outras
-- empresas."*
--
-- Ele tem razao, e o ganho e maior do que a conveniencia: com o certificado em
-- variavel de ambiente, cada troca (e ele VENCE TODO ANO) e mexer no Render e
-- reiniciar o servico, e cada empresa nova e uma variavel nova. Pela tela, e
-- subir um arquivo.
--
-- ⚠️ ISTO E A CREDENCIAL MAIS SENSIVEL DO SISTEMA. Com o arquivo e a senha,
-- qualquer um EMITE NOTA em nome da empresa. Por isso:
--
--   1. O ARQUIVO E A SENHA FICAM CIFRADOS. Um vazamento do banco - backup
--      esquecido, acesso indevido - entrega bytes embaralhados, nao o
--      certificado. A chave que decifra vive FORA do banco, no ambiente.
--   2. NAO HA COMO BAIXAR DE VOLTA. A tela mostra de quem e, ate quando vale e
--      quem subiu; o conteudo so sai daqui para dentro do proprio sistema, na
--      hora de falar com a Receita.
--   3. A VALIDADE E LIDA DO PROPRIO ARQUIVO no momento em que ele sobe. O A1
--      vale um ano e PARA DE FUNCIONAR CALADO no dia seguinte - guardar a data
--      e o que permite avisar antes, em vez de descobrir pela nota que nao
--      chegou.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analisesps.certificados (
    cnpj          TEXT PRIMARY KEY,            -- so digitos; um por empresa
    apelido       TEXT NOT NULL DEFAULT '',    -- como a pessoa reconhece
    titular       TEXT NOT NULL DEFAULT '',    -- o nome que esta DENTRO do certificado
    -- Cifrados. Nunca leia estas duas colunas fora de `certificados.py`.
    arquivo       BYTEA NOT NULL,
    senha         BYTEA NOT NULL,
    -- Lida de dentro do arquivo, nao digitada: data digitada a mao erra, e o
    -- erro so aparece no dia em que a busca para.
    valido_ate    DATE,
    ativo         BOOLEAN NOT NULL DEFAULT TRUE,
    subido_por    TEXT NOT NULL DEFAULT '',
    subido_em     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A busca percorre os ativos; a tela avisa dos que estao perto de vencer.
CREATE INDEX IF NOT EXISTS ix_cert_ativo ON analisesps.certificados (ativo);
CREATE INDEX IF NOT EXISTS ix_cert_validade ON analisesps.certificados (valido_ate);
