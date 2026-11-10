-- ---------------------------------------------------------------------------
-- 100 — OS PAPÉIS VIAJAM COM O PEDIDO, antes de a pessoa existir
--
-- Pedido do dono em 10/11/2026, depois de discordar — com razão — da minha
-- proposta de deixar o cadastro entrar incompleto:
--
--   *"Se você permitir que entre funcionário sem os documentos, esse documento
--   para ele voltar e ser concluído é um Deus nos acuda. (…) Não se consegue
--   pagar o funcionário no SomaPay se não tiver CPF, não se consegue fazer o
--   cadastro do funcionário se não tiver comprovante de endereço e CPF. Então
--   essa parte cadastral documental básica — dados da pessoa com CPF, CTPS,
--   PIS e comprovante de endereço — isso é o básico do cadastro."*
--
-- ⚑ O ARGUMENTO QUE DERRUBOU O MEU: deixar passar não evita o mal que eu
-- queria evitar. A pessoa continua sem receber (o SomaPay recusa sem CPF), e
-- ainda sobra um cadastro pela metade que ninguém fecha. Pendência de
-- documento não é dívida que se paga depois — é dívida que ninguém paga.
--
-- Para o cadastro poder ser RECUSADO sem documento, o documento precisa vir
-- JUNTO do pedido. E o pedido de cadastro é o único que não tem colaborador
-- ainda: a pessoa nasce quando o DP atende. Daí esta coluna.
--
-- Ao atender, o documento é transferido para o colaborador recém-criado, na
-- mesma transação — e some daqui. O papel nunca fica órfão.
-- ---------------------------------------------------------------------------
ALTER TABLE documentos
    ADD COLUMN IF NOT EXISTS dp_solicitacao_id BIGINT
        REFERENCES dp_solicitacoes(id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_documentos_dp_solicitacao
    ON documentos (dp_solicitacao_id) WHERE dp_solicitacao_id IS NOT NULL;

-- ⚑ O CHECK DE DONO ÚNICO PRECISA CONHECER O NOVO DONO. Sem isto, anexar um
-- papel ao pedido seria recusado pelo próprio banco — o documento tem de ter
-- exatamente UM dono, e "pedido de DP" passa a ser um deles.
ALTER TABLE documentos DROP CONSTRAINT IF EXISTS ck_documento_um_dono;
ALTER TABLE documentos ADD CONSTRAINT ck_documento_um_dono CHECK (
    (CASE WHEN empresa_id        IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN obra_id           IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN colaborador_id    IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN fornecedor_id     IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN lancamento_id     IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN dp_solicitacao_id IS NOT NULL THEN 1 ELSE 0 END) = 1);

-- Os tipos de documento que o cadastro exige. `ON CONFLICT DO NOTHING`
-- porque os três primeiros já existem desde a migração do Arquivo.
INSERT INTO documento_tipos (codigo, nome, grupo, dono)
VALUES ('DOC-IDENTIDADE',              'Documento de identificação (RG, CNH ou CTPS)', 'PESSOAL', 'PESSOA'),
       ('CPF-DOC',                     'CPF (Cadastro de Pessoa Física)',              'PESSOAL', 'PESSOA'),
       ('COMPROVANTE-ENDERECO-PESSOA', 'Comprovante de endereço',                      'PESSOAL', 'PESSOA'),
       ('CTPS-FOTO',                   'CTPS (página com a foto do colaborador)',      'PESSOAL', 'PESSOA'),
       ('CTPS-INFORMACOES',            'CTPS (página com as informações do colaborador)', 'PESSOAL', 'PESSOA'),
       ('DOC-DEPENDENTE',              'Documento do dependente (certidão ou RG)',     'PESSOAL', 'PESSOA')
ON CONFLICT (codigo) DO NOTHING;
