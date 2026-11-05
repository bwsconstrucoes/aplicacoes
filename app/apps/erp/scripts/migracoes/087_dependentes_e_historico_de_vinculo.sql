-- ============================================================================
-- 087 — DEPENDENTES, e a HISTÓRIA do vínculo, do cargo e do salário
--
-- Duas coisas que o Pipefy não conseguia guardar, e é por isso que o dono pediu
-- o cadastro no ERP.
--
-- 1. DEPENDENTES. No pipe são ONZE campos fixos: "Quantidade de Dependentes"
--    (um select com as opções *Filho 1*…*Filho 5*), mais Nome e Data de
--    Nascimento de cada um. Quem tem seis filhos não cabe; quem tem um
--    dependente que não é filho nem cônjuge também não. E o campo "quantidade"
--    é uma LISTA, não um número — porque o Pipefy precisava dele para mostrar e
--    esconder os outros dez.
--
-- 2. A HISTÓRIA. O dono, 04/11/2026: *"a gente tem a base dos desligados, para
--    a gente não refazer um cadastro que já existe (…) essa pessoa sai, mas ele
--    está sendo readmitido"*. E sobre o prestador: *"você fez um cadastro dele
--    como prestador e quer admitir e tornar ele CTPS"*. As duas coisas exigem a
--    mesma peça: **o vínculo tem começo e fim, e a pessoa tem mais de um.**
--
--    Hoje `colaboradores` guarda UMA admissão e UMA demissão. Readmitir
--    sobrescreve a data e apaga a passagem anterior; trocar prestador por CTPS
--    apaga o período em que ela foi prestador. Nos dois casos o cadastro passa a
--    mentir sobre o passado — e é o passado que a rescisão, o INSS e a Justiça
--    do Trabalho perguntam.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. DEPENDENTES — sem teto e sem lista falsa
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS colaborador_dependentes (
    id                  BIGSERIAL PRIMARY KEY,
    colaborador_id      BIGINT NOT NULL REFERENCES colaboradores(id) ON DELETE CASCADE,
    nome                TEXT NOT NULL,
    -- FILHO, CONJUGE, ENTEADO, PAI, MAE, OUTRO. O pipe só tinha "Filho" e
    -- "Cônjuge" no cadastro de dependente, e uma lista maior ("Pai ou Mãe",
    -- "Avô ou Avó", "Irmãos") só no pedido de LICENÇA POR FALECIMENTO — duas
    -- listas para a mesma pergunta. Aqui é uma.
    parentesco          TEXT NOT NULL,
    data_nascimento     DATE,
    cpf                 TEXT,
    -- As duas marcas que a folha usa, e que são DIFERENTES: salário-família tem
    -- limite de idade e de renda; dependente de IR, outro critério. Guardar uma
    -- só obrigaria a folha a adivinhar a outra.
    salario_familia     BOOLEAN NOT NULL DEFAULT FALSE,
    dependente_ir       BOOLEAN NOT NULL DEFAULT FALSE,
    observacoes         TEXT,
    criado_em           TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_dependentes_colaborador
    ON colaborador_dependentes (colaborador_id);

-- O mesmo CPF não entra duas vezes como dependente da mesma pessoa. Fica
-- parcial porque criança não tem CPF em muitos cadastros antigos.
CREATE UNIQUE INDEX IF NOT EXISTS ux_dependentes_cpf
    ON colaborador_dependentes (colaborador_id, regexp_replace(cpf, '\D', '', 'g'))
 WHERE cpf IS NOT NULL AND regexp_replace(cpf, '\D', '', 'g') <> '';

-- ----------------------------------------------------------------------------
-- 2. VÍNCULOS — cada passagem da pessoa pela empresa
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS colaborador_vinculos (
    id                  BIGSERIAL PRIMARY KEY,
    colaborador_id      BIGINT NOT NULL REFERENCES colaboradores(id) ON DELETE CASCADE,
    -- CTPS ou PRESTADOR: o mesmo valor de `colaboradores.vinculo`, congelado no
    -- período. É assim que "foi prestador até março e virou CTPS em abril" fica
    -- legível sem ninguém reconstruir nada.
    vinculo             TEXT NOT NULL,
    tipo_contrato       TEXT,
    empresa_id          BIGINT REFERENCES empresas(id),
    cnpj_admissao       TEXT,
    cargo_id            BIGINT REFERENCES cargos(id),
    convencao_id        BIGINT REFERENCES convencoes(id),
    obra_id             BIGINT REFERENCES obras(id),
    salario             NUMERIC(14,2),
    matricula           TEXT,
    admissao            DATE NOT NULL,
    -- Nulo = é o vínculo de HOJE. Há índice único garantindo um só aberto.
    demissao            DATE,
    aviso_previo        DATE,
    ultimo_dia          DATE,
    -- Do pipe: "À Pedido do Colaborador", "Encerramento de Contrato de
    -- Experiência", "Demissão com Aviso Trabalhado", "Demissão com Aviso
    -- Indenizado".
    forma_desligamento  TEXT,
    motivo              TEXT,
    -- READMISSÃO: aponta para a passagem anterior. É o que permite responder
    -- "quantas vezes esta pessoa já trabalhou aqui" sem contar linha na mão.
    vinculo_anterior_id BIGINT REFERENCES colaborador_vinculos(id),
    usuario_id          BIGINT REFERENCES usuarios(id),
    criado_em           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (demissao IS NULL OR demissao >= admissao)
);

CREATE INDEX IF NOT EXISTS ix_vinculos_colaborador
    ON colaborador_vinculos (colaborador_id, admissao DESC);

-- UM vínculo aberto por pessoa. Sem isto, readmitir sem fechar o anterior
-- deixaria a pessoa com dois contratos valendo — e a folha pagaria os dois.
CREATE UNIQUE INDEX IF NOT EXISTS ux_vinculo_aberto
    ON colaborador_vinculos (colaborador_id) WHERE demissao IS NULL;

-- ----------------------------------------------------------------------------
-- 3. AS ALTERAÇÕES SALARIAIS E DE CARGO DA PESSOA
--
-- Pedido do dono, 05/11/2026: *"tem a questão salarial e as alterações
-- salariais pra serem gravadas."*
--
-- É DIFERENTE do histórico de piso da 085: lá é o mínimo que a convenção manda
-- para o cargo; aqui é o que ESTA pessoa recebe. O piso sobe por dissídio para
-- todo mundo; o salário dela sobe por promoção, mérito ou enquadramento. Quem
-- junta as duas histórias perde a resposta de "por que ela ganha mais que o
-- piso" — que é a pergunta da auditoria e do sindicato.
--
-- O pipe tinha isso como pedido ("Solicitação de Alteração de Cargo", com cargo
-- pleiteado, cargo deferido, salário e justificativa) mas NÃO guardava o
-- resultado em lugar nenhum: o card fechava e o campo do cadastro era
-- sobrescrito.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS colaborador_alteracoes (
    id                  BIGSERIAL PRIMARY KEY,
    colaborador_id      BIGINT NOT NULL REFERENCES colaboradores(id) ON DELETE CASCADE,
    vinculo_id          BIGINT REFERENCES colaborador_vinculos(id),
    -- SALARIO, CARGO, CARGO_E_SALARIO, OBRA, CONVENCAO
    tipo                TEXT NOT NULL,
    vigencia            DATE NOT NULL,
    cargo_de_id         BIGINT REFERENCES cargos(id),
    cargo_para_id       BIGINT REFERENCES cargos(id),
    convencao_de_id     BIGINT REFERENCES convencoes(id),
    convencao_para_id   BIGINT REFERENCES convencoes(id),
    obra_de_id          BIGINT REFERENCES obras(id),
    obra_para_id        BIGINT REFERENCES obras(id),
    salario_de          NUMERIC(14,2),
    salario_para        NUMERIC(14,2),
    -- MERITO, PROMOCAO, DISSIDIO, ENQUADRAMENTO, CORRECAO, TRANSFERENCIA
    motivo              TEXT,
    justificativa       TEXT,
    usuario_id          BIGINT REFERENCES usuarios(id),
    criado_em           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_alteracoes_colaborador
    ON colaborador_alteracoes (colaborador_id, vigencia DESC);
