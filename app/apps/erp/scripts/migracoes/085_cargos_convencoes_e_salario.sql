-- ============================================================================
-- 085 — CARGO, CONVENÇÃO COLETIVA e o SALÁRIO COM HISTÓRICO
--
-- Pedido do dono, 05/11/2026, olhando a planilha de cargos que ele exportou do
-- Pipefy: *"veja que ela tá mal construída. Mistura Cargo e convenção. Isso é
-- pra ser distinto. Seleciona Cargo e Selecionar Convenção Coletiva. Ao mesmo
-- tempo tem a questão salarial e as alterações salariais pra serem gravadas."*
--
-- ELE ESTÁ CERTO, E OS NÚMEROS DA PLANILHA PROVAM: 135 linhas para apenas 63
-- cargos — cada cargo repetido 2,1 vez, uma por convenção. PEDREIRO aparece 11
-- vezes; VIGIA e SERVENTE DE OBRAS, 7 cada. A chave da planilha é o texto
-- "PEDREIRO (CE-Fortaleza)", que junta as duas coisas num campo só — e é por
-- isso que mudar o piso de uma convenção obriga a achar e editar linha por
-- linha.
--
-- O DESENHO AQUI SEPARA TRÊS COISAS que a planilha confunde:
--
--   1. `cargos`           — o que a pessoa FAZ. PEDREIRO é um, com um CBO.
--   2. `convencoes`       — o acordo coletivo que manda no piso. "CE-Fortaleza",
--                           "BA-Sobradinho", "Sem Convenção".
--   3. `cargo_convencao`  — o CRUZAMENTO: o piso de PEDREIRO em CE-Fortaleza, e
--                           a categoria dele dentro daquela convenção.
--
-- E O PISO TEM HISTÓRICO (`cargo_convencao_salarios`), porque convenção tem
-- data-base: o piso de 2025 não é o de 2026, e a folha de um mês passado tem de
-- continuar conferindo com o piso que valia naquele mês. Guardar só o valor de
-- hoje é apagar a prova do que foi pago — e é o que a planilha faz.
--
-- A ALTERAÇÃO SALARIAL DA PESSOA é outra coisa, e vem na 087: o piso é o mínimo
-- da convenção; o salário dela pode ser maior, e muda por promoção, mérito ou
-- dissídio. São duas histórias diferentes e não podem morar na mesma tabela.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. CARGOS — o que a pessoa faz
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cargos (
    id              BIGSERIAL PRIMARY KEY,
    nome            TEXT NOT NULL,
    -- O CBO vem da planilha em CINCO formatos diferentes: 87 linhas com máscara
    -- ("7152-10"), 24 como número decimal ("715505.0" — o Sheets converteu em
    -- número e perdeu o traço), 15 vazias, 7 só com dígitos e 1 com máscara
    -- errada ("72411-0", 5 dígitos antes do traço). Aqui ele é guardado SÓ COM
    -- OS DÍGITOS (7 deles), e a tela exibe com a máscara: assim "715210",
    -- "7152-10" e "715210.0" param de ser três cargos diferentes.
    cbo             TEXT,
    descricao       TEXT,
    -- Riscos e EPI vêm da planilha com as colunas existindo e ZERO linhas
    -- preenchidas. Ficam aqui porque a ficha de EPI é documento obrigatório na
    -- obra e um dia alguém vai preencher — mas nascem vazios e sem promessa.
    riscos          TEXT,
    epis            TEXT,
    ativo           BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Nome único SEM depender de maiúscula e acento: a planilha tem "ALMOXARIFADO"
-- onde queria "ALMOXARIFE" e "PEDEEIRO" onde queria "PEDREIRO". Erro de
-- digitação não se resolve por índice — mas cargo repetido com outra caixa, sim.
CREATE UNIQUE INDEX IF NOT EXISTS ux_cargos_nome ON cargos (upper(btrim(nome)));

-- ----------------------------------------------------------------------------
-- 2. CONVENÇÕES COLETIVAS — quem manda no piso
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS convencoes (
    id              BIGSERIAL PRIMARY KEY,
    nome            TEXT NOT NULL,
    uf              TEXT,
    -- O MÊS DA DATA-BASE (1 a 12). É quando o piso muda, todo ano. Serve para o
    -- sistema avisar "a convenção X tem data-base em janeiro e o piso é de
    -- 2025" — hoje ninguém é avisado, e a folha roda com piso velho.
    mes_data_base   SMALLINT CHECK (mes_data_base IS NULL
                                    OR mes_data_base BETWEEN 1 AND 12),
    sindicato       TEXT,
    observacoes     TEXT,
    ativo           BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A planilha tem 16 convenções que na verdade são 13: "MT-Mato Grosso" e
-- "MT- Mato Grosso", "Sem Convenção" e "SEM CONVENÇÃO", "SP-Feticom" e
-- "SP - Feticom". O índice abaixo ignora caixa, espaço repetido E ESPAÇO AO
-- REDOR DO HÍFEN — e é esse último que importa: sem ele, dois dos três pares
-- continuariam entrando como convenções diferentes, com dois pisos para o
-- mesmo cargo. (Descoberto rodando a importação contra a planilha de verdade:
-- deu 15 convenções onde devia dar 13.)
CREATE UNIQUE INDEX IF NOT EXISTS ux_convencoes_nome
    ON convencoes (regexp_replace(
         regexp_replace(upper(btrim(nome)), '\s+', ' ', 'g'), '\s*-\s*', '-', 'g'));

-- ----------------------------------------------------------------------------
-- 3. O CRUZAMENTO: o piso deste cargo nesta convenção
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cargo_convencao (
    id              BIGSERIAL PRIMARY KEY,
    cargo_id        BIGINT NOT NULL REFERENCES cargos(id),
    convencao_id    BIGINT NOT NULL REFERENCES convencoes(id),
    -- A CATEGORIA é o nível DENTRO da convenção (PROFISSIONAL, QUALIFICADO,
    -- SERVENTE, MEIO-PROFISSIONAL…), e é ela que a convenção usa para fixar o
    -- piso. Na planilha vêm 19 valores, mas 6 são a mesma coisa escrita
    -- diferente ("não qualificados", "Não qualificados", "NÃO QUALIFICADO") e
    -- 4 trazem o sufixo da própria convenção ("PROFISSIONAL (MA)") — redundante,
    -- porque a convenção já é a coluna ao lado.
    categoria       TEXT,
    -- O piso VIGENTE, repetido aqui de propósito: é o que toda tela lê. A
    -- história fica na tabela de baixo, e as duas são mantidas juntas pela
    -- mesma função — ver `core/cadastros/cargos.py`.
    salario_piso    NUMERIC(14,2),
    valor_diaria    NUMERIC(14,2),      -- 48 das 135 linhas têm diária
    ativo           BOOLEAN NOT NULL DEFAULT TRUE,
    ref_pipefy      TEXT,               -- o "Código" do card, para conferência
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (cargo_id, convencao_id)
);

CREATE INDEX IF NOT EXISTS ix_cargo_convencao_cargo ON cargo_convencao (cargo_id);
CREATE INDEX IF NOT EXISTS ix_cargo_convencao_conv ON cargo_convencao (convencao_id);

-- ----------------------------------------------------------------------------
-- 4. O HISTÓRICO DO PISO — append-only
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cargo_convencao_salarios (
    id                  BIGSERIAL PRIMARY KEY,
    cargo_convencao_id  BIGINT NOT NULL REFERENCES cargo_convencao(id) ON DELETE CASCADE,
    salario_piso        NUMERIC(14,2) NOT NULL,
    valor_diaria        NUMERIC(14,2),
    -- De quando vale. `vigencia_fim` nulo = é o piso de hoje.
    vigencia_inicio     DATE NOT NULL,
    vigencia_fim        DATE,
    motivo              TEXT,           -- "dissídio 2026", "correção", "importado"
    usuario_id          BIGINT REFERENCES usuarios(id),
    criado_em           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (vigencia_fim IS NULL OR vigencia_fim >= vigencia_inicio)
);

CREATE INDEX IF NOT EXISTS ix_cc_salarios_cc
    ON cargo_convencao_salarios (cargo_convencao_id, vigencia_inicio DESC);

-- Um piso vigente por cruzamento: sem isto, duas linhas abertas fariam o
-- sistema responder dois valores para "qual o piso hoje" — e cada tela
-- escolheria um.
CREATE UNIQUE INDEX IF NOT EXISTS ux_cc_salario_vigente
    ON cargo_convencao_salarios (cargo_convencao_id) WHERE vigencia_fim IS NULL;

-- ----------------------------------------------------------------------------
-- 5. A LIGAÇÃO COM O CADASTRO DA PESSOA
--
-- `colaboradores.funcao_id` continua existindo e continua sendo usado pelo que
-- já está em produção (a diária de referência da despesa de colaborador). O
-- cargo novo entra AO LADO, e não no lugar: trocar a coluna de uma vez
-- quebraria o lançamento de despesa, que é dinheiro, por uma mudança de
-- cadastro. Quando o cadastro novo estiver em uso, a função sai numa migração
-- própria — com o de-para pronto.
-- ----------------------------------------------------------------------------
ALTER TABLE colaboradores
    ADD COLUMN IF NOT EXISTS cargo_id      BIGINT REFERENCES cargos(id),
    ADD COLUMN IF NOT EXISTS convencao_id  BIGINT REFERENCES convencoes(id);

CREATE INDEX IF NOT EXISTS ix_colaboradores_cargo ON colaboradores (cargo_id);
