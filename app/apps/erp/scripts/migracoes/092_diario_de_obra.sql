-- ===========================================================================
-- 092 — O DIÁRIO DE OBRA
--
-- Ditado pelo dono em 08/11/2026. O PRINCÍPIO, e ele manda em todo o desenho:
-- o diário pergunta SÓ o que ninguém sabe além de quem esteve lá. Tudo o que o
-- sistema já tem, o sistema preenche — e por isso quase nada do cabeçalho vira
-- coluna aqui.
--
-- O QUE NÃO É COLUNA, DE PROPÓSITO:
--
--   · o cabeçalho da obra (contrato, OS, início, término, objeto, responsável
--     técnico) — vem de `obras`. Copiar aqui seria criar uma segunda verdade
--     que envelhece: obra que tem o prazo aditado passaria a ter um diário
--     dizendo o prazo velho;
--   · a conta de prazo (dia N de M, faltam X) — é aritmética sobre aquelas
--     datas, e aritmética guardada é aritmética que desatualiza;
--   · o EFETIVO de quem é funcionário — vem do PONTO, e foi exigência dele:
--     *"hoje a gente tem um ponto. Então a gente não precisaria colocar
--     quantidade de pessoas e função (…) já traria do ponto que é quem
--     trabalhou."* Guardar a lista aqui faria o diário divergir do espelho no
--     primeiro ajuste de marcação;
--   · o material recebido — vem das entregas de Suprimentos, pelo mesmo
--     motivo.
--
-- O QUE É COLUNA: o que só existe porque alguém escreveu. O relato, o tempo,
-- as ocorrências, a observação da fiscalização, e as assinaturas.
--
-- ⚑ O TERCEIRIZADO É A EXCEÇÃO e por isso tem tabela própria: ele trabalhou na
-- obra e NÃO bate ponto ali — *"às vezes você tem um fornecedor terceirizado
-- que também está trabalhando, um pintor terceirizado que não é funcionário"*.
-- Esse dado não existe em lugar nenhum senão aqui.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS diarios_obra (
    id              BIGSERIAL PRIMARY KEY,
    obra_id         BIGINT      NOT NULL REFERENCES obras(id),
    data            DATE        NOT NULL,

    -- O TEMPO, por período. Texto e não ENUM porque a lista muda com o modelo
    -- que o órgão exige, e migração para acrescentar "garoa" seria absurdo.
    tempo_manha     TEXT,
    tempo_tarde     TEXT,

    -- ⚑ O RELATO DIÁRIO. Ele corrigiu o nome em 08/11/2026: *"em relação aos
    -- serviços executados, eu acho que a gente pode alterar esse termo, colocar
    -- relato diário mesmo"*. O nome da coluna segue o nome que ele usa.
    relato          TEXT,

    -- "NÃO HOUVE EXPEDIENTE" é estado, não texto no relato. Sábado, domingo e
    -- feriado nascem assim — e deixam de ser pendência na tela sem que ninguém
    -- precise escrever "não houve expediente" 104 vezes por ano.
    sem_expediente  BOOLEAN     NOT NULL DEFAULT FALSE,

    -- As ocorrências e a visita da fiscalização.
    ocorrencias     TEXT,
    fiscalizacao_esteve BOOLEAN NOT NULL DEFAULT FALSE,
    fiscalizacao_quem   TEXT,

    -- ⚑ ESTE CAMPO É DA FISCALIZAÇÃO, E A OBRA NÃO ESCREVE NELE. Regra dele:
    -- *"a gente poder conceder acesso a, por exemplo, a fiscalização, para a
    -- fiscalização fazer observações."* Um diário que a própria obra pode
    -- reescrever por inteiro não prova nada — é justamente a parte que ela NÃO
    -- controla que dá valor ao documento. A separação é garantida no código
    -- (`core/obras/diario.py`), não só aqui.
    obs_fiscalizacao        TEXT,
    obs_fiscalizacao_por    TEXT,
    obs_fiscalizacao_em     TIMESTAMPTZ,

    -- O CLIMA MEDIDO, não o informado. Guardado como veio do serviço, com a
    -- fonte e a hora: é prova INDEPENDENTE para pleito de prazo, e prova sem
    -- procedência não é prova. `jsonb` porque o formato do serviço muda e não
    -- vale uma migração por campo novo.
    clima_medido    JSONB,

    situacao        TEXT        NOT NULL DEFAULT 'ABERTO',
    fechado_em      TIMESTAMPTZ,
    fechado_por     BIGINT      REFERENCES usuarios(id),

    -- A assinatura do responsável técnico, que vem do cadastro da OBRA.
    assinado_em     TIMESTAMPTZ,
    assinado_por    BIGINT      REFERENCES usuarios(id),
    assinado_nome   TEXT,

    criado_por      BIGINT      REFERENCES usuarios(id),
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- UM DIÁRIO POR OBRA POR DIA. É a trava que impede dois administrativos
    -- abrirem o mesmo dia e um sobrescrever o outro.
    CONSTRAINT diario_unico_por_dia UNIQUE (obra_id, data),
    CONSTRAINT diario_situacao_conhecida
        CHECK (situacao IN ('ABERTO', 'FECHADO'))
);

CREATE INDEX IF NOT EXISTS idx_diarios_obra_data
    ON diarios_obra (obra_id, data DESC);
CREATE INDEX IF NOT EXISTS idx_diarios_pendentes
    ON diarios_obra (data) WHERE situacao = 'ABERTO';


-- ---------------------------------------------------------------------------
-- QUEM TRABALHOU E NÃO BATE PONTO AQUI
--
-- O efetivo próprio vem do ponto e não é copiado. Esta tabela é só para o que
-- o ponto não sabe: o terceirizado. Por isso ela guarda NOME e não
-- `colaborador_id` — essa gente não está no cadastro, e forçá-la para dentro
-- dele para poder aparecer no diário criaria colaborador fantasma na folha.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS diario_efetivo_extra (
    id              BIGSERIAL PRIMARY KEY,
    diario_id       BIGINT      NOT NULL REFERENCES diarios_obra(id) ON DELETE CASCADE,
    descricao       TEXT        NOT NULL,   -- "Equipe Silva Pinturas"
    funcao          TEXT,                   -- "pintura"
    quantidade      INTEGER     NOT NULL DEFAULT 1,
    empresa         TEXT,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT efetivo_extra_quantidade_positiva CHECK (quantidade > 0)
);

CREATE INDEX IF NOT EXISTS idx_diario_efetivo_extra
    ON diario_efetivo_extra (diario_id);


-- ---------------------------------------------------------------------------
-- A CORREÇÃO DEPOIS DE FECHADO
--
-- Diário é DOCUMENTO: ele sustenta pleito de prazo e é lido por fiscalização.
-- Corrigir não pode ser proibido (erro de digitação acontece) nem silencioso
-- (reescrever o passado sem rastro é o que tira o valor do documento). Então
-- corrige-se COM REGISTRO: o que estava, o que passou a estar, quem e por quê.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS diario_correcoes (
    id              BIGSERIAL PRIMARY KEY,
    diario_id       BIGINT      NOT NULL REFERENCES diarios_obra(id) ON DELETE CASCADE,
    campo           TEXT        NOT NULL,
    antes           TEXT,
    depois          TEXT,
    motivo          TEXT        NOT NULL,
    usuario_id      BIGINT      REFERENCES usuarios(id),
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_diario_correcoes
    ON diario_correcoes (diario_id, criado_em DESC);


-- ---------------------------------------------------------------------------
-- QUEM VÊ E QUEM MEXE
--
-- A SEÇÃO entra aqui; as AÇÕES que ela concede vivem em `core/auth/secoes.py`,
-- que é onde o ERP inteiro as declara. Não se cria tabela de permissão nova
-- para uma tela nova — a máquina já existe.
--
-- `obr_diario` dá: ver o diário (LER) e lançá-lo (EDITAR). Quem lança é o
-- ADMINISTRATIVO da obra — decisão dele em 08/11/2026: *"quem lança realmente
-- é o administrativo da obra, não é encarregado, não."*
--
-- ⚑ O PARCEIRO ENTRA COM "LER", E ISSO NÃO É DETALHE. É por ele que a
-- FISCALIZAÇÃO alcança o diário: gente de fora, presa a uma obra. Ela lê tudo
-- daquela obra e escreve APENAS no bloco de observações da fiscalização — e
-- essa separação é garantida no código (`core/obras/diario.py`), não pelo
-- nível da seção, porque é uma regra sobre QUAL CAMPO, não sobre a tela.
-- ---------------------------------------------------------------------------
INSERT INTO perfil_secoes (perfil_id, secao, nivel)
SELECT p.id, v.secao, v.nivel
  FROM perfis p
  JOIN (VALUES
    ('Administrador',           'obr_diario', 'EDITAR'),
    ('Gestor de obras',         'obr_diario', 'EDITAR'),
    ('Supervisor de obras',     'obr_diario', 'EDITAR'),
    ('Administrativo de obra',  'obr_diario', 'EDITAR'),
    ('Diretor financeiro',      'obr_diario', 'LER'),
    ('Administrativo financeiro', 'obr_diario', 'LER'),
    ('Departamento pessoal',    'obr_diario', 'LER'),
    ('Aprovador',               'obr_diario', 'LER'),
    ('Consulta',                'obr_diario', 'LER'),
    -- ⚑ O NOME DO PERFIL É "Parceiro da obra", NÃO "Parceiro". Escrevi
    -- "Parceiro" na primeira versão, o JOIN não casou, e ele ficou em silêncio
    -- sem a seção — justamente o perfil por onde a FISCALIZAÇÃO entra. Quem
    -- acusou foi o teste que compara o perfil pronto do banco com o cargo
    -- antigo do código. INSERT com JOIN por nome não reclama quando o nome
    -- está errado: ele simplesmente não insere nada.
    ('Parceiro da obra',        'obr_diario', 'LER')
  ) AS v(perfil, secao, nivel) ON v.perfil = p.nome
ON CONFLICT (perfil_id, secao) DO NOTHING;
