-- ============================================================================
-- Migração 072 — ACOMPANHAMENTO: a gestão burocrática da obra
--
-- Pedido do dono em 17/09/2026, aprovado em 18/09/2026. O desenho inteiro está
-- em `app/apps/erp/ACOMPANHAMENTO.md`; aqui fica só o que o banco precisa.
--
-- O que isto guarda: assunto que corre FORA da BWS e depende de terceiro —
-- aditivo de prazo, apostilamento, renovação de licença, protocolo. Não é
-- execução física da obra, e não é tarefa interna da equipe.
--
-- DUAS TABELAS, e a segunda é a que faz o módulo viver:
--
--   processos             o assunto, com situação, responsável e onde está;
--   processo_andamentos   a FRASE que alguém escreveu ao ligar para o órgão.
--
-- O andamento é uma linha de texto com data e autor, e não um formulário, por
-- exigência explícita dele: *"simples de alimentar e de atualizar"*. Se lançar
-- custar mais que uma frase, ninguém lança — e acompanhamento desatualizado é
-- pior que nenhum, porque quem bate o olho acredita.
--
-- O QUE NÃO ESTÁ AQUI, DE PROPÓSITO:
--
--   · nenhuma coluna de "etapa obrigatória" ou "fase que trava". Ele citou o
--     SEI como CONTRAEXEMPLO: *"não quero uma coisa travada"*. Pular, voltar e
--     fechar fora de ordem tem de ser permitido;
--   · nenhuma coluna "parado". Parado é CONCLUSÃO do sistema (dias desde o
--     último andamento), não campo que alguém marca — todo campo de situação
--     que depende de alguém lembrar de mexer vai estar errado um dia.
--
-- Obrigatórios: assunto e obra (ou empresa). O resto entra conforme a coisa
-- anda. Exigir órgão e prazo na abertura faz a pessoa abrir "depois", e depois
-- é nunca.
-- ============================================================================

CREATE TABLE IF NOT EXISTS processos (
    id              BIGSERIAL PRIMARY KEY,
    -- Número curto para citar em ofício e ao telefone ("é o AC-000042").
    numero          TEXT NOT NULL UNIQUE,
    assunto         TEXT NOT NULL,
    tipo            TEXT NOT NULL,
    -- Preso a UMA obra, ou à EMPRESA quando é da empresa (certidão, alvará da
    -- sede). Um dos dois, nunca os dois nem nenhum: sem dono, o processo não
    -- tem escopo, e sem escopo não há como recortar quem vê o quê.
    obra_id         BIGINT REFERENCES obras(id),
    empresa_id      BIGINT REFERENCES empresas(id),
    orgao           TEXT,
    responsavel_id  BIGINT REFERENCES usuarios(id),
    situacao        TEXT NOT NULL DEFAULT 'RASCUNHO',
    -- Onde o papel está AGORA: setor, pessoa, mesa. Texto livre de propósito —
    -- o organograma do órgão não é nosso para modelar, e tentar modelá-lo faria
    -- a pessoa escolher numa lista errada em vez de escrever o que ouviu.
    onde_esta       TEXT,
    protocolo       TEXT,
    protocolado_em  DATE,
    -- Para quando prometeram. É daqui que sai "passou da previsão".
    previsao        DATE,
    -- O documento que este processo está renovando ou aditivando, quando vem
    -- de um vencimento da Agenda. Liga o alerta ao trabalho que o resolve.
    documento_id    BIGINT REFERENCES anexos(id),
    encerrado_em    TIMESTAMPTZ,
    criado_por      BIGINT REFERENCES usuarios(id),
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ck_processo_situacao CHECK (situacao IN (
        'RASCUNHO', 'PROTOCOLADO', 'EM_ANALISE', 'EXIGENCIA',
        'DEFERIDO', 'INDEFERIDO', 'ARQUIVADO')),
    -- Exatamente um dono. `num_nonnulls` conta quantos não são nulos.
    CONSTRAINT ck_processo_um_dono CHECK (num_nonnulls(obra_id, empresa_id) = 1)
);

CREATE INDEX IF NOT EXISTS idx_processos_obra ON processos (obra_id, situacao);
CREATE INDEX IF NOT EXISTS idx_processos_responsavel
    ON processos (responsavel_id, situacao);
CREATE INDEX IF NOT EXISTS idx_processos_situacao ON processos (situacao, previsao);

COMMENT ON TABLE processos IS
    'Assunto burocrático que corre fora da BWS: aditivo, apostilamento, '
    'licença, protocolo. Ver app/apps/erp/ACOMPANHAMENTO.md.';


CREATE TABLE IF NOT EXISTS processo_andamentos (
    id           BIGSERIAL PRIMARY KEY,
    processo_id  BIGINT NOT NULL REFERENCES processos(id) ON DELETE CASCADE,
    -- A frase. "Liguei, está com o fulano do jurídico." É isto que alguém lê
    -- em trinta segundos ao assumir o assunto de outra pessoa.
    texto        TEXT NOT NULL,
    -- Os três opcionais que a mesma frase pode mudar de uma vez, sem abrir
    -- outro formulário.
    onde_esta    TEXT,
    previsao     DATE,
    situacao     TEXT,
    anexo_id     BIGINT REFERENCES anexos(id),
    por_id       BIGINT REFERENCES usuarios(id),
    em           TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ck_andamento_situacao CHECK (situacao IS NULL OR situacao IN (
        'RASCUNHO', 'PROTOCOLADO', 'EM_ANALISE', 'EXIGENCIA',
        'DEFERIDO', 'INDEFERIDO', 'ARQUIVADO'))
);

CREATE INDEX IF NOT EXISTS idx_andamentos_processo
    ON processo_andamentos (processo_id, em DESC);

COMMENT ON TABLE processo_andamentos IS
    'A frase que alguém escreveu ao ligar para o órgão, com data e autor. '
    'É o histórico que se lê em trinta segundos ao assumir um processo.';


-- ---------------------------------------------------------------------------
-- QUEM ENXERGA A TELA NOVA NO DIA DA VIRADA
--
-- Desde a migração 065 o perfil é CADASTRO, e seção que ninguém marcou vale
-- NADA — o padrão NEGAR do ERP. Está certo, e tem um efeito que precisa ser
-- tratado aqui: sem esta parte, a tela nova não abriria para NINGUÉM, nem para
-- o administrador, e o dono concluiria (com razão) que ela está quebrada.
--
-- A herança é a da AGENDA, e não a do painel de obras. Os dois motivos:
--
--   · quem trata agenda já faz este trabalho — perseguir obrigação com prazo e
--     terceiro envolvido é a mesma função, só que aqui com histórico;
--   · o painel de obras só existe no nível LER, e herdar dele daria
--     `ver_acompanhamento` sem `tocar_processo`: todo mundo entrando numa tela
--     onde ninguém consegue lançar nada.
--
-- Perfil que não trata agenda NÃO ganha nada, e quem quiser dar acesso marca
-- em Configurações › Perfis, que é onde essa decisão mora.
-- ---------------------------------------------------------------------------
INSERT INTO perfil_secoes (perfil_id, secao, nivel)
SELECT ps.perfil_id, 'obr_acompanhamento', ps.nivel
  FROM perfil_secoes ps
 WHERE ps.secao = 'obr_agenda'
   AND ps.nivel IN ('LER', 'EDITAR')
   AND NOT EXISTS (SELECT 1 FROM perfil_secoes x
                    WHERE x.perfil_id = ps.perfil_id
                      AND x.secao = 'obr_acompanhamento');
