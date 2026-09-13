-- ============================================================================
-- Migração 065 — o PERFIL vira cadastro, e a OBRA vira coisa do operador
--
-- O PEDIDO, do dono, em 13/09/2026, com uma reclamação junto ("não era pra
-- gente estar discutindo tanto isso repetidamente"):
--
--   "Eu crio um perfil de usuário. O perfil eu digo: esse perfil tem acesso a
--   isso, aquilo e aquilo outro. E o usuário está dentro daquele perfil. (…) O
--   operador, mais, ele não vai ter acesso a nada. Aí eu vou agregando ao
--   cadastro dele possibilidades: somente leitura de alguma área, leitura e
--   edição das áreas. Isso pra uma obra, pra várias obras, pra todas as obras."
--
-- O QUE MUDA, em uma frase: o que a pessoa pode fazer deixa de vir colado ao
-- NOME DO CARGO, escrito em código, e passa a ser um cadastro que ele edita.
-- E as obras deixam de depender do cargo: passam a ser do operador.
--
-- ⚠️ ESTA MIGRAÇÃO NÃO TIRA ACESSO DE NINGUÉM. Cada cargo de hoje vira um
-- perfil pronto, com exatamente as mesmas seções, e cada operador é apontado
-- para o perfil do cargo dele. Quem hoje enxerga todas as obras continua
-- enxergando. A partir daí o dono ajusta na tela — o que é justamente o que
-- ele pediu.
--
-- A coluna `perfil` (o cargo antigo) CONTINUA na tabela, e de propósito: o
-- código sobe para o Render antes do botão ser apertado, e a guarda de
-- permissão roda antes de toda rota. Tirar a coluna no mesmo passo derrubaria
-- o ERP nessa janela. Ela sai numa migração futura, quando nada mais a ler.
-- ============================================================================

CREATE TABLE IF NOT EXISTS perfis (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nome        TEXT        NOT NULL UNIQUE,
    descricao   TEXT,
    -- Perfil de sistema é o que nasceu desta migração, espelhando um cargo.
    -- Pode ser EDITADO à vontade; o que ele não pode é ser apagado, porque há
    -- operador apontando para ele e apagar deixaria gente sem perfil nenhum.
    de_sistema  BOOLEAN     NOT NULL DEFAULT FALSE,
    ativo       BOOLEAN     NOT NULL DEFAULT TRUE,
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    criado_por  BIGINT      REFERENCES usuarios(id)
);

-- O nível de cada seção dentro do perfil. O que NÃO estiver aqui é NADA — o
-- padrão, e a razão de um perfil novo não abrir porta nenhuma.
CREATE TABLE IF NOT EXISTS perfil_secoes (
    perfil_id   BIGINT NOT NULL REFERENCES perfis(id) ON DELETE CASCADE,
    secao       TEXT   NOT NULL,
    nivel       TEXT   NOT NULL CHECK (nivel IN ('LER', 'EDITAR')),
    PRIMARY KEY (perfil_id, secao)
);

ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS perfil_id BIGINT REFERENCES perfis(id);

-- AS OBRAS SÃO DO OPERADOR, e não do cargo. Quem não for marcado enxerga só
-- as obras associadas a ele: é o padrão NEGAR do ERP aplicado à pergunta que
-- o dono repetiu mais vezes.
--
-- A coluna aceita NULO, e isso é escolha: nulo quer dizer "ninguém disse", e
-- aí vale o cargo antigo — o mesmo princípio do perfil. O UPDATE logo abaixo
-- não deixa NENHUMA linha nula, então na prática a decisão fica escrita para
-- todo mundo; o nulo existe para o cadastro que nasceu antes desta migração
-- não virar "não enxerga nada" sem ninguém ter decidido isso.
ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS ve_todas_as_obras BOOLEAN;

CREATE INDEX IF NOT EXISTS idx_usuarios_perfil ON usuarios (perfil_id);

-- ---------------------------------------------------------------------------
-- Os perfis prontos, um por cargo de hoje, com as MESMAS seções.
-- ---------------------------------------------------------------------------
INSERT INTO perfis (nome, descricao, de_sistema)
VALUES
    ('Administrador',            'Acesso completo, incluindo configuração do sistema', TRUE),
    ('Diretor financeiro',       'Acesso completo ao financeiro, obras, pessoal e suprimentos', TRUE),
    ('Administrativo financeiro','Lança, aprova, paga e concilia', TRUE),
    ('Gestor de obras',          'Acompanha as obras dele de ponta a ponta', TRUE),
    ('Supervisor de obras',      'Lança e avaliza nas obras dele', TRUE),
    ('Administrativo de obra',   'Lança e acompanha o que é da obra dele', TRUE),
    ('Departamento pessoal',     'Colaboradores e despesas com pessoal', TRUE),
    ('Aprovador',                'Confirma e aprova lançamento', TRUE),
    ('Lançador',                 'Só lança', TRUE),
    ('Consulta',                 'Só olha', TRUE),
    ('Parceiro da obra',         'De fora da empresa: só olha a obra dele', TRUE)
ON CONFLICT (nome) DO NOTHING;

-- ---------------------------------------------------------------------------
-- As seções de cada perfil pronto. Espelham a tabela que estava em código.
-- ---------------------------------------------------------------------------
INSERT INTO perfil_secoes (perfil_id, secao, nivel)
SELECT p.id, v.secao, v.nivel
  FROM perfis p
  JOIN (VALUES
    -- ⚠️ ESTE BLOCO NÃO SE ESCREVE À MÃO. Ele é a tabela de cargos que
    -- estava em código, traduzida seção por seção — e a suíte confere
    -- isso com banco de verdade (tests/test_perfis_cadastro_banco.py):
    -- cada perfil semeado tem de responder EXATAMENTE o mesmo que o
    -- cargo respondia, ação por ação. A primeira versão foi digitada à
    -- mão e dava aprovação a quem só confirmava; foi essa conferência
    -- que pegou.
    -- Administrador
    ('Administrador','fin_lancar','EDITAR'),('Administrador','fin_titulos','EDITAR'),
    ('Administrador','fin_avalizar','EDITAR'),('Administrador','fin_aprovar','EDITAR'),
    ('Administrador','fin_pagar','EDITAR'),('Administrador','fin_conciliar','EDITAR'),
    ('Administrador','fin_receber','EDITAR'),('Administrador','fin_notas','EDITAR'),
    ('Administrador','fin_relatorios','LER'),('Administrador','obr_contratos','LER'),
    ('Administrador','obr_notas_emitidas','EDITAR'),('Administrador','obr_agenda','EDITAR'),
    ('Administrador','pes_despesas','EDITAR'),('Administrador','pes_colaboradores','EDITAR'),
    ('Administrador','sup_solicitar','EDITAR'),('Administrador','sup_comprar','EDITAR'),
    ('Administrador','sup_autorizar','EDITAR'),('Administrador','sup_cadastros','EDITAR'),
    ('Administrador','ger_arquivo','EDITAR'),('Administrador','ger_encaminhar','EDITAR'),
    ('Administrador','ger_equipe','LER'),('Administrador','adm_configurar','EDITAR'),
    ('Administrador','adm_operadores','EDITAR'),
    -- Diretor financeiro
    ('Diretor financeiro','fin_lancar','EDITAR'),('Diretor financeiro','fin_titulos','EDITAR'),
    ('Diretor financeiro','fin_avalizar','EDITAR'),('Diretor financeiro','fin_aprovar','EDITAR'),
    ('Diretor financeiro','fin_pagar','EDITAR'),('Diretor financeiro','fin_conciliar','EDITAR'),
    ('Diretor financeiro','fin_receber','EDITAR'),('Diretor financeiro','fin_notas','EDITAR'),
    ('Diretor financeiro','fin_relatorios','LER'),('Diretor financeiro','obr_contratos','LER'),
    ('Diretor financeiro','obr_notas_emitidas','EDITAR'),('Diretor financeiro','obr_agenda','EDITAR'),
    ('Diretor financeiro','pes_despesas','EDITAR'),('Diretor financeiro','pes_colaboradores','EDITAR'),
    ('Diretor financeiro','sup_solicitar','EDITAR'),('Diretor financeiro','sup_comprar','EDITAR'),
    ('Diretor financeiro','sup_autorizar','EDITAR'),('Diretor financeiro','sup_cadastros','EDITAR'),
    ('Diretor financeiro','ger_arquivo','EDITAR'),('Diretor financeiro','ger_encaminhar','EDITAR'),
    ('Diretor financeiro','ger_equipe','LER'),
    -- Administrativo financeiro
    ('Administrativo financeiro','fin_lancar','EDITAR'),('Administrativo financeiro','fin_titulos','EDITAR'),
    ('Administrativo financeiro','fin_aprovar','EDITAR'),('Administrativo financeiro','fin_pagar','EDITAR'),
    ('Administrativo financeiro','fin_conciliar','EDITAR'),('Administrativo financeiro','fin_receber','EDITAR'),
    ('Administrativo financeiro','fin_notas','EDITAR'),('Administrativo financeiro','fin_relatorios','LER'),
    ('Administrativo financeiro','obr_contratos','LER'),('Administrativo financeiro','obr_notas_emitidas','EDITAR'),
    ('Administrativo financeiro','obr_agenda','EDITAR'),('Administrativo financeiro','pes_despesas','EDITAR'),
    ('Administrativo financeiro','pes_colaboradores','LER'),('Administrativo financeiro','sup_solicitar','LER'),
    ('Administrativo financeiro','sup_cadastros','LER'),('Administrativo financeiro','ger_arquivo','EDITAR'),
    ('Administrativo financeiro','ger_encaminhar','EDITAR'),
    -- Gestor de obras
    ('Gestor de obras','fin_lancar','EDITAR'),('Gestor de obras','fin_avalizar','EDITAR'),
    ('Gestor de obras','fin_pagar','LER'),('Gestor de obras','fin_notas','LER'),
    ('Gestor de obras','fin_relatorios','LER'),('Gestor de obras','obr_contratos','LER'),
    ('Gestor de obras','obr_notas_emitidas','LER'),('Gestor de obras','obr_agenda','EDITAR'),
    ('Gestor de obras','pes_despesas','EDITAR'),('Gestor de obras','pes_colaboradores','LER'),
    ('Gestor de obras','sup_solicitar','EDITAR'),('Gestor de obras','sup_cadastros','LER'),
    ('Gestor de obras','ger_arquivo','EDITAR'),('Gestor de obras','ger_encaminhar','EDITAR'),
    -- Supervisor de obras
    ('Supervisor de obras','fin_lancar','EDITAR'),('Supervisor de obras','fin_avalizar','EDITAR'),
    ('Supervisor de obras','fin_pagar','LER'),('Supervisor de obras','fin_notas','LER'),
    ('Supervisor de obras','fin_relatorios','LER'),('Supervisor de obras','obr_agenda','EDITAR'),
    ('Supervisor de obras','pes_despesas','EDITAR'),('Supervisor de obras','pes_colaboradores','LER'),
    ('Supervisor de obras','sup_solicitar','EDITAR'),('Supervisor de obras','sup_cadastros','LER'),
    ('Supervisor de obras','ger_arquivo','LER'),('Supervisor de obras','ger_encaminhar','EDITAR'),
    -- Administrativo de obra
    ('Administrativo de obra','fin_lancar','EDITAR'),('Administrativo de obra','obr_agenda','EDITAR'),
    ('Administrativo de obra','pes_despesas','EDITAR'),('Administrativo de obra','pes_colaboradores','LER'),
    ('Administrativo de obra','sup_solicitar','EDITAR'),('Administrativo de obra','sup_cadastros','LER'),
    ('Administrativo de obra','ger_arquivo','LER'),('Administrativo de obra','ger_encaminhar','EDITAR'),
    -- Departamento pessoal
    ('Departamento pessoal','fin_lancar','EDITAR'),('Departamento pessoal','obr_agenda','EDITAR'),
    ('Departamento pessoal','pes_despesas','EDITAR'),('Departamento pessoal','pes_colaboradores','EDITAR'),
    ('Departamento pessoal','ger_arquivo','EDITAR'),('Departamento pessoal','ger_encaminhar','EDITAR'),
    -- Aprovador
    ('Aprovador','fin_aprovar','EDITAR'),('Aprovador','fin_pagar','LER'),
    ('Aprovador','obr_agenda','LER'),('Aprovador','ger_arquivo','LER'),
    ('Aprovador','ger_encaminhar','EDITAR'),
    -- Lançador
    ('Lançador','fin_lancar','EDITAR'),('Lançador','ger_encaminhar','EDITAR'),
    -- Consulta
    ('Consulta','fin_notas','LER'),('Consulta','obr_contratos','LER'),
    ('Consulta','obr_notas_emitidas','LER'),('Consulta','obr_agenda','LER'),
    ('Consulta','sup_solicitar','LER'),('Consulta','sup_cadastros','LER'),
    ('Consulta','ger_arquivo','LER'),
    -- Parceiro da obra
    ('Parceiro da obra','fin_relatorios','LER'),('Parceiro da obra','obr_agenda','LER'),
    ('Parceiro da obra','pes_despesas','LER'),('Parceiro da obra','pes_colaboradores','LER'),
    ('Parceiro da obra','sup_solicitar','LER'),('Parceiro da obra','sup_cadastros','LER'),
    ('Parceiro da obra','ger_arquivo','LER')
  ) AS v(perfil, secao, nivel) ON v.perfil = p.nome
ON CONFLICT (perfil_id, secao) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Cada operador aponta para o perfil do cargo dele. NINGUÉM PERDE ACESSO.
-- ---------------------------------------------------------------------------
UPDATE usuarios u SET perfil_id = p.id
  FROM perfis p
 WHERE u.perfil_id IS NULL
   AND p.nome = CASE u.perfil::text
       WHEN 'ADMIN'                THEN 'Administrador'
       WHEN 'DIRETOR_FINANCEIRO'   THEN 'Diretor financeiro'
       WHEN 'FINANCEIRO'           THEN 'Administrativo financeiro'
       WHEN 'GESTOR_OBRA'          THEN 'Gestor de obras'
       WHEN 'SUPERVISOR_OBRA'      THEN 'Supervisor de obras'
       WHEN 'ADMINISTRATIVO_OBRA'  THEN 'Administrativo de obra'
       WHEN 'DEPARTAMENTO_PESSOAL' THEN 'Departamento pessoal'
       WHEN 'APROVADOR'            THEN 'Aprovador'
       WHEN 'LANCADOR'             THEN 'Lançador'
       WHEN 'CONSULTA'             THEN 'Consulta'
       WHEN 'PARCEIRO'             THEN 'Parceiro da obra'
   END;

-- Quem hoje enxerga TODAS as obras continua enxergando: administrador,
-- diretoria e financeiro. Decisão do dono em 12/09/2026 — *"com exceção dos
-- perfis de diretoria e financeiro, o natural é visualizar somente as obras
-- associadas no cadastro do operador"*. Os demais ficam com FALSE — e note que
-- é FALSE ESCRITO, não padrão de coluna: depois desta linha ninguém fica com a
-- pergunta em aberto, e a tela mostra a decisão de cada pessoa.
UPDATE usuarios
   SET ve_todas_as_obras = (perfil::text IN ('ADMIN', 'DIRETOR_FINANCEIRO',
                                             'FINANCEIRO'))
 WHERE ve_todas_as_obras IS NULL;

-- ---------------------------------------------------------------------------
-- QUEM JÁ ENXERGAVA POR OBRA CONTINUA ENXERGANDO POR OBRA.
--
-- Até aqui, cinco cargos enxergavam pela obra por estarem numa lista em
-- código, e o campo "o que esta pessoa enxerga" do cadastro deles nem era
-- consultado — ficou em "só os lançamentos dela", o padrão da coluna. Com o
-- alcance saindo do cargo e indo para o cadastro, esse campo passa a valer:
-- sem esta linha, o supervisor acordaria amanhã vendo só o que ele mesmo
-- lançou, e ninguém entenderia por quê.
-- ---------------------------------------------------------------------------
UPDATE usuarios SET escopo_visao = 'OBRAS_DESIGNADAS'
 WHERE perfil::text IN ('SUPERVISOR_OBRA', 'PARCEIRO', 'GESTOR_OBRA',
                        'APROVADOR', 'CONSULTA');
