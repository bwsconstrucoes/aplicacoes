-- ===========================================================================
-- 108 — A PESSOA PEDE O ACESSO AO PORTAL PELO PONTO (17/11/2026)
--
-- Desenho dele, com todas as letras:
--
--   "E que tal a partir do ponto eu requerer a liberação do acesso ao
--    portal? (…) Ela já tem o cadastro no ponto, e lá pelo ponto ela vai
--    informar o e-mail dela, na hora que ela pediu acesso ao portal."
--
-- O problema que isto resolve: hoje o acesso ao ERP/portal só nasce se
-- alguém do DP souber o e-mail da pessoa e for digitar na ficha dela. O
-- e-mail quase nunca está no cadastro — e o DP não tem como adivinhar. A
-- pessoa, essa sim, sabe o próprio e-mail; e ela já entra todo dia no ponto
-- com CPF e PIN, que é uma autenticação de verdade.
--
-- Então o pedido nasce do lado de quem tem a informação, e a decisão continua
-- do lado de quem tem a alçada. Esta tabela é só a FILA entre os dois.
--
-- ⚑ POR QUE NO SCHEMA `public` (do ERP) E NÃO NO `ponto`: quem decide é o
-- ERP, e o resultado da decisão é uma linha em `usuarios` — tabela do ERP.
-- Pôr a fila no ponto obrigaria a apertar DOIS botões de migração (o do ERP e
-- o do ponto) para uma coisa só funcionar, e esse é o tipo de detalhe que
-- deixa metade do recurso no ar sem ninguém entender por quê. O ponto já lê e
-- escreve `public.<tabela>` por desenho (ver `app/apps/ponto/db.py`).
--
-- ⚑ `ON DELETE CASCADE` no colaborador é DE PROPÓSITO, e não descuido: um
-- pedido de acesso não é movimento da pessoa no sistema. Com a cascata, ele
-- sai junto quando o cadastro é apagado e NÃO vira mais um impedimento na
-- exclusão (`core/cadastros/pessoas.remover_cadastro` recusa apagar quem tem
-- referência sem cascata — era isso que travava a limpeza da importação).
-- ===========================================================================

CREATE TABLE IF NOT EXISTS portal_pedidos_de_acesso (
    id              BIGSERIAL PRIMARY KEY,
    colaborador_id  BIGINT      REFERENCES colaboradores(id) ON DELETE CASCADE,
    -- Copiados no momento do pedido, de propósito: o DP precisa saber QUEM
    -- pediu mesmo que o cadastro mude de nome ou de obra depois.
    cpf             TEXT        NOT NULL,
    nome            TEXT,
    email           TEXT        NOT NULL,
    telefone        TEXT,
    -- A obra de onde a pessoa pede, para o acesso nascer enxergando o lugar
    -- onde ela trabalha. Vem do ponto (onde ela bate), não de digitação.
    obra_id         BIGINT      REFERENCES obras(id) ON DELETE SET NULL,
    -- PONTO (pediu pelo celular) ou DP (o próprio DP abriu a fila).
    origem          TEXT        NOT NULL DEFAULT 'PONTO',
    -- PENDENTE · LIBERADO · RECUSADO
    status          TEXT        NOT NULL DEFAULT 'PENDENTE',
    motivo          TEXT,
    usuario_id      BIGINT      REFERENCES usuarios(id) ON DELETE SET NULL,
    decidido_por    BIGINT      REFERENCES usuarios(id) ON DELETE SET NULL,
    decidido_em     TIMESTAMPTZ,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- UM PEDIDO PENDENTE POR PESSOA. Sem isto, quem não vê resposta no mesmo dia
-- aperta o botão de novo — e o DP abre a tela com a mesma pessoa sete vezes.
CREATE UNIQUE INDEX IF NOT EXISTS ux_portal_pedidos_pendente
    ON portal_pedidos_de_acesso (colaborador_id)
 WHERE status = 'PENDENTE' AND colaborador_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_portal_pedidos_status
    ON portal_pedidos_de_acesso (status, criado_em DESC);

-- ===========================================================================
-- E A SEÇÃO "SALÁRIO E PAGAMENTO" NOS PERFIS PRONTOS
--
-- Pedido dele em 17/11/2026, sobre o administrativo de obra: *"visão restrita
-- é os dados básicos mesmo (…) Visualizar os arquivos da documentação da
-- pessoa, não."*
--
-- A ação `ver_remuneracao` nasceu para isso: quem precisa saber QUEM está na
-- obra e em que etapa o cadastro dela está não precisa saber QUANTO cada um
-- ganha. Ela foi dada a todo mundo que já via a tela de Pessoal, MENOS o
-- administrativo de obra — ninguém perde o que tinha.
--
-- ⚑ POR QUE ISTO É MIGRAÇÃO, E NÃO SÓ CÓDIGO: desde a migração 065 quem decide
-- é o PERFIL CADASTRADO, e não o cargo escrito no código. Ação nova que não
-- entra nos perfis prontos fica fechada para todo mundo em silêncio — quem
-- acusa é o teste que compara o perfil do banco com o cargo
-- (`test_perfis_cadastro_banco.py`), e ele acusou.
--
-- ⚠️ O NOME DO PERFIL TEM DE SER EXATO. "Parceiro da obra", não "Parceiro":
-- INSERT com JOIN por nome não reclama quando o nome está errado, só não
-- insere nada (precedente na migração 092).
--
-- "LER" e não "EDITAR" de propósito: a seção é sobre ENXERGAR o número. Quem
-- MUDA salário muda o cadastro, e isso é `editar_colaboradores`.
-- ===========================================================================
INSERT INTO perfil_secoes (perfil_id, secao, nivel)
SELECT p.id, v.secao, v.nivel
  FROM perfis p
  JOIN (VALUES
    ('Administrador',             'pes_remuneracao', 'LER'),
    ('Diretor financeiro',        'pes_remuneracao', 'LER'),
    ('Administrativo financeiro', 'pes_remuneracao', 'LER'),
    ('Gestor de obras',           'pes_remuneracao', 'LER'),
    ('Supervisor de obras',       'pes_remuneracao', 'LER'),
    ('Departamento pessoal',      'pes_remuneracao', 'LER'),
    ('Parceiro da obra',          'pes_remuneracao', 'LER')
    -- ⚑ "Administrativo de obra" FICA DE FORA. É o ponto do pedido dele.
  ) AS v(perfil, secao, nivel) ON v.perfil = p.nome
ON CONFLICT (perfil_id, secao) DO NOTHING;

-- ---------------------------------------------------------------------------
-- E O ARQUIVO DE DOCUMENTOS SAI DO ADMINISTRATIVO DE OBRA
--
-- A outra metade do mesmo pedido, dita com todas as letras: *"Tirar o arquivo
-- desse perfil, sim. Visualizar os arquivos da documentação da pessoa, não."*
--
-- `ger_arquivo` é o acervo da empresa — contrato, certidão, ART — e junto com
-- ele vinha o acesso aos documentos arquivados na ficha das pessoas (RG, CNH,
-- carteira de trabalho). Quem administra uma obra não precisa de nenhum dos
-- dois para abrir pedido e acompanhar quem está lá.
--
-- ⚠️ ISTO TIRA ACESSO DE QUEM TEM, e é o único lugar deste arquivo que tira.
-- Está aqui porque foi pedido explicitamente. Se alguém com esse perfil
-- precisar do arquivo, a alçada por PESSOA resolve (migração 032, tabela
-- `usuario_permissoes`): concede-se a ela sem mexer no cargo de todos.
-- ---------------------------------------------------------------------------
DELETE FROM perfil_secoes
 WHERE secao = 'ger_arquivo'
   AND perfil_id IN (SELECT id FROM perfis WHERE nome = 'Administrativo de obra');
