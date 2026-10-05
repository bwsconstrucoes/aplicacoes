-- ============================================================================
-- Migração 082 — as seções "Ponto" nos perfis que já existem
--
-- O módulo de ponto eletrônico (app/apps/ponto, 03/10/2026) ganhou a gestão
-- dentro do ERP, com quatro seções novas no cadastro de perfis
-- (core/auth/secoes.py): pon_gestao, pon_dp, pon_competencia e pon_config.
--
-- Desde a migração 065 o perfil é CADASTRO, e seção que ninguém marcou vale
-- NADA. Sem esta migração a tela do ponto não abriria para ninguém — nem para o
-- administrador. Os perfis semeados pela 065 recebem exatamente o que o cargo
-- correspondente tem em código (permissoes.py), e há teste cobrando a
-- igualdade (`test_o_perfil_pronto_responde_igual_ao_cargo_antigo`).
--
-- Perfil criado à mão pelo dono NÃO ganha nada: quem decide o que ele abre é
-- quem o criou, em Configurações › Perfis.
-- ============================================================================
INSERT INTO perfil_secoes (perfil_id, secao, nivel)
SELECT p.id, v.secao, v.nivel
  FROM perfis p
  JOIN (VALUES
    ('Administrador',          'pon_gestao',      'EDITAR'),
    ('Administrador',          'pon_dp',          'EDITAR'),
    ('Administrador',          'pon_competencia', 'EDITAR'),
    ('Administrador',          'pon_config',      'EDITAR'),
    ('Diretor financeiro',     'pon_gestao',      'LER'),
    ('Gestor de obras',        'pon_gestao',      'EDITAR'),
    ('Supervisor de obras',    'pon_gestao',      'EDITAR'),
    ('Administrativo de obra', 'pon_gestao',      'LER'),
    ('Departamento pessoal',   'pon_gestao',      'EDITAR'),
    ('Departamento pessoal',   'pon_dp',          'EDITAR'),
    ('Departamento pessoal',   'pon_competencia', 'EDITAR'),
    ('Departamento pessoal',   'pon_config',      'EDITAR')
  ) AS v(perfil, secao, nivel) ON v.perfil = p.nome
ON CONFLICT (perfil_id, secao) DO NOTHING;
