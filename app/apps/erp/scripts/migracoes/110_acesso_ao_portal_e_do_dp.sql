-- ===========================================================================
-- O DP LIBERA O ACESSO AO PORTAL
--
-- Decisão dele em 18/11/2026, respondendo à pergunta de quem deveria resolver
-- a fila de quem pede acesso pelo aplicativo do ponto: *"libera o DP também."*
--
-- O PROBLEMA QUE ISTO RESOLVE: a ação era `gerir_usuarios`, que é só do
-- ADMIN. Na prática, só ele próprio conseguia liberar — o DP, que é quem
-- trabalha a fila, não enxergava nem o cartão dela. Fila que só uma pessoa
-- resolve é fila que espera essa pessoa viajar.
--
-- ⚑ POR QUE NÃO DEI `gerir_usuarios` AO DP, que seria uma linha mais curta:
-- essa ação abre a tela de operadores INTEIRA — criar operador com qualquer
-- perfil (ADMIN incluído), marcar exceção de alçada de qualquer um, mexer no
-- acesso do financeiro. Ele pediu para o DP resolver UMA fila, não para o DP
-- virar administrador do sistema. Então a fila ganhou ação própria,
-- `liberar_acesso_portal`, e esta migração a põe na seção nova
-- "Pessoal › Acesso ao portal".
--
-- O que o DP passa a poder, e só isto: ver quem pediu, liberar (o acesso nasce
-- SEMPRE administrativo de obra, preso à obra em que a pessoa bate ponto) e
-- recusar com motivo. A rota IGNORA o perfil que venha do navegador para quem
-- não tem `gerir_usuarios` — senão o DP pediria "ADMIN" na mão e criaria um
-- administrador por uma fila de acesso de obra. Há teste disso.
--
-- ⚠️ SEM ESTA MIGRAÇÃO, o perfil pronto "Departamento pessoal" fica sem a
-- seção e o DP continua sem a fila, ainda que o código já esteja no ar — é a
-- razão de ela existir, e não só da tabela de permissões ter mudado.
-- ===========================================================================
INSERT INTO perfil_secoes (perfil_id, secao, nivel)
SELECT p.id, v.secao, v.nivel
  FROM perfis p
  JOIN (VALUES
    ('Administrador',        'pes_acesso_portal', 'EDITAR'),
    ('Departamento pessoal', 'pes_acesso_portal', 'EDITAR')
  ) AS v(perfil, secao, nivel) ON v.perfil = p.nome
ON CONFLICT (perfil_id, secao) DO NOTHING;
