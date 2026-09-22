-- ============================================================================
-- 081 — A seção que libera a homologação da conta do credor
--
-- Achado ao simular a cadeia inteira em 22/09/2026: a regra `homologar_conta`
-- existia no sistema e NÃO TINHA QUEM A CHAMASSE — nem rota, nem botão. Num
-- banco novo isso travava o ERP: todo título pago por Pix ou TED nasce
-- BLOQUEADO enquanto a conta do credor não é homologada, e não havia como
-- homologar. O dinheiro parava sem saída.
--
-- A ação ganhou SEÇÃO PRÓPRIA em vez de entrar em "Pagamentos": homologar é
-- conferir PARA ONDE o dinheiro vai, e quem lança pode cadastrar essa conta.
-- Juntar as duas tiraria do dono a chance de dar uma sem a outra — e é essa
-- separação que segura o golpe da troca de conta bancária.
--
-- Os três perfis financeiros recebem a seção. Os demais ficam de fora de
-- propósito: obra, pessoal e compras não conferem destino de pagamento.
-- ============================================================================

INSERT INTO perfil_secoes (perfil_id, secao, nivel)
SELECT p.id, v.secao, v.nivel
  FROM perfis p
  JOIN (VALUES
    ('Administrador',             'fin_homologar_conta', 'EDITAR'),
    ('Diretor financeiro',        'fin_homologar_conta', 'EDITAR'),
    ('Administrativo financeiro', 'fin_homologar_conta', 'EDITAR')
  ) AS v(perfil, secao, nivel) ON v.perfil = p.nome
ON CONFLICT (perfil_id, secao) DO NOTHING;
