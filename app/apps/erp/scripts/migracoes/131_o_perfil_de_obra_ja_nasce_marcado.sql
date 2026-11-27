-- ============================================================================
-- 131 — O PERFIL "ADMINISTRATIVO DE OBRA" JÁ NASCE MARCADO (27/11/2026)
--
-- CONSERTO DE UM BURACO DA MIGRAÇÃO 130, e ele foi encontrado pelo dono no
-- primeiro uso, com a pergunta certa: *"eu entrei em uma determinada pessoa
-- para poder liberar o perfil e coloquei ele como administrativo de obra (…)
-- não aparece nenhuma caixinha marcada para esse perfil. Tá certo assim? Ou eu
-- ainda tenho que marcar? Mas não era para eu ter que marcar, né?"*
--
-- Não era mesmo. A 130 criou a coluna com padrão FALSE para TODOS os perfis —
-- inclusive o "Administrativo de obra", que é justamente o perfil que a
-- migração 065 criou a partir do cargo que SEMPRE levou ao portal
-- (`PERFIS_SO_PORTAL`). Resultado: quem é da obra deixou de ser desviado, e o
-- perfil dele abre **39 telas em 5 módulos** — telas que o desvio escondia.
--
-- Isto NÃO é uma decisão nova: é devolver o comportamento que existia antes
-- de 130. Quem quiser que essa gente use as telas de dentro agora tem onde
-- desmarcar, na tela do perfil.
--
-- Mexe só no perfil DE SISTEMA com esse nome (o que veio pronto). Perfil que o
-- dono criou com o mesmo nome não é alcançado, e perfil que ele já tiver
-- marcado à mão continua marcado.
-- ============================================================================
UPDATE perfis
   SET entra_pelo_portal = TRUE
 WHERE de_sistema
   AND nome = 'Administrativo de obra';
