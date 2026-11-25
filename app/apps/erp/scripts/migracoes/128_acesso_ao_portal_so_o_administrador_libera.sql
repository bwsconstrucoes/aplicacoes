-- ===========================================================================
-- 128 — LIBERAR O ACESSO AO PORTAL É SÓ DO ADMINISTRADOR; O DP SÓ VÊ (25/11/2026)
--
-- Decisão do dono: *"o portal não é só para o DP (…) quem é para fazer a
-- liberação tem que ter acesso à parte de operadores (…) isso não é o DP quem
-- vai fazer."* E: *"o DP pode ser avisado. É bom que ele visualize (…) ele só
-- não vai ter o botão para confirmar."*
--
-- A seção "Acesso ao portal" passou a ter LER (ver a fila) e EDITAR (liberar).
-- O perfil pronto "Departamento pessoal" desce de EDITAR para LER. Quem
-- ajustou o perfil na tela continua com o que escolheu, exceto esta seção.
-- ===========================================================================
UPDATE perfil_secoes SET nivel = 'LER'
 WHERE secao = 'pes_acesso_portal'
   AND perfil_id IN (SELECT id FROM perfis WHERE nome = 'Departamento pessoal');
