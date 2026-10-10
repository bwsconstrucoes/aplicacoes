-- ===========================================================================
-- 010 — A LISTA DAS LICENÇAS DA LEI, ATUALIZADA (10/10/2026)
--
-- O dono perguntou "da onde é que você tirou esses tipos de licença? (…) tudo
-- isso aqui é da lei?". Na conferência, dois itens da 006 estavam atrás da lei:
--
--   · PRÉ-NATAL (CLT art. 473, X): a Lei 14.457/2022 trocou "até 2 dias" por
--     "o tempo necessário para acompanhar em até 6 consultas ou exames durante
--     a gravidez". O limite passa de 2 para 6.
--   · PATERNIDADE: a Lei 15.371/2026 amplia aos poucos — 5 dias em 2026, 10 em
--     2027, 15 em 2028 e 20 em 2029. Em 2026 continua 5; a base legal passa a
--     dizer isso, e o DP sobe os dias na Configuração em janeiro de 2027.
--
-- Só mexe no que ninguém ajustou pela tela (atualizado_por vazio): o ajuste do
-- DP, que pode vir da convenção, vale mais que a correção daqui.
-- ===========================================================================

UPDATE ponto.tipos_licenca
   SET limite_ano = 6,
       nome = 'Acompanhar a esposa ou companheira em consulta ou exame da gravidez (até 6)',
       base_legal = 'CLT art. 473, X (Lei 14.457/2022)',
       atualizado_em = now()
 WHERE codigo = 'PRE_NATAL' AND atualizado_por IS NULL;

UPDATE ponto.tipos_licenca
   SET base_legal = 'CF/ADCT art. 10, § 1º e Lei 15.371/2026 (5 dias em 2026; 10 em 2027; 15 em 2028; 20 em 2029)',
       atualizado_em = now()
 WHERE codigo = 'PATERNIDADE' AND atualizado_por IS NULL;
