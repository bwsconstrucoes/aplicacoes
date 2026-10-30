-- ===========================================================================
-- 022 — O TURNO DA NOITE NO PRIMEIRO DIA DA ESCALA
--
-- O dono, 31/10/2026, num vigia do 12x36 com a escala própria a partir de 16/10 (o começo do ponto): o 07:01 do
-- dia 16 é a saída do turno que começou no dia 15 — e ficou no dia 16 ("descanso · batida faltando"), porque, para
-- a véspera, sem escala própria, valia a escala padrão da empresa (de dia). A 021 não pegou esse caso pelo mesmo
-- motivo. O código novo (escalas.dia_de_trabalho) já decide certo; esta migração acerta, uma vez, o que ficou:
--
--   batida antes das 10h (hora de Fortaleza), de quem NÃO está marcado como vigia noturno, sem nenhuma escala
--   própria na véspera, e com uma escala própria que COMEÇA no próprio dia da batida e é da noite (12x36 com a
--   saída menor que a entrada; semanal com o turno da véspera passando da meia-noite, ou todos os turnos de
--   noite) — vai para a véspera.
--
-- Só muda `data_referencia` (a hora, a obra e o hash encadeado ficam — Portaria 671). Mês FECHADO não é tocado.
-- A véspera costuma ser de antes do ponto valer: a batida simplesmente deixa de aparecer como solta no primeiro dia.
-- ===========================================================================

WITH m AS (
    SELECT mc.id, mc.colaborador_id,
           (mc.timestamp_servidor AT TIME ZONE 'America/Fortaleza')::date AS dia,
           (mc.timestamp_servidor AT TIME ZONE 'America/Fortaleza')::date - 1 AS anterior
      FROM ponto.marcacoes mc
      LEFT JOIN ponto.colaborador_config cc ON cc.colaborador_id = mc.colaborador_id
     WHERE EXTRACT(HOUR FROM mc.timestamp_servidor AT TIME ZONE 'America/Fortaleza') < 10
       AND mc.data_referencia = (mc.timestamp_servidor AT TIME ZONE 'America/Fortaleza')::date
       AND COALESCE(cc.tipo_jornada, 'PADRAO_4') <> 'VIGIA_NOTURNO_2'
), com_escala AS (
    SELECT m.id, m.anterior, ce.escala_id
      FROM m
      JOIN ponto.colaborador_escalas ce ON ce.colaborador_id = m.colaborador_id AND ce.vigencia_inicio = m.dia
     WHERE NOT EXISTS (SELECT 1 FROM ponto.colaborador_escalas v
                        WHERE v.colaborador_id = m.colaborador_id AND v.vigencia_inicio <= m.anterior)
), noite AS (
    SELECT c.id, c.anterior
      FROM com_escala c JOIN ponto.escalas e ON e.id = c.escala_id
     WHERE (e.tipo = 'CICLO_12X36' AND e.ciclo_saida <= e.ciclo_entrada)
        OR (e.tipo = 'SEMANAL' AND (
               EXISTS (SELECT 1 FROM jsonb_array_elements(
                                COALESCE(e.semana -> ((EXTRACT(ISODOW FROM c.anterior)::int - 1)::text), '[]'::jsonb)) p
                        WHERE p ->> 1 <= p ->> 0)
            OR (EXISTS (SELECT 1 FROM jsonb_each(e.semana) d, jsonb_array_elements(d.value) p)
                AND NOT EXISTS (SELECT 1 FROM jsonb_each(e.semana) d, jsonb_array_elements(d.value) p
                                 WHERE p ->> 1 > p ->> 0))))
)
UPDATE ponto.marcacoes mc
   SET data_referencia = n.anterior
  FROM noite n
 WHERE mc.id = n.id
   AND NOT EXISTS (SELECT 1 FROM ponto.competencias k
                    WHERE k.situacao = 'FECHADA'
                      AND k.competencia IN (date_trunc('month', n.anterior)::date,
                                            date_trunc('month', n.anterior + 1)::date));
