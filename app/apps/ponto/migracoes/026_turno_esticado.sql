-- ===========================================================================
-- 026 — A SAÍDA DO TURNO ESTICADO (até o meio-dia)
--
-- O dono, 31/10/2026, num vigia que entrou às 15:55 do dia 23 e saiu às 11:52 do dia 24: "nesse dia o rapaz
-- precisou ficar mais tempo (…) acho que podemos deixar até o meio-dia". O código novo (escalas.dia_de_trabalho) já
-- leva para a véspera a batida entre 10h e meio-dia que FECHA o turno da noite que ficou aberto; esta migração acerta,
-- uma vez, o que já estava gravado:
--
--   batida entre 10h e 11h59 (hora de Fortaleza), no dia do relógio, de quem trabalha de noite na véspera (jornada
--   "vigia noturno", ou a escala própria que vale na véspera é da noite — 12x36 com a saída menor que a entrada, ou
--   semanal com o turno daquele dia passando da meia-noite), quando a véspera tem um número ÍMPAR de batidas antes
--   dela e a última a menos de 24 h — vai para a véspera.
--
-- A trava do "ímpar" é o que separa a SAÍDA esticada da ENTRADA de um turno longo de fim de semana (11:59 de domingo).
-- Só muda `data_referencia` (a hora, a obra e o hash encadeado ficam — Portaria 671). Mês FECHADO não é tocado.
-- ===========================================================================

WITH m AS (
    SELECT mc.id, mc.colaborador_id, mc.timestamp_servidor AS ts,
           (mc.timestamp_servidor AT TIME ZONE 'America/Fortaleza')::date - 1 AS anterior,
           COALESCE(cc.tipo_jornada, 'PADRAO_4') AS jornada
      FROM ponto.marcacoes mc
      LEFT JOIN ponto.colaborador_config cc ON cc.colaborador_id = mc.colaborador_id
     WHERE EXTRACT(HOUR FROM mc.timestamp_servidor AT TIME ZONE 'America/Fortaleza') BETWEEN 10 AND 11
       AND mc.data_referencia = (mc.timestamp_servidor AT TIME ZONE 'America/Fortaleza')::date
       AND mc.status <> 'REJEITADA'
), vig AS (
    SELECT m.*, (SELECT ce.escala_id FROM ponto.colaborador_escalas ce
                  WHERE ce.colaborador_id = m.colaborador_id AND ce.vigencia_inicio <= m.anterior
                  ORDER BY ce.vigencia_inicio DESC LIMIT 1) AS escala_id
      FROM m
), noite AS (
    SELECT v.* FROM vig v LEFT JOIN ponto.escalas e ON e.id = v.escala_id
     WHERE v.jornada = 'VIGIA_NOTURNO_2'
        OR (e.tipo = 'CICLO_12X36' AND e.ciclo_saida <= e.ciclo_entrada)
        OR (e.tipo = 'SEMANAL' AND EXISTS (
               SELECT 1 FROM jsonb_array_elements(
                        COALESCE(e.semana -> ((EXTRACT(ISODOW FROM v.anterior)::int - 1)::text), '[]'::jsonb)) p
                WHERE p ->> 1 <= p ->> 0))
), aberta AS (
    SELECT n.id, n.anterior FROM noite n
     WHERE (SELECT count(*) FROM ponto.marcacoes x
             WHERE x.colaborador_id = n.colaborador_id AND x.data_referencia = n.anterior
               AND x.status <> 'REJEITADA' AND x.timestamp_servidor < n.ts) % 2 = 1
       AND (SELECT max(x.timestamp_servidor) FROM ponto.marcacoes x
             WHERE x.colaborador_id = n.colaborador_id AND x.data_referencia = n.anterior
               AND x.status <> 'REJEITADA' AND x.timestamp_servidor < n.ts) >= n.ts - INTERVAL '24 hours'
)
UPDATE ponto.marcacoes mc
   SET data_referencia = a.anterior
  FROM aberta a
 WHERE mc.id = a.id
   AND NOT EXISTS (SELECT 1 FROM ponto.competencias k
                    WHERE k.situacao = 'FECHADA'
                      AND k.competencia IN (date_trunc('month', a.anterior)::date,
                                            date_trunc('month', a.anterior + 1)::date));
