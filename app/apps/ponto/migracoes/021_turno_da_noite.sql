-- ===========================================================================
-- 021 — O TURNO DA NOITE: A SAÍDA DA MADRUGADA VOLTA PARA O DIA DA ENTRADA
--
-- O dono, 30/10/2026, sobre os vigias na escala 12x36 das 17h às 6h: "se eu já estou dizendo que é
-- vigia noturno, o dia que ele inicia de tarde (…) é para ser considerado o horário de entrada dele.
-- E amanhã, no dia seguinte, é de saída."
--
-- Até aqui, só a jornada "vigia noturno" (na janela da pessoa) levava a batida da madrugada para o
-- dia anterior. Com a escala da noite e a jornada padrão, a saída das 6h ficou gravada no dia
-- seguinte, e o turno se partiu em dois. O código novo decide pela ESCALA (escalas.dia_de_trabalho);
-- esta migração acerta, uma vez, as batidas que já foram gravadas no dia errado — com a mesma regra:
--
--   batida antes das 10h (hora de Fortaleza), de quem NÃO está marcado como vigia noturno (esses
--   já estão certos), cuja escala no dia anterior tem o turno passando da meia-noite:
--     - 12x36 com a saída menor que a entrada; ou
--     - semanal com o turno daquele dia da semana passando da meia-noite, ou com TODOS os turnos
--       passando (a escala só de noite).
--   A escala do dia anterior é a vigência própria mais recente; sem ela, a escala padrão da empresa.
--
-- Só muda `data_referencia` — a hora, a obra e o hash encadeado ficam como estão (Portaria 671; o
-- hash não inclui o dia de trabalho). Mês FECHADO não é tocado.
-- ===========================================================================

WITH m AS (
    SELECT mc.id, mc.colaborador_id,
           (mc.timestamp_servidor AT TIME ZONE 'America/Fortaleza')::date - 1 AS anterior
      FROM ponto.marcacoes mc
      LEFT JOIN ponto.colaborador_config cc ON cc.colaborador_id = mc.colaborador_id
     WHERE EXTRACT(HOUR FROM mc.timestamp_servidor AT TIME ZONE 'America/Fortaleza') < 10
       AND mc.data_referencia = (mc.timestamp_servidor AT TIME ZONE 'America/Fortaleza')::date
       AND COALESCE(cc.tipo_jornada, 'PADRAO_4') <> 'VIGIA_NOTURNO_2'
), com_escala AS (
    SELECT m.id, m.anterior,
           COALESCE(
               (SELECT ce.escala_id FROM ponto.colaborador_escalas ce
                 WHERE ce.colaborador_id = m.colaborador_id AND ce.vigencia_inicio <= m.anterior
                 ORDER BY ce.vigencia_inicio DESC LIMIT 1),
               CASE WHEN NOT EXISTS (SELECT 1 FROM ponto.colaborador_escalas ce
                                      WHERE ce.colaborador_id = m.colaborador_id
                                        AND ce.vigencia_inicio <= m.anterior)
                    THEN (SELECT e.id FROM ponto.parametros pa JOIN ponto.escalas e
                            ON pa.valor ~ '^[0-9]+$' AND e.id = pa.valor::bigint AND e.ativo
                           WHERE pa.chave = 'escalas.padrao') END
           ) AS escala_id
      FROM m
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
