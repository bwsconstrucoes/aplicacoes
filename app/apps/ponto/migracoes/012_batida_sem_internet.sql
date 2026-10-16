-- ===========================================================================
-- 012 — A BATIDA SEM INTERNET NO PONTO DA OBRA
--
-- Pedido do dono, 16/10/2026: "imagina que a gente está num local que não tem
-- internet". O ponto da obra guarda a batida (foto, localização e hora) e manda
-- quando a conexão volta. Decisões dele: reconhecer por CPF e QR pela lista que
-- o aparelho baixa antes; só no ponto da obra.
--
-- A HORA DA BATIDA é a de quando a pessoa bateu (`timestamp_servidor`, a mesma
-- coluna de sempre — o espelho, o cálculo e o AFD continuam lendo dela); a hora
-- em que o servidor RECEBEU fica em `recebida_em`. O NSR é dado na chegada: ele é
-- a ordem do REGISTRO, não do relógio (o mesmo princípio da batida incluída por
-- ajuste). A Portaria 671 prevê a marcação off-line no REP-P com as duas horas.
--
-- `id_no_aparelho` é o número que o aparelho deu à batida: se a resposta se
-- perder e ele mandar de novo, a segunda vez devolve a primeira — não duplica.
-- ===========================================================================

ALTER TABLE ponto.marcacoes ADD COLUMN IF NOT EXISTS sem_internet BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE ponto.marcacoes ADD COLUMN IF NOT EXISTS recebida_em TIMESTAMPTZ;
ALTER TABLE ponto.marcacoes ADD COLUMN IF NOT EXISTS id_no_aparelho TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS marcacoes_id_no_aparelho_unico
    ON ponto.marcacoes (dispositivo_id, id_no_aparelho)
 WHERE id_no_aparelho IS NOT NULL;
