-- ===========================================================================
-- 060 — A pergunta que chega sozinha
--
-- O PEDIDO, do dono, com as palavras dele: *"toda segunda-feira me manda
-- determinado tipo de informação. Aí a própria [IA] agendar essa necessidade
-- minha e fazer aquela ação executar e me mandar. Isso é muito poderoso."*
--
-- O QUE SE GUARDA AQUI, E POR QUE NÃO É A FRASE.
-- Guarda-se a CONSULTA: a chave da pergunta e os filtros dela. Se o sistema
-- reinterpretasse a frase toda segunda, o relatório mudaria de critério
-- sozinho — e aí não dá para comparar uma segunda com a outra, que é
-- justamente para o que ele serve. O que ficou combinado é o que roda.
--
-- AS TRÊS COLUNAS QUE PARECEM DETALHE E SÃO O CORAÇÃO:
--
--   `usuario_id`  — de QUEM recebe. O relatório roda com a permissão dela,
--                   não com a de quem criou. Sem isso, agendar um relatório
--                   para o gestor de uma obra mandaria a ele o número da
--                   empresa inteira.
--   `so_se_houver`— relatório que chega igual todo mês vira spam e para de ser
--                   lido. Marcado, ele só sai quando há o que mandar.
--   `ultimo_total`— o número da rodada anterior. "R$ 340 mil a pagar (era R$
--                   280 mil)" é gestão; "R$ 340 mil" sozinho é ruído.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS perguntas_agendadas (
    id              BIGSERIAL PRIMARY KEY,
    usuario_id      BIGINT NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    criado_por      BIGINT REFERENCES usuarios(id),

    -- A consulta combinada, não a frase.
    chave           TEXT   NOT NULL,
    parametros      JSONB  NOT NULL DEFAULT '{}'::jsonb,
    titulo          TEXT   NOT NULL,

    -- Quando. `dia` é o dia da semana (0 = segunda) na frequência SEMANAL, e
    -- o dia do mês na MENSAL; na DIARIA não é usado.
    frequencia      TEXT   NOT NULL,
    dia             SMALLINT,

    canal           TEXT   NOT NULL DEFAULT 'TELEGRAM',
    so_se_houver    BOOLEAN NOT NULL DEFAULT FALSE,
    ativa           BOOLEAN NOT NULL DEFAULT TRUE,

    -- A memória da rodada anterior, que é o que permite o "(era …)".
    ultima_rodada   DATE,
    ultimo_total    NUMERIC(16, 2),
    ultimas_linhas  INTEGER,
    ultimo_erro     TEXT,

    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE perguntas_agendadas DROP CONSTRAINT IF EXISTS ck_agendada_frequencia;
ALTER TABLE perguntas_agendadas ADD CONSTRAINT ck_agendada_frequencia
    CHECK (frequencia IN ('DIARIA', 'SEMANAL', 'MENSAL'));

ALTER TABLE perguntas_agendadas DROP CONSTRAINT IF EXISTS ck_agendada_canal;
ALTER TABLE perguntas_agendadas ADD CONSTRAINT ck_agendada_canal
    CHECK (canal IN ('TELEGRAM', 'EMAIL'));

-- A mesma pessoa não precisa do mesmo relatório duas vezes. A trava é do
-- BANCO porque dois cliques seguidos no botão chegam juntos, e aí conferir
-- antes no Python não adianta.
--
-- `WHERE ativa` é a parte que importa: a trava vale só entre os LIGADOS.
-- Quem desligou um relatório e quer de volta tem de conseguir — sem isso, o
-- desligar viraria uma porta que só abre para fora.
CREATE UNIQUE INDEX IF NOT EXISTS idx_agendada_sem_repetir
    ON perguntas_agendadas (usuario_id, chave, parametros)
    WHERE ativa;

CREATE INDEX IF NOT EXISTS idx_agendada_ativa
    ON perguntas_agendadas (ativa, frequencia);

COMMENT ON TABLE perguntas_agendadas IS
  'Perguntas que o sistema responde sozinho e manda para quem pediu. Roda com '
  'a permissão de quem RECEBE (usuario_id), nunca com a de quem criou.';
