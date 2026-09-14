-- ============================================================================
-- Migração 069 — o número-índice ANCORADO no número oficial
--
-- O QUE ACONTECEU, em 14/09/2026, logo depois de publicar a 068:
--
--   "Apareceram os índices, mas os números estão diferentes do que eu costumo
--   ver. Veja o de 08/2026: 305,943822."
--
-- Ele está certo, e o número não estava errado: estava numa BASE diferente.
-- A 068 acumulou a série com **100 no mês mais antigo guardado** (01/2010),
-- porque o Banco Central republica só a VARIAÇÃO mensal do INCC-DI — a série
-- do número-índice da FGV é licenciada. Com essa base, 08/2026 dá 305,943822:
-- o custo da construção multiplicou por 3,059 desde janeiro de 2010.
--
-- O boletim que ele confere usa outra base, então mostra outro número para o
-- MESMO mês. A razão entre dois meses é idêntica nas duas bases — o reajuste
-- sempre saiu certo —, mas ninguém confere um número que não bate com o papel
-- que está na mão. E número que não bate vira desconfiança no sistema inteiro.
--
-- A SAÍDA: a pessoa informa UM número oficial, e a série toda se alinha
--
-- Esta tabela guarda esse ponto de referência: um mês e o número-índice que o
-- boletim (ou o contrato) publica para ele. O sistema recalcula a série
-- inteira a partir dali — para a frente multiplicando pelas variações, para
-- trás dividindo. As variações não mudam; o que muda é onde a régua começa.
--
-- Por que UMA âncora por índice, e não uma por mês: duas âncoras que não
-- fecham entre si (por erro de digitação, ou por serem de versões diferentes
-- do INCC) fariam a série ter dois trechos incompatíveis, e o fator entre dois
-- meses de lados diferentes sairia errado — com cara de certo. Uma só, e a
-- série inteira pendurada nela.
--
-- Sem âncora nenhuma, nada muda: continua 100 no mês mais antigo, como a 068.
-- ============================================================================

CREATE TABLE IF NOT EXISTS indices_ancora (
    codigo          TEXT PRIMARY KEY,
    competencia     DATE NOT NULL,
    numero_indice   NUMERIC(18, 6) NOT NULL CHECK (numero_indice > 0),
    -- De onde veio o número: "boletim FGV de agosto/2026", "contrato da obra
    -- tal". Escrito por quem digitou, para o próximo entender a régua.
    observacao      TEXT,
    definido_em     TIMESTAMPTZ NOT NULL DEFAULT now(),
    definido_por    BIGINT REFERENCES usuarios(id)
);

COMMENT ON TABLE indices_ancora IS
    'O ponto de referência do número-índice: um mês e o número oficial que o '
    'boletim publica para ele. A série inteira é recalculada a partir daqui. '
    'Sem linha, vale 100 no mês mais antigo guardado.';
