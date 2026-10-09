-- ===========================================================================
-- 053 — A TELA DE FATURAMENTO
--
-- O dono, 09/10/2026: *"numa nova tela no Análise de SPs, que a gente pode
-- chamar de Faturamento, eu quero fazer o controle de notas — ver faturamento,
-- fazer o download da nota, uma parte gráfica de evolução, poder fazer toda
-- essa gestão."* O desenho está em `app/apps/emissaonf/FATURAMENTO.md`.
--
-- A fonte é a aba "Base Faturamento" (planilha das notas), que o EMISSOR grava.
-- Aqui fica uma CÓPIA para a tela filtrar e somar sem ir ao Google a cada
-- clique — trazida pela tarefa "faturamento" do processo separado, inteira, a
-- cada carga (são poucos milhares de notas).
--
--   faturamento_nota — uma linha por nota. As colunas soltas são as que a tela
--                      filtra e soma; o resto da linha vai inteiro em `dados`.
--   faturamento_obra — a C. Diários ("Centro de Custo"), pelos DOIS códigos da
--                      obra: empresa, SCP, contrato, município, tributação. O
--                      dono: *"informação que vem da C. Diários não precisa
--                      entrar na base, a gente vai cruzar"*.
--
-- O código não lê nada daqui antes de este arquivo rodar (`tem_coluna`).
-- ===========================================================================
CREATE TABLE IF NOT EXISTS analisesps.faturamento_nota (
    nota_numero      TEXT PRIMARY KEY,
    nota_sequencial  TEXT NOT NULL DEFAULT '',
    data_emissao     DATE,
    competencia      TEXT NOT NULL DEFAULT '',
    status           TEXT NOT NULL DEFAULT '',
    obra_codigo      TEXT NOT NULL DEFAULT '',
    tomador_nome     TEXT NOT NULL DEFAULT '',
    valor_total      NUMERIC(14, 2),
    valor_liquido    NUMERIC(14, 2),
    valor_recebido   NUMERIC(14, 2),
    data_recebimento DATE,
    dados            JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS ix_analisesps_faturamento_emissao
    ON analisesps.faturamento_nota (data_emissao);

CREATE TABLE IF NOT EXISTS analisesps.faturamento_obra (
    codigo  TEXT PRIMARY KEY,          -- maiúsculo, sem espaço
    dados   JSONB NOT NULL DEFAULT '{}'::jsonb
);
