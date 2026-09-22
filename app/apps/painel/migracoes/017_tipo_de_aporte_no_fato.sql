-- 017 — O tipo de aporte passa a ser calculado UMA vez, na montagem do fato
--
-- 22/09/2026, o dono: "Tela montada em 126753 ms — 15 consultas ao banco,
-- 126743 ms delas", no bloco de Aportes e Dividendos do DRE.
--
-- Cada consulta do bloco decidia, linha a linha, se o lançamento era aporte:
-- tirar acento da categoria, testar dez expressões regulares e procurar "bws"
-- na contraparte — para 185 mil linhas, quinze vezes por tela. A decisão não
-- muda entre uma tela e outra; ela muda quando a base é refeita. Então passa a
-- ser feita ali, e guardada aqui.
--
-- '' (vazio) quer dizer "não é aporte"; NULL quer dizer "ainda não foi
-- calculado" — só nas linhas anteriores a esta migração, até o próximo
-- "Só refazer os números". Enquanto houver NULL, a consulta cai na conta
-- antiga para essa linha: lenta, mas certa.

ALTER TABLE painel.fato ADD COLUMN IF NOT EXISTS tipo_aporte TEXT;

CREATE INDEX IF NOT EXISTS ix_fato_tipo_aporte
    ON painel.fato (tipo_aporte)
 WHERE tipo_aporte IS NOT NULL AND tipo_aporte <> '';
