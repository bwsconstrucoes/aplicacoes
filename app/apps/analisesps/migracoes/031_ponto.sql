-- ===========================================================================
-- 031 — O PONTO, DIA POR DIA
--
-- ⚠️ É O GARGALO DE TUDO. Sem o ponto não há total por obra, não há diária, não
-- há apropriação — e sem apropriação não há arquivo de pagamento. Todo o resto
-- da folha espera por esta tabela.
--
-- DE ONDE VEM: a API do Mobponto, endpoint `FOLHA_BWS_EXCEL`, paginada por
-- mês/ano. O contrato foi lido dos Apps Script que o dono mandou (não é
-- suposição):
--
--     GET .../endpoint.php?type_data=FOLHA_BWS_EXCEL&status=false
--                          &mes=<M>&ano=<AAAA>&pagina=<N>
--     → {"result": {"total_paginas": N,
--                   "funcionarios": [{"cpf", "nome",
--                                     "relatorio": [{"dia", "matricula", …}]}]}}
--
-- ⚠️ OS CAMPOS DE CADA DIA SÃO DINÂMICOS, e o próprio script do dono os descobre
-- em tempo de execução (`Object.keys` do primeiro dia). Eu NÃO os conheço — e
-- inventar nome de campo aqui decidiria em qual obra cai o salário de 500
-- pessoas com base num palpite.
--
-- Então a tabela guarda DUAS coisas:
--   1. `campos`  — o dia inteiro como veio, em JSON (TEXTO, seguindo a regra da
--                  casa: JSONB só quando se procura dentro, e aqui isto é
--                  matéria-prima e material de diagnóstico);
--   2. as colunas RESOLVIDAS — hoje só a data. As marcações e a obra entram
--      quando o nome dos campos for conhecido, no primeiro carregamento de
--      verdade, e a tela mostra os nomes que vieram.
--
-- Uma migração depois preenche as colunas resolvidas a partir do que já está
-- guardado — não é preciso recarregar nada da API.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS analisesps.ponto_carga (
    id            SERIAL PRIMARY KEY,
    ano           INTEGER NOT NULL,
    mes           INTEGER NOT NULL CHECK (mes BETWEEN 1 AND 12),
    -- Quantas páginas a API disse que havia, e quantas foram lidas: é o que
    -- permite dizer "veio pela metade" em vez de mostrar um total a menos como
    -- se fosse o certo.
    paginas       INTEGER NOT NULL DEFAULT 0,
    paginas_lidas INTEGER NOT NULL DEFAULT 0,
    pessoas       INTEGER NOT NULL DEFAULT 0,
    dias          INTEGER NOT NULL DEFAULT 0,
    -- Os nomes dos campos que vieram em cada dia, separados por "|". É a
    -- descoberta que destrava o mapeamento das marcações e da obra.
    campos_vistos TEXT NOT NULL DEFAULT '',
    avisos        TEXT NOT NULL DEFAULT '',
    carregado_em  TIMESTAMPTZ NOT NULL DEFAULT now(),
    carregado_por TEXT NOT NULL DEFAULT ''
);

-- UMA CARGA POR COMPETÊNCIA: recarregar substitui. Duas cargas de 08/2026
-- deixariam qualquer contagem de dias ambígua.
CREATE UNIQUE INDEX IF NOT EXISTS ix_analisesps_ponto_carga_competencia
    ON analisesps.ponto_carga (ano, mes);

CREATE TABLE IF NOT EXISTS analisesps.ponto_dia (
    id        BIGSERIAL PRIMARY KEY,
    carga_id  INTEGER NOT NULL
              REFERENCES analisesps.ponto_carga (id) ON DELETE CASCADE,
    cpf       TEXT NOT NULL DEFAULT '',
    nome      TEXT NOT NULL DEFAULT '',
    -- A data do dia, já convertida. NULA quando o campo `dia` não deu para ler —
    -- e aí a linha continua guardada, com o JSON, para alguém entender por quê.
    data      DATE,
    matricula TEXT NOT NULL DEFAULT '',
    campos    TEXT NOT NULL DEFAULT '',
    -- RESOLVIDAS DEPOIS, quando o nome dos campos for conhecido. Ficam nulas
    -- agora, e é melhor assim: coluna vazia é pergunta aberta; coluna preenchida
    -- por palpite é resposta errada com cara de certa.
    obra      TEXT,
    presenca  TEXT,
    falta     TEXT
);

-- As duas perguntas da apropriação: "os dias desta pessoa neste período" e
-- "quantos dias esta obra teve".
CREATE INDEX IF NOT EXISTS ix_analisesps_ponto_dia_pessoa
    ON analisesps.ponto_dia (carga_id, cpf, data);
CREATE INDEX IF NOT EXISTS ix_analisesps_ponto_dia_obra
    ON analisesps.ponto_dia (carga_id, obra) WHERE obra IS NOT NULL;
