-- ===========================================================================
-- 002 — Agenda de compromissos, feriados e o lote de trabalho
--
-- Três coisas que o Streamlit guardava em chave/valor dentro do SQLite local e
-- que aqui ganham tabela de verdade, porque passam a ser consultadas e
-- filtradas em vez de só lidas inteiras.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- Compromissos da agenda.
--
-- A origem continua sendo a aba "Agenda" da planilha — esta tabela é a cópia
-- local, refeita pela sincronização. Os campos são os mesmos da planilha, com
-- os mesmos nomes, para a ida e a volta continuarem óbvias.
--
-- `dia_mes` = 31 quer dizer "sempre o ÚLTIMO dia do mês", não o dia 31: é como
-- o original trata, e é o que faz um compromisso de fim de mês cair em 28 de
-- fevereiro em vez de sumir.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analisesps.agenda (
    id                 TEXT PRIMARY KEY,
    titulo             TEXT,
    descricao          TEXT,
    categoria          TEXT,
    data_base          TEXT,
    data_base_d        DATE,
    recorrencia        TEXT,
    dia_mes            TEXT,
    ajuste_dia_util    TEXT,
    alerta_dias_antes  TEXT,
    status             TEXT,
    concluido_em       TEXT,
    responsavel        TEXT,
    criado_por         TEXT,
    criado_em          TEXT,
    atualizado_em      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_agenda_data ON analisesps.agenda(data_base_d);
CREATE INDEX IF NOT EXISTS ix_agenda_cat  ON analisesps.agenda(categoria);


-- ---------------------------------------------------------------------------
-- Feriados que não são nacionais (estaduais e municipais).
--
-- Os nacionais são calculados — inclusive os móveis, que dependem da Páscoa.
-- Só os locais precisam ser informados, e vêm da aba "Feriados" da planilha.
-- Sem eles, um vencimento ajustado para "próximo dia útil" cairia num feriado
-- de Fortaleza e o pagamento atrasaria.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analisesps.feriados (
    dia     DATE PRIMARY KEY,
    nome    TEXT
);


-- ---------------------------------------------------------------------------
-- O LOTE de trabalho.
--
-- É o bloco de texto que o operador monta na tela de Lote: números de SP, e
-- linhas de texto que viram títulos de grupo. Ficava numa chave do SQLite
-- local; aqui fica no banco, e por isso sobrevive a trocar de computador.
--
-- Uma diferença consciente em relação ao original, e ela precisa estar escrita
-- porque um dia vai surpreender alguém: o lote é ÚNICO E COMPARTILHADO. No
-- Streamlit ele era do computador; aqui, duas pessoas que abrirem a tela veem
-- o mesmo lote, e a segunda a salvar sobrescreve a primeira. É de propósito —
-- a equipe trabalha sobre a mesma remessa de pagamentos, e dois lotes
-- paralelos seriam pior. A tela diz quem salvou por último e quando.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analisesps.lote (
    id             INTEGER PRIMARY KEY,     -- sempre 1: existe uma linha só
    conteudo       TEXT NOT NULL DEFAULT '',
    salvo_por      TEXT,
    salvo_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT lote_linha_unica CHECK (id = 1)
);

INSERT INTO analisesps.lote (id, conteudo) VALUES (1, '')
    ON CONFLICT (id) DO NOTHING;


-- ---------------------------------------------------------------------------
-- Referências do rateio: obras e categorias do Omie.
--
-- Vêm das abas "C. Diários" e "Plano Financeiro". São listas curtas que mudam
-- pouco, e só servem para montar as opções da tela de Ratear.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analisesps.referencias_rateio (
    tipo     TEXT NOT NULL,        -- 'obra' ou 'categoria'
    nome     TEXT NOT NULL,
    codigo   TEXT,
    PRIMARY KEY (tipo, nome)
);
