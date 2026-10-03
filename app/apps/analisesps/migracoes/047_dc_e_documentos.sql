-- ===========================================================================
-- 047 — DESPESAS COM COLABORADORES (DC) E OS DOCUMENTOS DA FICHA
--
-- O dono, 03/10/2026: as solicitações de "despesa com colaboradores" do Pipefy
-- (que chegam à aba "Data" da planilha DC) passam a ter tela própria, no padrão
-- das outras folhas, e o arquivo do BeeVale/SomaPay sai daqui. E: *"planilha de
-- cadastro BeeVale, planilha de cadastro do Soma (…) a gente selecionar algumas
-- pessoas"* — o cadastro da SomaPay pede RG, nome da mãe, endereço…, que a
-- ficha tem e o sistema não guardava.
--
-- O código não lê nada daqui antes de este arquivo rodar (`tem_coluna` /
-- tabela inexistente = tela avisa).
-- ===========================================================================

-- Os campos de documento da ficha ("Dados Documentos"), num JSON: RG, emissão,
-- órgão, UF, nome da mãe, sexo, endereço. Só servem à planilha de cadastro.
ALTER TABLE analisesps.colaborador
    ADD COLUMN IF NOT EXISTS documentos TEXT NOT NULL DEFAULT '';

-- A seleção da tela DC: só as exceções (quem NÃO vai, ou a obra trocada), por
-- linha da aba Data (card + CPF + tipo de despesa).
CREATE TABLE IF NOT EXISTS analisesps.dc_ajuste (
    chave        TEXT PRIMARY KEY,
    pagar        BOOLEAN,
    obra         TEXT NOT NULL DEFAULT '',
    alterado_em  TIMESTAMPTZ NOT NULL DEFAULT now(),
    alterado_por TEXT NOT NULL DEFAULT ''
);

-- Cada geração de arquivos da DC (o "lote") e as linhas que entraram nela. É o
-- que esconde da tela o que já foi gerado, o que alimenta a SP do Pipefy e a
-- crítica de duplicidade. Excluir a geração em "Arquivos gerados" apaga o lote,
-- e as linhas voltam para a tela.
CREATE TABLE IF NOT EXISTS analisesps.dc_lote (
    id           SERIAL PRIMARY KEY,
    analise_id   INTEGER,
    criado_em    TIMESTAMPTZ NOT NULL DEFAULT now(),
    criado_por   TEXT NOT NULL DEFAULT '',
    total        NUMERIC(14, 2) NOT NULL DEFAULT 0,
    cards_movidos BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS ix_analisesps_dc_lote_analise
    ON analisesps.dc_lote (analise_id);

CREATE TABLE IF NOT EXISTS analisesps.dc_linha (
    id           SERIAL PRIMARY KEY,
    lote_id      INTEGER NOT NULL REFERENCES analisesps.dc_lote (id) ON DELETE CASCADE,
    chave        TEXT NOT NULL,
    card_id      TEXT NOT NULL DEFAULT '',
    cpf          TEXT NOT NULL DEFAULT '',
    nome         TEXT NOT NULL DEFAULT '',
    obra         TEXT NOT NULL DEFAULT '',
    conta        TEXT NOT NULL DEFAULT '',
    tipo_despesa TEXT NOT NULL DEFAULT '',
    categoria    TEXT NOT NULL DEFAULT '',
    record_id    TEXT NOT NULL DEFAULT '',
    carteira     TEXT NOT NULL DEFAULT '',
    valor        NUMERIC(12, 2) NOT NULL DEFAULT 0,
    destino      TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS ix_analisesps_dc_linha_chave
    ON analisesps.dc_linha (chave);
CREATE INDEX IF NOT EXISTS ix_analisesps_dc_linha_cpf
    ON analisesps.dc_linha (cpf);
