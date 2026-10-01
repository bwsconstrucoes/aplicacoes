-- ===========================================================================
-- 043 — A FOLHA ANALÍTICA DA CONTABILIDADE, E A DATA DE NASCIMENTO NO CADASTRO
--
-- O dono, 01/10/2026: "a contabilidade nos envia dois arquivos, um analítico e
-- um resumido. O que a gente importou é o resumido. (…) Na hora que eu abrisse
-- o analítico do colaborador, eu visualizar do mês o detalhamento desses
-- valores — como é que se chegou àquele valor."
--
-- A analítica fica ao lado da sintética, ligada pela COMPETÊNCIA E PELO TIPO
-- (e não pelo número da folha importada): reimportar a sintética — que é normal,
-- a contabilidade corrige e manda de novo — não pode soltar o detalhamento.
--
-- Os eventos de cada pessoa (salário-base, horas extras, INSS, consignado,
-- faltas…) ficam num JSON por pessoa: são lidos só para mostrar, sempre juntos,
-- e uma tabela por evento seria ~6.000 linhas por mês para nenhuma consulta.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS analisesps.folha_analitica (
    id            SERIAL PRIMARY KEY,
    ano           INTEGER      NOT NULL,
    mes           INTEGER      NOT NULL,
    tipo          VARCHAR(20)  NOT NULL,
    nome_arquivo  VARCHAR(300) NOT NULL DEFAULT '',
    pessoas       INTEGER      NOT NULL DEFAULT 0,
    batem         INTEGER      NOT NULL DEFAULT 0,
    importado_por VARCHAR(120) NOT NULL DEFAULT '',
    importado_em  TIMESTAMP    NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_folha_analitica_competencia
    ON analisesps.folha_analitica (ano, mes, tipo);

CREATE TABLE IF NOT EXISTS analisesps.folha_analitica_pessoa (
    id                   SERIAL PRIMARY KEY,
    analitica_id         INTEGER NOT NULL
                         REFERENCES analisesps.folha_analitica (id) ON DELETE CASCADE,
    id_fortes            VARCHAR(10)  NOT NULL,
    nome                 VARCHAR(160) NOT NULL DEFAULT '',
    cargo                VARCHAR(160) NOT NULL DEFAULT '',
    filial               VARCHAR(160) NOT NULL DEFAULT '',
    setor                VARCHAR(160) NOT NULL DEFAULT '',
    total_proventos      NUMERIC(14,2),
    total_descontos      NUMERIC(14,2),
    liquido              NUMERIC(14,2),
    fgts                 NUMERIC(14,2),
    admissao             VARCHAR(20)  NOT NULL DEFAULT '',
    dependentes          VARCHAR(10)  NOT NULL DEFAULT '',
    filhos               VARCHAR(10)  NOT NULL DEFAULT '',
    horas_mes            VARCHAR(10)  NOT NULL DEFAULT '',
    salario_contribuicao NUMERIC(14,2),
    base_inss            NUMERIC(14,2),
    base_fgts            NUMERIC(14,2),
    situacao             VARCHAR(500) NOT NULL DEFAULT '',
    eventos              TEXT         NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS ix_folha_analitica_pessoa
    ON analisesps.folha_analitica_pessoa (analitica_id, id_fortes);

-- A data de nascimento, pedida no analítico do funcionário ("pelo menos a data
-- de nascimento e o CPF deveriam ter aqui"). Vem da aba "Dados Documentos".
ALTER TABLE analisesps.colaborador
    ADD COLUMN IF NOT EXISTS data_nascimento DATE;

-- O QUE MUDOU NA REIMPORTAÇÃO da folha sintética (01/10/2026). O dono: "às vezes
-- é necessário reimportar o arquivo (…) a contabilidade esqueceu uma falta, o
-- valor era para ser menor ou maior, uma hora extra que não foi calculada (…) e
-- o sistema iria criticar: esse veio com valor diferente, esse foi eliminado da
-- folha, esse entrou." A comparação com a versão anterior fica guardada na folha
-- nova, em JSON, até a próxima importação.
ALTER TABLE analisesps.folha
    ADD COLUMN IF NOT EXISTS mudancas TEXT NOT NULL DEFAULT '';
