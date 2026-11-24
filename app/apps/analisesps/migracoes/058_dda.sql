-- ===========================================================================
-- 058 — O DDA DO BRADESCO (24/11/2026)
--
-- O dono: "No sistema do Bradesco, se a gente entrar lá no DDA, a gente consegue
-- copiar uma série de informações (…) vencimento, beneficiário, pagador, o CNPJ do
-- pagador, se é o da matriz, da filial, o valor, e o código de barras. (…) Eu vou
-- ficar colando diariamente (…) e a gente tentar cruzar essas informações com os
-- registros que nós temos lançado (…) Se tem boletos que estão lançados no sistema
-- DDA, mas não estão lançados no nosso registro financeiro."
--
-- Um boleto é UMA linha, pela linha digitável: colar o DDA de novo no dia seguinte
-- não duplica — atualiza e marca em que colagem ele apareceu por último (o que sai
-- do DDA, em geral, foi pago). O vínculo com a SP é calculado na hora, a cada vez
-- que a tela abre (a SP pode ser lançada depois do boleto); só o vínculo FEITO À MÃO
-- fica gravado aqui.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS analisesps.dda_colagem (
    id          SERIAL PRIMARY KEY,
    colado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    colado_por  TEXT NOT NULL DEFAULT '',
    boletos     INTEGER NOT NULL DEFAULT 0,
    total       NUMERIC(14,2) NOT NULL DEFAULT 0,
    -- O "Total (36 boletos) R$ 190.520,90" que o banco escreve no fim: é a prova
    -- de que a leitura não perdeu nenhum.
    total_do_banco   NUMERIC(14,2),
    boletos_do_banco INTEGER
);

CREATE TABLE IF NOT EXISTS analisesps.dda_boleto (
    id                 SERIAL PRIMARY KEY,
    barras             TEXT NOT NULL UNIQUE,      -- o código de barras (44 dígitos)
    linha_digitavel    TEXT NOT NULL DEFAULT '',
    vencimento         DATE,
    limite             DATE,
    pagador_nome       TEXT NOT NULL DEFAULT '',
    pagador_cnpj       TEXT NOT NULL DEFAULT '',  -- só dígitos
    beneficiario_nome  TEXT NOT NULL DEFAULT '',
    beneficiario_doc   TEXT NOT NULL DEFAULT '',  -- só dígitos (CNPJ ou CPF)
    beneficiario_final_nome TEXT NOT NULL DEFAULT '',
    beneficiario_final_doc  TEXT NOT NULL DEFAULT '',
    banco              TEXT NOT NULL DEFAULT '',
    valor              NUMERIC(14,2) NOT NULL DEFAULT 0,
    valor_documento    NUMERIC(14,2),
    descontos          NUMERIC(14,2),
    juros              NUMERIC(14,2),
    multa              NUMERIC(14,2),
    numero_documento   TEXT NOT NULL DEFAULT '',
    primeira_colagem   INTEGER REFERENCES analisesps.dda_colagem (id) ON DELETE SET NULL,
    ultima_colagem     INTEGER REFERENCES analisesps.dda_colagem (id) ON DELETE SET NULL,
    visto_em           TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- O vínculo FEITO À MÃO (o automático é calculado na hora): a SP, ou
    -- 'NENHUMA' quando alguém confirmou que o boleto não tem lançamento de propósito.
    sp_manual          TEXT NOT NULL DEFAULT '',
    sp_manual_por      TEXT NOT NULL DEFAULT '',
    sp_manual_em       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_dda_boleto_venc ON analisesps.dda_boleto (vencimento);
CREATE INDEX IF NOT EXISTS ix_dda_boleto_ultima ON analisesps.dda_boleto (ultima_colagem);
