-- ===========================================================================
-- Migração 052 — empreita: RETENÇÃO DE GARANTIA e ALÇADA POR VALOR.
--
-- 1. A RETENÇÃO DE GARANTIA
--
-- É o costume da construção: guarda-se uma parte de cada medição (na BWS, 5%)
-- e devolve-se no fim, quando o serviço passou pelo período de garantia. Serve
-- para o dia em que o empreiteiro some e o reparo fica com a obra.
--
-- HOJE ISSO SE FAZ NA PLANILHA, e o defeito é sempre o mesmo: retém-se
-- direitinho por doze medições, e no fim ninguém sabe quanto ficou retido nem
-- quando devolver. O dinheiro fica parado, o empreiteiro cobra, e alguém
-- refaz a conta de memória.
--
-- POR QUE O VALOR RETIDO FICA GRAVADO EM CADA MEDIÇÃO, e não só o percentual
-- no contrato: o percentual pode mudar por aditivo, e a medição de março tem
-- de continuar dizendo quanto foi retido EM MARÇO. Guardar só o percentual
-- atual faria a conta do passado mudar sozinha — que é como se perde uma
-- discussão com o empreiteiro.
--
-- A LIBERAÇÃO É UM TÍTULO A PAGAR, não um acerto de planilha: ela passa pelo
-- mesmo caminho de qualquer pagamento (aprovação, baixa, comprovante), porque
-- é dinheiro saindo. E fica registrado quem liberou e quando.
--
-- 2. A ALÇADA POR VALOR DE CONTRATO
--
-- Hoje qualquer perfil de obra aprova empreita de qualquer tamanho. Uma
-- empreita de oitocentos reais e uma de oitocentos mil passam pela mesma
-- porta, e isso não é razoável nem seguro.
--
-- A faixa é TABELA, não número no código, porque o teto muda com o tamanho da
-- empresa — e quando mudar, quem muda é o dono, na tela, sem esperar
-- publicação.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- Retenção de garantia
-- ---------------------------------------------------------------------------
ALTER TABLE contratos_servico
    ADD COLUMN IF NOT EXISTS retencao_garantia_pct NUMERIC(5, 2) NOT NULL DEFAULT 0;
ALTER TABLE contratos_servico
    ADD COLUMN IF NOT EXISTS retencao_liberada_em DATE;
ALTER TABLE contratos_servico
    ADD COLUMN IF NOT EXISTS retencao_liberada_por BIGINT REFERENCES usuarios(id);
ALTER TABLE contratos_servico
    ADD COLUMN IF NOT EXISTS retencao_titulo_id BIGINT REFERENCES titulos(id);
ALTER TABLE contratos_servico
    ADD COLUMN IF NOT EXISTS retencao_motivo TEXT;

-- Percentual fora de 0..50 é dígito trocado. Reter metade de uma empreita já
-- é absurdo; mais que isso não existe.
ALTER TABLE contratos_servico DROP CONSTRAINT IF EXISTS ck_empreita_retencao_pct;
ALTER TABLE contratos_servico ADD CONSTRAINT ck_empreita_retencao_pct
    CHECK (retencao_garantia_pct >= 0 AND retencao_garantia_pct <= 50);

-- O VALOR RETIDO NA MEDIÇÃO. Gravado, não recalculado: o percentual do
-- contrato pode mudar, e a medição de março tem de continuar dizendo quanto
-- foi retido em março.
ALTER TABLE contrato_medicoes
    ADD COLUMN IF NOT EXISTS valor_retido NUMERIC(14, 2) NOT NULL DEFAULT 0;

ALTER TABLE contrato_medicoes DROP CONSTRAINT IF EXISTS ck_medicao_retido_cabe;
ALTER TABLE contrato_medicoes ADD CONSTRAINT ck_medicao_retido_cabe
    CHECK (valor_retido >= 0 AND valor_retido <= valor_medido);

-- Liberado tem de dizer quem e quando — os dois, ou nenhum.
ALTER TABLE contratos_servico DROP CONSTRAINT IF EXISTS ck_empreita_liberacao_completa;
ALTER TABLE contratos_servico ADD CONSTRAINT ck_empreita_liberacao_completa
    CHECK ((retencao_liberada_em IS NULL AND retencao_liberada_por IS NULL)
           OR (retencao_liberada_em IS NOT NULL AND retencao_liberada_por IS NOT NULL));

CREATE INDEX IF NOT EXISTS idx_empreita_retencao_aberta
    ON contratos_servico (status)
    WHERE retencao_garantia_pct > 0 AND retencao_liberada_em IS NULL;

-- ---------------------------------------------------------------------------
-- Alçada por valor de contrato
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS empreita_alcadas (
    id          BIGSERIAL PRIMARY KEY,
    -- Teto da faixa. NULL é "daqui para cima", e existe exatamente uma linha
    -- assim — senão haveria contrato grande que ninguém pode aprovar.
    valor_ate   NUMERIC(14, 2),
    perfis      TEXT[] NOT NULL,
    descricao   TEXT,
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE empreita_alcadas DROP CONSTRAINT IF EXISTS ck_alcada_tem_perfil;
ALTER TABLE empreita_alcadas ADD CONSTRAINT ck_alcada_tem_perfil
    CHECK (array_length(perfis, 1) >= 1);

ALTER TABLE empreita_alcadas DROP CONSTRAINT IF EXISTS ck_alcada_valor_positivo;
ALTER TABLE empreita_alcadas ADD CONSTRAINT ck_alcada_valor_positivo
    CHECK (valor_ate IS NULL OR valor_ate > 0);

-- Só uma faixa "daqui para cima".
CREATE UNIQUE INDEX IF NOT EXISTS idx_alcada_teto_unico
    ON empreita_alcadas ((1)) WHERE valor_ate IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_alcada_valor_unico
    ON empreita_alcadas (valor_ate) WHERE valor_ate IS NOT NULL;

-- As faixas iniciais reproduzem o que já valia — todo mundo que aprovava
-- continua aprovando o contrato pequeno — e só ESTREITAM o de cima. Migração
-- que muda quem pode o quê sem avisar é migração que quebra a operação na
-- segunda de manhã.
INSERT INTO empreita_alcadas (valor_ate, perfis, descricao)
SELECT * FROM (VALUES
    (50000.00::NUMERIC,
     ARRAY['ADMIN','DIRETOR_FINANCEIRO','FINANCEIRO','GESTOR_OBRA','SUPERVISOR_OBRA'],
     'Empreita pequena: quem acompanha a obra aprova'),
    (200000.00::NUMERIC,
     ARRAY['ADMIN','DIRETOR_FINANCEIRO','FINANCEIRO','GESTOR_OBRA'],
     'Empreita média: sai do supervisor, fica com o gestor'),
    (NULL::NUMERIC,
     ARRAY['ADMIN','DIRETOR_FINANCEIRO'],
     'Empreita grande: só direção')
) AS novo(valor_ate, perfis, descricao)
WHERE NOT EXISTS (SELECT 1 FROM empreita_alcadas);
