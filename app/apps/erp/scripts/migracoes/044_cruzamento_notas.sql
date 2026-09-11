-- ===========================================================================
-- Migração 044 — o cruzamento de notas fiscais.
--
-- POR QUE EXISTE
--
-- Ditado pelo dono em 07/09/2026 e escrito inteiro em `NOTAS_FISCAIS.md`. O
-- resumo dele: *"esse cravamento dessas informações, essa conexão, é o que faz
-- a diferença de um bom ERP, reduzindo a necessidade do trabalho humano nessa
-- conferência — que pra fazer uma a uma, dentro do volume que nós temos, é
-- muito difícil."*
--
-- Hoje: um sistema de terceiro baixa as notas emitidas contra os CNPJs da BWS,
-- alguém tira um relatório, e NA MÃO tenta descobrir de que pedido é cada nota
-- e qual título paga ela. O que não se acha, não se confere — e nota emitida
-- contra o CNPJ da empresa sem ninguém saber é problema fiscal.
--
-- O QUE ESTA MIGRAÇÃO ACRESCENTA
--
-- A nota já existia (`documentos_fiscais`), e o título já sabia apontar para
-- ela (`titulos.documento_fiscal_id`). Faltavam as OUTRAS pontas do cruzamento
-- e a memória da conferência humana:
--
--   empresa_id       contra QUAL CNPJ nosso a nota foi emitida. A BWS opera com
--                    mais de um, e sem isso não dá para separar.
--   pedido_compra_id de que pedido esta nota é. UM PEDIDO TEM VÁRIAS NOTAS —
--                    "comprei dez carradas de brita e o fornecedor emite a nota
--                    por carrada" —, então a ligação é do lado da NOTA, nunca
--                    do lado do pedido.
--   titulo_item_id   a linha da prestação de fundo fixo, quando a nota entrou
--                    por ali. "Uma notazinha de cem reais que alguém da obra
--                    mandou emitir no CNPJ da empresa."
--   conferencia      o que uma PESSOA decidiu: PENDENTE, CASADA, SEM_PAR,
--                    IGNORADA. O sistema propõe; quem confirma é gente, como
--                    na conciliação bancária.
--
-- A TRAVA CONTRA CONTAR A MESMA DESPESA DUAS VEZES
--
-- É o ponto mais perigoso do desenho todo, porque o erro NÃO aparece na tela —
-- aparece na contabilidade, meses depois. Uma nota que virou título e que
-- também está dentro de uma prestação de fundo fixo seria deduzida duas vezes.
--
-- `titulo_item_id` é ÚNICO: a mesma linha de prestação não recebe duas notas.
-- E `titulos.documento_fiscal_id` já era único: a mesma nota não vira dois
-- títulos. A combinação proibida — nota com título próprio E dentro de uma
-- prestação — é barrada no código (`core/notas/cruzamento.py`), porque
-- envolve duas tabelas, e tem caso de teste com banco de verdade.
-- ===========================================================================

ALTER TABLE documentos_fiscais ADD COLUMN IF NOT EXISTS empresa_id BIGINT REFERENCES empresas(id);
ALTER TABLE documentos_fiscais ADD COLUMN IF NOT EXISTS pedido_compra_id BIGINT REFERENCES pedidos_compra(id);
ALTER TABLE documentos_fiscais ADD COLUMN IF NOT EXISTS titulo_item_id BIGINT REFERENCES titulo_itens(id);
ALTER TABLE documentos_fiscais ADD COLUMN IF NOT EXISTS conferencia TEXT NOT NULL DEFAULT 'PENDENTE';
ALTER TABLE documentos_fiscais ADD COLUMN IF NOT EXISTS conferencia_motivo TEXT;
ALTER TABLE documentos_fiscais ADD COLUMN IF NOT EXISTS conferido_por BIGINT REFERENCES usuarios(id);
ALTER TABLE documentos_fiscais ADD COLUMN IF NOT EXISTS conferido_em TIMESTAMPTZ;

ALTER TABLE documentos_fiscais DROP CONSTRAINT IF EXISTS ck_nota_conferencia;
ALTER TABLE documentos_fiscais ADD CONSTRAINT ck_nota_conferencia
    CHECK (conferencia IN ('PENDENTE', 'CASADA', 'SEM_PAR', 'IGNORADA'));

-- Ignorar uma nota é decisão que precisa de motivo escrito: seis meses depois
-- ninguém lembra por que aquela nota foi posta de lado, e "sem motivo" é
-- exatamente o que o fisco pergunta.
ALTER TABLE documentos_fiscais DROP CONSTRAINT IF EXISTS ck_nota_ignorada_com_motivo;
ALTER TABLE documentos_fiscais ADD CONSTRAINT ck_nota_ignorada_com_motivo
    CHECK (conferencia <> 'IGNORADA' OR nullif(btrim(conferencia_motivo), '') IS NOT NULL);

-- Uma linha de prestação de fundo fixo recebe UMA nota, não duas.
CREATE UNIQUE INDEX IF NOT EXISTS idx_nota_item_prestacao
    ON documentos_fiscais (titulo_item_id) WHERE titulo_item_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_nota_empresa ON documentos_fiscais (empresa_id);
CREATE INDEX IF NOT EXISTS idx_nota_pedido ON documentos_fiscais (pedido_compra_id);
CREATE INDEX IF NOT EXISTS idx_nota_conferencia ON documentos_fiscais (conferencia);
CREATE INDEX IF NOT EXISTS idx_nota_emissao ON documentos_fiscais (data_emissao DESC);
CREATE INDEX IF NOT EXISTS idx_nota_emitente ON documentos_fiscais (emitente_doc);
