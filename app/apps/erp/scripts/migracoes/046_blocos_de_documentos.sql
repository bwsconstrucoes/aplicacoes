-- ===========================================================================
-- Migração 046 — os BLOCOS de documentos: o que sempre é pedido junto.
--
-- A PERGUNTA DO DONO, em 09/09/2026: *"a minha dúvida é como é que esses
-- blocos vão se associar a determinados documentos, se isso é fácil de
-- resolver."*
--
-- É fácil, e a razão é uma decisão de desenho que vale ficar escrita:
--
--   O BLOCO NÃO APONTA PARA DOCUMENTOS. ELE APONTA PARA TIPOS.
--
-- Um bloco é uma lista de TIPOS ("folha", "guia de FGTS", "recibo da DCTFWeb")
-- mais um RECORTE ("desta obra, desta competência"). Na hora de baixar, o
-- sistema procura, para cada tipo da lista, o documento que casa com o
-- recorte. Ninguém escolhe arquivo a arquivo, e ninguém precisa manter lista
-- nenhuma atualizada.
--
-- Por que isso importa: se o bloco apontasse para documentos, cada competência
-- nova exigiria remontar o bloco à mão — que é exatamente o trabalho que a
-- gestão de documentos veio eliminar. Apontando para tipos, o bloco de agosto
-- e o de setembro são o MESMO bloco, com recortes diferentes.
--
-- O RECORTE tem quatro formatos, e eles cobrem tudo que foi pedido:
--   EMPRESA            — habilitação, cadastro de fornecedor
--   EMPRESA_COMPETENCIA— documentos fiscais que são da empresa, não da obra
--   OBRA               — o dossiê da obra
--   OBRA_COMPETENCIA   — o bloco fiscal da medição
--
-- E o detalhe que faz o bloco fiscal funcionar de verdade: dentro dele há
-- documentos DA OBRA (folha, guia de FGTS) e documentos DA EMPRESA (recibo da
-- DCTFWeb, DARF do INSS). Pedindo o bloco de uma obra, o sistema resolve os
-- itens de empresa pela EMPRESA DA OBRA — que já está no cadastro. Sem isso, o
-- bloco viria pela metade e ninguém entenderia por quê.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS documento_blocos (
    codigo      TEXT PRIMARY KEY,
    nome        TEXT NOT NULL,
    descricao   TEXT,
    recorte     TEXT NOT NULL,
    ativo       BOOLEAN NOT NULL DEFAULT TRUE,
    ordem       INT NOT NULL DEFAULT 100,
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE documento_blocos DROP CONSTRAINT IF EXISTS ck_bloco_recorte;
ALTER TABLE documento_blocos ADD CONSTRAINT ck_bloco_recorte
    CHECK (recorte IN ('EMPRESA', 'EMPRESA_COMPETENCIA', 'OBRA', 'OBRA_COMPETENCIA'));

ALTER TABLE documento_blocos DROP CONSTRAINT IF EXISTS ck_bloco_codigo;
ALTER TABLE documento_blocos ADD CONSTRAINT ck_bloco_codigo
    CHECK (codigo ~ '^[A-Z0-9]+(-[A-Z0-9]+)*$');


CREATE TABLE IF NOT EXISTS documento_bloco_itens (
    id            BIGSERIAL PRIMARY KEY,
    bloco_codigo  TEXT NOT NULL REFERENCES documento_blocos(codigo) ON DELETE CASCADE,
    tipo_codigo   TEXT NOT NULL REFERENCES documento_tipos(codigo),
    -- Item OBRIGATÓRIO que falta vira aviso em destaque no arquivo de
    -- conferência; item opcional que falta é só uma linha. Sem essa diferença,
    -- todo bloco pareceria incompleto e ninguém olharia mais a lista.
    obrigatorio   BOOLEAN NOT NULL DEFAULT TRUE,
    ordem         INT NOT NULL DEFAULT 100,
    observacao    TEXT
);

-- O mesmo tipo entra UMA vez em cada bloco. Duas linhas iguais colocariam o
-- mesmo arquivo duas vezes no zip.
CREATE UNIQUE INDEX IF NOT EXISTS idx_bloco_item
    ON documento_bloco_itens (bloco_codigo, tipo_codigo);
CREATE INDEX IF NOT EXISTS idx_bloco_item_bloco
    ON documento_bloco_itens (bloco_codigo, ordem);
