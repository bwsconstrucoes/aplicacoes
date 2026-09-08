-- ===========================================================================
-- Migração 042 — a trava que impede o mesmo pagamento de ser baixado DUAS VEZES.
--
-- POR QUE EXISTE
--
-- O dono, em 07/09/2026: "o sistema realmente não pode baixar duas vezes,
-- precisa barrar". Até aqui NÃO HAVIA TRAVA NENHUMA no ERP: o mesmo
-- comprovante processado de novo dava outra baixa, sem nada reclamar.
--
-- E o comprovante VAI chegar repetido — não é hipótese, é rotina: ele é
-- encaminhado do banco e também anexado na tela; o Make reprocessa um e-mail;
-- alguém manda de novo achando que o primeiro não foi.
--
-- POR QUE A TRAVA MORA AQUI, NO BANCO, E NÃO NO CÓDIGO
--
-- A trava que existia no `baixabradesco` vivia numa lista lida para a memória,
-- de uma planilha. Ela falha LIBERANDO: qualquer erro de leitura devolvia
-- lista vazia, e o lote inteiro parecia novo. Restrição única no banco não tem
-- esse defeito — ela não depende de o código lembrar de perguntar, e vale
-- mesmo com duas execuções ao mesmo tempo.
--
-- DOIS NÍVEIS, porque um só não pega tudo:
--
--   1. O ARQUIVO, pelo CONTEÚDO — e sem o nome. Era esse o maior buraco da
--      trava antiga: "comprovante.pdf" e "comprovante (1).pdf" são o mesmo
--      documento, e renomear acontece o tempo todo.
--   2. O PAGAMENTO: a mesma parcela, com o mesmo valor, no mesmo dia. É o que
--      pega o PDF REGERADO pelo banco — bytes diferentes, pagamento igual.
--
-- O segundo é parcial de propósito (`WHERE parcela_id IS NOT NULL`): duas
-- leituras que não acharam título nenhum não são duplicata uma da outra.
--
-- E PAGAMENTO PARCIAL CONTINUA POSSÍVEL: a mesma parcela aceita outra baixa em
-- outro dia, ou com outro valor. O que a trava barra é a repetição idêntica.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS comprovantes_lidos (
    id              BIGSERIAL PRIMARY KEY,

    -- sha256 do CONTEÚDO do arquivo. Sem o nome, de propósito.
    hash_conteudo   TEXT    NOT NULL,
    nome_arquivo    TEXT,
    tamanho_bytes   INT,

    -- por qual porta entrou: TELA, MAKE ou EMAIL. As três caem aqui.
    origem          TEXT    NOT NULL DEFAULT 'TELA'
                    CHECK (origem IN ('TELA', 'MAKE', 'EMAIL')),

    -- o que a leitura concluiu
    situacao        TEXT    NOT NULL,
    titulo_id       BIGINT REFERENCES titulos(id),
    parcela_id      BIGINT REFERENCES parcelas(id),
    pagamento_id    BIGINT REFERENCES pagamentos(id),

    valor           NUMERIC(14,2),
    data_pagamento  DATE,
    favorecido      TEXT,
    documento       TEXT,
    mensagem        TEXT,

    usuario_id      BIGINT REFERENCES usuarios(id),
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 1. O MESMO ARQUIVO nunca entra duas vezes, venha de onde vier.
CREATE UNIQUE INDEX IF NOT EXISTS idx_comprovante_arquivo
    ON comprovantes_lidos (hash_conteudo);

-- 2. O MESMO PAGAMENTO nunca é baixado duas vezes, mesmo por arquivos
--    diferentes. Só vale para leitura que virou baixa de verdade.
CREATE UNIQUE INDEX IF NOT EXISTS idx_comprovante_pagamento
    ON comprovantes_lidos (parcela_id, valor, data_pagamento)
    WHERE parcela_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_comprovante_titulo
    ON comprovantes_lidos (titulo_id);
CREATE INDEX IF NOT EXISTS idx_comprovante_quando
    ON comprovantes_lidos (criado_em DESC);
