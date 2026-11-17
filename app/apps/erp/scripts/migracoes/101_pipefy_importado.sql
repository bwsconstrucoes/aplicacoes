-- ---------------------------------------------------------------------------
-- 101 — O QUE JÁ VEIO DO PIPEFY, para a segunda passada não baixar de novo
--
-- Pedido do dono em 10/11/2026, olhando o cadastro das pessoas:
--
--   *"Olhando os cadastros, a planilha lá, dados e documentos, tem o número do
--   card do pipe, certo? Então, tu não consegue fazer logo essa importação,
--   não? Faz a leitura do card, baixa os documentos, importa para o ERP. Só
--   das pessoas que você já sabe que tem que estar."*
--
-- O número do card já existe: é `colaboradores.ref_pipefy`. O que falta é
-- memória do que JÁ foi trazido.
--
-- ⚑ POR QUE NÃO BASTA O DESEMPATE QUE JÁ EXISTE: o ERP já recusa guardar duas
-- vezes o mesmo arquivo da mesma pessoa (ele compara o conteúdo, não o nome).
-- Só que, para comparar, PRECISA TER BAIXADO. Com 575 pessoas e até oito
-- anexos cada, rodar de novo seriam ~4.600 downloads para concluir que nada
-- mudou — meia hora de rede para um resultado vazio. Esta tabela responde
-- "esse já veio" ANTES de sair baixando.
--
-- Ela também é o relatório: de qual card e de qual campo do Pipefy saiu cada
-- documento da pessoa. Sem isso, daqui a um ano ninguém sabe se a CTPS
-- guardada foi alguém que fotografou ou a importação que trouxe.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipefy_importado (
    id              BIGSERIAL PRIMARY KEY,
    card            TEXT   NOT NULL,
    campo           TEXT   NOT NULL,
    item            INT    NOT NULL DEFAULT 0,
    colaborador_id  BIGINT NOT NULL REFERENCES colaboradores(id) ON DELETE CASCADE,
    documento_id    BIGINT          REFERENCES documentos(id)    ON DELETE SET NULL,
    resultado       TEXT   NOT NULL DEFAULT 'GUARDADO',
    detalhe         TEXT,
    importado_em    TIMESTAMPTZ NOT NULL DEFAULT now(),
    importado_por   BIGINT          REFERENCES usuarios(id)
);

-- ⚑ A CHAVE É (card, campo, item) — e `item` existe porque um campo de anexo
-- do Pipefy guarda VÁRIOS arquivos. "Documentos Diversos" com três páginas são
-- três documentos, e sem o item o segundo e o terceiro nunca entrariam.
CREATE UNIQUE INDEX IF NOT EXISTS idx_pipefy_importado_unico
    ON pipefy_importado (card, campo, item);

CREATE INDEX IF NOT EXISTS idx_pipefy_importado_pessoa
    ON pipefy_importado (colaborador_id);

-- O que pode ter acontecido com cada anexo. 'GUARDADO' entrou; 'PULADO' já
-- estava lá; 'FALHOU' não veio, e o motivo fica em `detalhe` — para a tela
-- poder mostrar a lista do que precisa de mão humana em vez de dizer
-- "importação concluída" escondendo trinta arquivos que não desceram.
ALTER TABLE pipefy_importado DROP CONSTRAINT IF EXISTS ck_pipefy_resultado;
ALTER TABLE pipefy_importado ADD CONSTRAINT ck_pipefy_resultado
    CHECK (resultado IN ('GUARDADO', 'PULADO', 'FALHOU'));

COMMENT ON TABLE pipefy_importado IS
    'De qual card e campo do Pipefy saiu cada documento importado (10/11/2026).';
