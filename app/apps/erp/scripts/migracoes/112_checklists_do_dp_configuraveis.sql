-- ===========================================================================
-- OS CHECKLISTS DO DP PASSAM A SER CONFIGURÁVEIS PELA TELA
--
-- Pedido dele em 18/11/2026, olhando as solicitações do DP:
--
--   *"Vi que você importou checklist do Pipefy, só que eles estão todos
--    desatualizados. Precisa de uma tela pra configurar eles de forma fácil."*
--
-- O QUE EXISTIA: os passos de cada tipo de solicitação estavam ESCRITOS EM
-- CÓDIGO (`core/pessoal_solicitacoes.py`, `TIPOS[...]["checklist"]`), copiados
-- do pipe em 05/11. Mudar um item era mexer no código e publicar — ou seja, o
-- DP dependia de uma publicação para corrigir a própria rotina de trabalho.
--
-- ⚑ O PADRÃO DO CÓDIGO NÃO SAI, e é o que torna isto seguro: esta tabela
-- GUARDA EXCEÇÃO, não a lista inteira. Tipo que ninguém configurou continua
-- usando a lista do código, que é a do pipe. Assim:
--   · nada muda no dia em que esta migração roda;
--   · "voltar ao padrão" é apagar as linhas daqui, não redigitar onze itens;
--   · tipo novo nasce com checklist sem ninguém configurar nada.
--
-- ⚠️ MUDAR O MODELO NÃO MEXE NO QUE JÁ ESTÁ ABERTO. Os itens são COPIADOS para
-- a solicitação na hora em que ela é aberta (`dp_solicitacao_itens`) — de
-- propósito, desde sempre: um pedido em andamento com os passos trocados no
-- meio faria o DP perder o que já marcou, e faria o histórico mentir sobre o
-- que foi conferido. A tela diz isso com essas palavras.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS dp_checklists (
    id          SERIAL PRIMARY KEY,
    tipo        TEXT    NOT NULL,
    -- NULL = vale para qualquer vínculo. 'CTPS' / 'PRESTADOR' = só para ele.
    -- O código já ramifica assim (`por_vinculo`), e a tela espelha isso.
    vinculo     TEXT,
    ordem       INTEGER NOT NULL,
    descricao   TEXT    NOT NULL,
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    criado_por  INTEGER REFERENCES usuarios(id),
    CONSTRAINT ck_dp_checklist_vinculo
        CHECK (vinculo IS NULL OR vinculo IN ('CTPS', 'PRESTADOR')),
    CONSTRAINT ck_dp_checklist_descricao
        CHECK (btrim(descricao) <> '')
);

-- A ordem dentro de um tipo+vínculo é única: dois itens no mesmo lugar fariam
-- a lista aparecer em ordem diferente a cada leitura.
CREATE UNIQUE INDEX IF NOT EXISTS ux_dp_checklist_ordem
    ON dp_checklists (tipo, COALESCE(vinculo, ''), ordem);

CREATE INDEX IF NOT EXISTS ix_dp_checklist_tipo
    ON dp_checklists (tipo, vinculo);
