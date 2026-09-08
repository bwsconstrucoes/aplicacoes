-- ===========================================================================
-- Migração 041 — quem RESPONDE pela obra.
--
-- POR QUE EXISTE
--
-- Até aqui o sistema ADIVINHAVA quem responde a conferência mensal dos
-- equipamentos: pegava um administrativo associado à obra. Adivinhar funciona
-- até a obra ter dois administrativos, ou nenhum — e aí a cobrança do agente
-- vai para a pessoa errada, ou para ninguém.
--
-- Decisão do dono (07/09/2026): "no cadastro da obra, a gente vai associar uma
-- das pessoas, um dos operadores, pra responder por aquela obra… e se por
-- acaso tiverem dois, a gente cadastrar dois, permitir também, os dois
-- recebem".
--
-- Então deixa de ser adivinhação e passa a ser ESCRITO. A tabela que já ligava
-- operador e obra ganha uma marca: esta pessoa responde por esta obra. Aceita
-- mais de uma por obra de propósito.
--
-- POR QUE AQUI E NÃO NUMA TABELA NOVA: a ligação operador↔obra já existe e é
-- por onde o escopo de visão passa. Criar uma segunda tabela para dizer quase
-- a mesma coisa faria as duas divergirem — alguém responderia por uma obra que
-- não enxerga.
-- ===========================================================================

ALTER TABLE usuario_obras
    ADD COLUMN IF NOT EXISTS responsavel BOOLEAN NOT NULL DEFAULT FALSE;

-- Uma pessoa não pode estar ligada duas vezes à mesma obra: sem isto, marcar
-- responsável no cadastro da obra e no cadastro do operador criaria duas
-- linhas, e as telas passariam a discordar sobre quem responde.
CREATE UNIQUE INDEX IF NOT EXISTS idx_usuario_obra_unica
    ON usuario_obras (usuario_id, obra_id);

CREATE INDEX IF NOT EXISTS idx_usuario_obra_responsavel
    ON usuario_obras (obra_id) WHERE responsavel;
