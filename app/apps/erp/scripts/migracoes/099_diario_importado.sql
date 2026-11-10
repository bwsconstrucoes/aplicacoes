-- ---------------------------------------------------------------------------
-- 099 — DE ONDE VEIO O DIÁRIO, quando ele não nasceu aqui
--
-- Pedido do dono em 10/11/2026, mandando a planilha com 1.427 diários já
-- escritos: *"eu gostaria de importar essa base que nós temos aí para dentro
-- do diário de obras, porque aí a gente poderia dar prosseguimento ao que a
-- gente já tem."*
--
-- ⚑ DIÁRIO IMPORTADO TEM DE DIZER QUE É IMPORTADO. Ele vale como documento —
-- é o que a obra escreveu, no dia — mas não passou pelo fluxo daqui: ninguém
-- fechou, ninguém assinou, e o efetivo não veio do ponto. Guardar a
-- procedência é o que permite, daqui a um ano, saber por que aquele dia não
-- tem batida de ponto nenhuma e mesmo assim declara 23 pessoas.
-- ---------------------------------------------------------------------------

-- Quem preencheu no sistema antigo. Não é o usuário do ERP (essa pessoa pode
-- nem ter login aqui) — é o nome como ficou registrado lá.
ALTER TABLE diarios_obra
    ADD COLUMN IF NOT EXISTS informado_por TEXT;

-- A origem ("planilha") e a chave de lá.
ALTER TABLE diarios_obra
    ADD COLUMN IF NOT EXISTS importado_de TEXT;
ALTER TABLE diarios_obra
    ADD COLUMN IF NOT EXISTS importado_id TEXT;

-- ⚑ O ÍNDICE ÚNICO É O QUE TORNA A IMPORTAÇÃO REPETÍVEL. Rodar o arquivo duas
-- vezes por engano é o acidente mais comum de importação, e sem isto o
-- segundo passe dobraria o efetivo de 1.427 dias. Parcial porque a esmagadora
-- maioria dos diários nasce aqui e não tem origem nenhuma.
CREATE UNIQUE INDEX IF NOT EXISTS idx_diario_importado_unico
    ON diarios_obra (importado_de, importado_id)
 WHERE importado_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- O EFETIVO DO DIÁRIO ANTIGO vem POR FUNÇÃO, sem nome: "1 - ENGENHEIRO CIVIL,
-- 1 - MESTRE DE OBRAS, 17 - SERVENTE". É exatamente o formato que
-- `diario_efetivo_extra` já guarda (função + quantidade), então ele é
-- reusado — mas com a origem marcada, porque a tela NÃO pode chamar isso de
-- "terceirizado que não bate ponto aqui": é o efetivo próprio de um dia em
-- que o ponto eletrônico ainda não existia naquela obra.
-- ---------------------------------------------------------------------------
ALTER TABLE diario_efetivo_extra
    ADD COLUMN IF NOT EXISTS origem TEXT NOT NULL DEFAULT 'OBRA';

ALTER TABLE diario_efetivo_extra
    DROP CONSTRAINT IF EXISTS efetivo_extra_origem_conhecida;
ALTER TABLE diario_efetivo_extra
    ADD CONSTRAINT efetivo_extra_origem_conhecida
    CHECK (origem IN ('OBRA', 'IMPORTADO'));
