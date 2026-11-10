-- ---------------------------------------------------------------------------
-- 098 — O EQUIPAMENTO DO DIÁRIO VEM DO CADASTRO DE INSUMOS
--
-- Pedido do dono em 10/11/2026, olhando a tela que a 097 criou:
--
--   *"Nós temos aqui o campo equipamentos, texto livre, e equipamentos em
--   obra. Eles são a mesma coisa, por que teriam os dois textos? Ou é um ou é
--   o outro. E eu acho que a opção da forma que ficou equipamentos em obra
--   ficou melhor. E esses equipamentos, o ideal é que a gente possa buscar
--   dentro da nossa lista de insumos, né? Até por questão de nomenclatura.
--   São os equipamentos inclusive categorizados como locáveis."*
--
-- Ele tem razão duas vezes. O texto livre virou duplicidade, e foi tirado da
-- tela. E equipamento digitado à mão produz "retro", "retroescavadeira",
-- "Retro-escavadeira" e "escavadeira" para a MESMA máquina — e aí nenhuma
-- conta de dias-máquina fecha.
--
-- ⚑ A DESCRIÇÃO CONTINUA GRAVADA NA LINHA, mesmo vindo do cadastro, e isso
-- não é redundância: o cadastro do insumo pode ser renomeado amanhã, e o
-- diário é DOCUMENTO — ele tem de dizer o que foi escrito NAQUELE dia, não o
-- que o cadastro diz hoje. O `insumo_id` serve para CONTAR; a descrição, para
-- PROVAR.
--
-- `insumo_id` é opcional de propósito: a obra começa às 7 da manhã e não pode
-- parar porque uma máquina ainda não foi cadastrada. Equipamento fora da
-- lista entra marcado, e a tela mostra a marca para Suprimentos arrumar.
-- ---------------------------------------------------------------------------
ALTER TABLE diario_equipamentos
    ADD COLUMN IF NOT EXISTS insumo_id BIGINT REFERENCES insumos(id);

CREATE INDEX IF NOT EXISTS idx_diario_equipamentos_insumo
    ON diario_equipamentos (insumo_id) WHERE insumo_id IS NOT NULL;
