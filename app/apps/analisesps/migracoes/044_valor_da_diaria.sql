-- ===========================================================================
-- 044 — O VALOR DA DIÁRIA NO CADASTRO
--
-- O dono, 01/10/2026: a prioridade agora é a folha dos diaristas, "que é o
-- próximo que eu vou gerar". A tela de diaristas já dizia QUEM tem dia de
-- diária e QUANTOS, mas não QUANTO: o valor da diária de cada pessoa não era
-- lido do cadastro (a planilha usa a coluna e pula quem está sem ela —
-- "CORRIGIR VALOR DIÁRIA").
--
-- Coluna opcional: a carga do cadastro só a preenche quando a coluna existe,
-- e o código não lê nada daqui antes de este arquivo rodar (`tem_coluna`).
-- ===========================================================================
ALTER TABLE analisesps.colaborador
    ADD COLUMN IF NOT EXISTS valor_diaria NUMERIC(12, 2);
