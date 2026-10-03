-- ===========================================================================
-- 046 — AUXÍLIO: VALOR ACRESCENTADO E DESCONTO DE AUSÊNCIAS
--
-- O dono, 03/10/2026:
--
--   *"mês anterior esquecemos de colocar um determinado valor (…) onde eu
--   pudesse adicionar um valor ao pagamento daquele mês do funcionário."*
--
--   *"se a pessoa no mês anterior faltou algum dia, aquele dia deveria ser
--   descontado, proporcional (…) que isso fosse uma opção de aplicar ou não o
--   desconto (…) isso serve só para o transporte."*
--
-- As duas coisas são decisões dele por pessoa, verba e competência — moram no
-- mesmo ajuste (033). `desconto_ausencias` NULO quer dizer "não decidido": o
-- desconto aparece proposto, e só vale quando ele aplica.
--
-- O código não lê nada daqui antes de este arquivo rodar (`tem_coluna`).
-- ===========================================================================
ALTER TABLE analisesps.auxilio_ajuste
    ADD COLUMN IF NOT EXISTS valor_extra NUMERIC(12, 2);
ALTER TABLE analisesps.auxilio_ajuste
    ADD COLUMN IF NOT EXISTS motivo_extra TEXT NOT NULL DEFAULT '';
ALTER TABLE analisesps.auxilio_ajuste
    ADD COLUMN IF NOT EXISTS desconto_ausencias BOOLEAN;
