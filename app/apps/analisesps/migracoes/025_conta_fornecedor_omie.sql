-- ===========================================================================
-- 025 — O FORNECEDOR DO OMIE PASSA A MORAR NA CONTA BANCÁRIA
--
-- Reclamação do dono em 25/09/2026, ao tentar lançar uma tarifa:
--
--   "0 de 1 linha(s) podem ser lançadas na conta BD 50024. 1 fica de fora:
--    TARIFA BANCARIA TRANSF PGTO PIX (-0,35) — o tipo 'Tarifa Bancária' está
--    sem o fornecedor/cliente do OMIE. O código fornecedor tem que estar
--    atrelado à conta bancária."
--
-- E ele tem razão. Quem cobra a tarifa é o BANCO DA CONTA: a do BD 50024 é do
-- Bradesco, a da Sicredi é da Sicredi. Guardar o fornecedor no TIPO obrigaria
-- a criar um tipo "Tarifa Bradesco", outro "Tarifa Sicredi", outro "Tarifa
-- Banco do Brasil" — cada um repetindo as mesmas palavras do histórico, e
-- todos brigando entre si na hora de reconhecer. É a mesma razão pela qual a
-- conta corrente do OMIE já morava aqui, e não no tipo.
--
-- O DO TIPO CONTINUA VALENDO COMO RESERVA: se a conta não tiver fornecedor, o
-- do tipo é usado. Assim nada do que já está configurado para de funcionar por
-- causa desta mudança — e quem quiser um fornecedor específico para um tipo
-- (uma taxa cobrada por um terceiro, por exemplo) continua podendo.
-- ===========================================================================

ALTER TABLE analisesps.conciliacao_conta
    ADD COLUMN IF NOT EXISTS omie_fornecedor BIGINT;
