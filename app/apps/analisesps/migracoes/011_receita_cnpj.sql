-- ---------------------------------------------------------------------------
-- 011 — QUEM E O DONO DE UM CNPJ, PERGUNTADO A RECEITA
--
-- Pedido do dono em 13/09/2026, na tela de nomes de credor: "quando voce da
-- sugestao aqui, esse CNPJ e o que? Eu quero que voce faca a consulta via API
-- do credor desse CNPJ."
--
-- POR QUE A RESPOSTA FICA GUARDADA. CNPJ nao muda de dono. Perguntar duas vezes
-- a mesma coisa e desperdicio, e e o que leva ao bloqueio por uso excessivo do
-- servico publico -- e ai a consulta para de funcionar para todo mundo,
-- inclusive no caso em que ela importa.
--
-- O ERRO TAMBEM FICA GUARDADO, e de proposito: "CNPJ nao encontrado na Receita"
-- e o achado MAIS UTIL quando o numero foi digitado errado. Guardar so o
-- sucesso faria a tela reperguntar a mesma coisa para sempre e nunca mostrar a
-- resposta que resolve o caso.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analisesps.receita_cnpj (
    cnpj          TEXT PRIMARY KEY,             -- so digitos
    razao_social  TEXT NOT NULL DEFAULT '',
    fantasia      TEXT NOT NULL DEFAULT '',
    situacao      TEXT NOT NULL DEFAULT '',     -- Ativa, Baixada, Suspensa...
    municipio     TEXT NOT NULL DEFAULT '',
    uf            TEXT NOT NULL DEFAULT '',
    consultado_em TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Vazio quando deu certo. Quando nao, a frase em portugues do que houve.
    erro          TEXT NOT NULL DEFAULT ''
);
