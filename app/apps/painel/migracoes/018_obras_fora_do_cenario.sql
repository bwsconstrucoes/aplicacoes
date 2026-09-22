-- 018 — Obras e projetos FORA da análise de um cenário
--
-- 22/09/2026, o dono: "preciso poder remover projetos ou obras da análise".
-- O que sai daqui não recebe estrutura, não recebe juros, não entra na quota
-- de ninguém e não pesa na régua. Cada cenário tem a sua lista — comparar
-- "com o Ceará" e "sem o Ceará" é trocar de cenário.
--
-- O item segue a mesma forma da Necessidade de Caixa: "obra:NOME" ou
-- "projeto:NOME" (o projeto se abre em todas as obras dele na hora da conta).

CREATE TABLE IF NOT EXISTS painel.cenario_excluida (
    cenario_id  BIGINT NOT NULL REFERENCES painel.cenario(id) ON DELETE CASCADE,
    item        TEXT NOT NULL,
    PRIMARY KEY (cenario_id, item)
);
