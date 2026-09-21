-- ===========================================================================
-- 014 — QUAIS CONTAS CORRENTES CADA PESSOA PODE VER
--
-- A migracao 013 prendeu a pessoa a OBRAS. O Extrato de conta corrente, pedido
-- no mesmo dia, traz outra dimensao: o dono quer liberar a tela do extrato para
-- alguem e dizer QUAIS CONTAS essa pessoa enxerga.
--
--     "eu queria poder disponibilizar essa tela para um determinado usuario,
--      mas definir qual conta e quais contas ele poderia visualizar."
--
-- Mesma regra das obras, e pelo mesmo motivo: SEM NENHUMA LINHA AQUI, a pessoa
-- nao ve conta nenhuma no extrato. Lista vazia quer dizer nenhuma, nunca todas.
--
-- Fica em tabela separada de `usuario_obras` de proposito: sao dois recortes
-- diferentes e independentes. Alguem pode ver a obra inteira e so uma das
-- contas por onde ela passa — e o contrario tambem.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS painel.usuario_contas (
    usuario_id BIGINT NOT NULL REFERENCES painel.usuarios(id) ON DELETE CASCADE,
    conta      TEXT   NOT NULL,
    PRIMARY KEY (usuario_id, conta)
);
