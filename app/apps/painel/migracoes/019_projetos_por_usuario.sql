-- ===========================================================================
-- 019 — ACESSO POR PROJETO, alem de por obra
--
-- 23/09/2026, o dono: "tanto define por obra como por projeto, porque pode
-- ser que eu queira dar acesso ao projeto como um todo".
--
-- A pessoa continua presa a OBRAS (migracao 013). O projeto e um atalho que
-- se abre em obras NA HORA DE ENTRAR: quem tem o projeto ve todas as obras
-- dele, inclusive as que ainda nao existiam quando o acesso foi dado. A
-- lista de obras marcadas uma a uma soma-se a isso.
--
-- Mesma regra de sempre: sem obra nenhuma (marcada ou vinda de projeto), a
-- pessoa nao entra. Lista vazia quer dizer nenhuma, nunca todas.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS painel.usuario_projetos (
    usuario_id BIGINT NOT NULL REFERENCES painel.usuarios(id) ON DELETE CASCADE,
    projeto    TEXT   NOT NULL,
    PRIMARY KEY (usuario_id, projeto)
);
