-- ============================================================================
-- 130 — O PERFIL DIZ QUEM ENTRA PELO PORTAL (27/11/2026)
--
-- Pedido dele, com todas as letras: *"o cargo base não deveria influenciar em
-- nada. quem deve mandar é o perfil."*
--
-- Até aqui, quem era levado ao portal em vez das telas de dentro era decidido
-- pelo CARGO da pessoa (`PERFIS_SO_PORTAL`, o "administrativo de obra"). Foi
-- isso que prendeu uma pessoa do DP fora do ERP por dias: o perfil dela abria
-- meio sistema, e o cargo mandava para o portal, sem aviso em tela nenhuma.
--
-- POR QUE UMA COLUNA, E NÃO UMA DEDUÇÃO. A primeira tentativa foi deduzir:
-- "entra pelo portal quem não abre nada além do diário". Não serve, e o
-- próprio portal mostra por quê — ele lista colaboradores e abre solicitações
-- ao DP, então quem trabalha na obra PRECISA dessas permissões para usá-lo.
-- Pela dedução, essa pessoa deixaria de ser "da obra" justamente por ter o que
-- precisa para trabalhar.
--
-- Portal e ERP são duas PORTAS para as mesmas permissões, e qual porta a
-- pessoa usa é uma decisão de quem monta o perfil — não uma conta. Por isso
-- vira uma marcação na tela, que o dono vê e muda.
--
-- Padrão FALSE: perfil que existe hoje continua entrando pelas telas de
-- dentro, como entra. Quem marcar a caixa passa a entrar pelo portal.
-- ============================================================================
ALTER TABLE perfis
    ADD COLUMN IF NOT EXISTS entra_pelo_portal BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN perfis.entra_pelo_portal IS
    'Gente de obra: ao entrar no ERP vai para o portal, não para as telas de '
    'dentro. Decide a PORTA, não o que a pessoa pode — isso continua sendo as '
    'seções marcadas no perfil.';
