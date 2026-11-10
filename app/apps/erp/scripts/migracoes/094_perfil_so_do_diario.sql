-- ===========================================================================
-- 094 — UM PERFIL SÓ PARA O DIÁRIO DE OBRA
--
-- Pergunta dele em 09/11/2026: *"a gente pode ter um perfil só para diário de
-- obras, só para preenchimento de diário de obra?"*
--
-- Pode, e agora já vem pronto. Quem receber este perfil entra no ERP, enxerga
-- a tela do Diário de obra e nada mais: nem título, nem pagamento, nem
-- cadastro, nem pessoal.
--
-- ⚑ POR QUE ISSO É SEGURO, e não um buraco: o perfil só marca a seção
-- `obr_diario`. Todas as outras ficam no padrão NADA — perfil não concede o
-- que não está marcado. A ação `ver_erp` (a porta de entrada) é dada
-- automaticamente a quem tem qualquer seção acima de NADA; sem ela a pessoa
-- não conseguiria nem abrir o ERP para chegar ao diário.
--
-- ⚑ E AS OBRAS CONTINUAM SENDO DO CADASTRO DA PESSOA, não do perfil. Duas
-- pessoas com este mesmo perfil alcançam obras diferentes, conforme o que foi
-- marcado no cadastro de cada uma. É a regra de 13/09/2026, e é ela que faz o
-- encarregado da obra 081 não enxergar o diário da 094.
--
-- NÃO É "de_sistema": ele pode ser editado e, se ele não servir, apagado na
-- tela de Configurações › Perfis. Os perfis de sistema são os que espelham os
-- cargos antigos e não podem sumir sem deixar gente sem perfil.
--
-- A LIÇÃO DA MIGRAÇÃO 092 ESTÁ APLICADA AQUI: nada de `JOIN perfis ON nome`
-- com nome chutado. O id do perfil vem do próprio INSERT, com `RETURNING`.
-- Lá, um nome errado ("Parceiro" em vez de "Parceiro da obra") fez o INSERT
-- não inserir nada e não dar erro nenhum.
-- ===========================================================================

WITH novo AS (
    INSERT INTO perfis (nome, descricao, de_sistema, ativo)
    VALUES ('Diário de obra',
            'Só preenche o diário de obra das obras marcadas no cadastro da '
            'pessoa. Não enxerga financeiro, suprimentos nem pessoal.',
            FALSE, TRUE)
    ON CONFLICT (nome) DO NOTHING
    RETURNING id
)
INSERT INTO perfil_secoes (perfil_id, secao, nivel)
SELECT id, 'obr_diario', 'EDITAR' FROM novo
ON CONFLICT (perfil_id, secao) DO UPDATE SET nivel = EXCLUDED.nivel;
