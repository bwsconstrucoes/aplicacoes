-- ===========================================================================
-- DEVOLVE À ATIVA QUEM TEM SAÍDA MARCADA PARA O FUTURO
--
-- ⚑ Achado por ele na tela, em 17/11/2026, horas depois de ligar o cadastro do
-- ERP como base do ponto:
--
--   "Pessoas que estavam com último dia trabalhado mesmo depois de hoje e
--    estavam na fase Solicitações DP, quando ativei o cadastro do ERP foram
--    considerados desligados."
--
-- Estava certo, e o estrago é dos piores que este sistema pode fazer: **gente
-- cumprindo aviso prévio parou de conseguir bater ponto**. Quem cumpre aviso
-- prévio TRABALHA, e a jornada dela é registro legal do REP-P — não pode ter
-- buraco.
--
-- A CAUSA: quatro lugares do ERP escreviam "DESLIGADO" ao ver que EXISTIA uma
-- data de saída, sem olhar se ela já havia chegado. Uma saída marcada para a
-- semana que vem virava saída de ontem. O caminho por onde a maioria passou
-- foi a importação da planilha do DP: na fase "Solicitações DP", data de saída
-- preenchida virava situação DESLIGADO na hora.
--
-- Por que só apareceu agora: com a planilha como base, o PONTO perguntava a
-- data ("saída menor que hoje"); com o cadastro do ERP, ele passou a acreditar
-- na caixinha. A caixinha estava errada desde a importação — a troca de base
-- só tornou isso visível.
--
-- Daqui em diante não nasce mais: `ja_saiu` em `core/cadastros/pessoas.py` é a
-- régua única, e o ponto voltou a perguntar a data nas DUAS bases. Esta
-- migração arruma quem já estava assim, e roda UMA vez.
--
-- ⚠️ A RÉGUA É "MENOR QUE HOJE", igual à do ponto, e é de propósito: **a
-- pessoa trabalha o último dia**. Saída marcada para hoje ainda é gente ativa
-- hoje.
--
-- ⚠️ O QUE ESTA MIGRAÇÃO PODE TRAZER DE VOLTA SEM DEVER: alguém que REALMENTE
-- saiu e cuja data de saída foi digitada errada (2027 em vez de 2026, por
-- exemplo) volta para a ativa. Isso é erro de cadastro, e aqui ele passa de
-- invisível a visível — a pessoa reaparece nas listas e no ponto. O rastro de
-- cada mudança fica em `eventos`, com a ação SITUACAO_CORRIGIDA.
-- ===========================================================================

-- 1) O RASTRO PRIMEIRO, enquanto ainda se sabe quem era quem.
INSERT INTO eventos (entidade_tipo, entidade_id, usuario_id, acao, detalhe)
SELECT 'colaborador', c.id, NULL, 'SITUACAO_CORRIGIDA',
       jsonb_build_object(
         'de', 'DESLIGADO', 'para', 'ATIVO',
         'etapa_antes', COALESCE(c.etapa, 'CONCLUIDO'),
         'data_saida', c.demissao,
         'ultimo_dia', c.ultimo_dia,
         'motivo', 'a data de saída ainda não chegou: a pessoa está cumprindo '
                || 'aviso prévio, trabalha, bate ponto e entra na folha '
                || '(migração 109)')
  FROM colaboradores c
 WHERE UPPER(COALESCE(c.situacao, 'ATIVO')) = 'DESLIGADO'
   AND c.demissao IS NOT NULL
   AND c.demissao >= CURRENT_DATE;

-- 2) A SITUAÇÃO: é ela que decide folha e ponto.
UPDATE colaboradores
   SET situacao = 'ATIVO'
 WHERE UPPER(COALESCE(situacao, 'ATIVO')) = 'DESLIGADO'
   AND demissao IS NOT NULL
   AND demissao >= CURRENT_DATE;

-- 3) A ETAPA de quem ficou na coluna "Desligado" do quadro.
--
--    Elas não podem ficar lá: a etapa é a FILA DE TRABALHO do DP, e cadastro
--    de gente que está trabalhando, com desligamento em andamento, é trabalho
--    pendente — é exatamente para isso que a etapa "Solicitações"
--    (`NA_CONTABILIDADE`) existe. Palavras dele, em 17/11/2026: *"alguém que
--    saiu e tem cadastro por finalizar É trabalho pendente do DP."*
--
--    Quem já estava em "Solicitações" (o caso da importação) não se mexe.
--    "Cancelado" também não: é cadastro que nunca virou gente na empresa.
UPDATE colaboradores
   SET etapa = 'NA_CONTABILIDADE'
 WHERE UPPER(COALESCE(situacao, 'ATIVO')) = 'ATIVO'
   AND demissao IS NOT NULL
   AND demissao >= CURRENT_DATE
   AND UPPER(COALESCE(etapa, 'CONCLUIDO')) = 'DESLIGADO';

-- 4) E a linha no histórico de etapas, para a ficha da pessoa contar a história.
INSERT INTO colaborador_etapas (colaborador_id, de, para, motivo, automatico, usuario_id)
SELECT c.id, 'DESLIGADO', 'NA_CONTABILIDADE',
       'saída marcada para o futuro: a pessoa está em aviso prévio (migração 109)',
       TRUE, NULL
  FROM colaboradores c
 WHERE UPPER(COALESCE(c.situacao, 'ATIVO')) = 'ATIVO'
   AND c.demissao IS NOT NULL
   AND c.demissao >= CURRENT_DATE
   AND UPPER(COALESCE(c.etapa, 'CONCLUIDO')) = 'NA_CONTABILIDADE'
   AND EXISTS (SELECT 1 FROM eventos e
                WHERE e.entidade_tipo = 'colaborador' AND e.entidade_id = c.id
                  AND e.acao = 'SITUACAO_CORRIGIDA'
                  AND e.detalhe ->> 'etapa_antes' = 'DESLIGADO');
