-- ===========================================================================
-- 059 — Busca DENTRO dos documentos arquivados
--
-- PARA QUE SERVE. O assistente responde sobre o que está no BANCO (números).
-- Esta migração abre a porta para ele responder sobre o que está nos
-- DOCUMENTOS: "o que o contrato da Creche diz sobre reajuste?". O texto já era
-- extraído e guardado na hora de arquivar; o que faltava era conseguir
-- PROCURAR nele sem varrer tudo.
--
-- POR QUE BUSCA DE TEXTO DO POSTGRES, E NÃO ÍNDICE POR SIGNIFICADO (vetor).
-- Decisão do dono em 11/09/2026: *"vamos começar do simples, depois a gente
-- decide se parte pro caro"*. O Postgres já tem dicionário de PORTUGUÊS
-- embutido — ele entende que "reajustar", "reajuste" e "reajustado" são a
-- mesma palavra, e ignora as palavras de ligação. Resolve bem a pergunta feita
-- com as palavras que ESTÃO no documento. O índice por significado acerta
-- também quando a pergunta usa outras palavras, e custa dinheiro por
-- documento indexado — fica para depois, se a medição mostrar que faz falta.
--
-- A COLUNA É GERADA PELO BANCO (`GENERATED ALWAYS`), e isso é de propósito:
-- não há como alguém gravar texto sem atualizar o índice, porque não existe o
-- passo de "atualizar o índice". Documento novo já nasce procurável.
--
-- O peso A/B faz o NOME e o RESUMO valerem mais que o corpo: quem procura
-- "contrato da creche" quer o contrato da creche, não toda página que
-- menciona a creche de passagem.
-- ===========================================================================

ALTER TABLE documentos
  ADD COLUMN IF NOT EXISTS busca tsvector
  GENERATED ALWAYS AS (
      setweight(to_tsvector('portuguese', coalesce(nome_padronizado, '')), 'A')
   || setweight(to_tsvector('portuguese', coalesce(referencia, '')),       'A')
   || setweight(to_tsvector('portuguese', coalesce(resumo, '')),           'B')
   || setweight(to_tsvector('portuguese', coalesce(texto, '')),            'C')
  ) STORED;

COMMENT ON COLUMN documentos.busca IS
  'Índice de busca em português, montado pelo próprio banco a partir do nome, '
  'da referência, do resumo e do texto extraído. Usado pelo assistente para '
  'responder sobre o que está escrito nos documentos, sempre citando o trecho.';

CREATE INDEX IF NOT EXISTS idx_documentos_busca ON documentos USING GIN (busca);
