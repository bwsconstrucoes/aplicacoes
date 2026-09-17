-- ============================================================================
-- Migração 070 — a ÁRVORE DE PASTAS do Google Drive
--
-- O PEDIDO, do dono em 17/09/2026:
--
--   "Essa documentação é algo que eventualmente a gente pode precisar
--   visualizar no computador. Então a minha ideia é que tivesse tudo no Google
--   Drive, separado numa pasta de obra (…) tem a pasta Obras, aí tem as
--   subpastas. E aqueles outros documentos, dentro da pasta do ERP, salvar em
--   outra pasta, tipo Arquivo (…) é importante, senão fica bagunçado. E a
--   gente não ocupa espaço na base de dados, que é o mais importante."
--
-- Ele ligou o Drive na mesma conversa — os documentos novos já vão para lá, e
-- os antigos estão sendo movidos. Sem esta árvore, tudo cai numa pasta só.
--
-- O QUE ESTA TABELA É, e por que ela existe
--
-- Um caderninho de "que pasta é essa": para cada lugar da árvore, o id que o
-- Google devolveu quando ela foi criada. Sem isso, cada arquivo guardado
-- exigiria uma busca no Drive para descobrir onde cai — chamada de rede a mais
-- em cima de quem está esperando a tela, e cota gasta à toa.
--
-- A chave é o que o ERP conhece ("obra:12", "raiz:Obras", "arquivo:Pessoas"),
-- não o caminho no Drive: se alguém renomear a pasta lá, o id continua valendo
-- e nada se perde. Apagar uma linha daqui não apaga nada no Drive — só faz o
-- sistema procurar (ou criar) de novo na próxima vez.
-- ============================================================================

CREATE TABLE IF NOT EXISTS drive_pastas (
    chave       TEXT PRIMARY KEY,
    file_id     TEXT NOT NULL,
    nome        TEXT,
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE drive_pastas IS
    'De-para entre um lugar da árvore do ERP ("obra:12", "raiz:Obras") e o id '
    'da pasta correspondente no Google Drive. Apagar uma linha não apaga nada '
    'no Drive: o sistema procura ou recria na próxima vez.';
