-- ---------------------------------------------------------------------------
-- DE ONDE VIERAM AS NOTAS QUE JA ESTAVAM AQUI — 15/09/2026
--
-- Relato do dono, minutos depois de aplicar a migracao 014: *"tem algo errado
-- com a atualizacao do banco agora. Se eu boto todas, aparecem as notas aqui,
-- seis mil e tantas. Se eu clico so as da Receita, nao aparece nada. Se eu
-- clico so as do relatorio, nao aparece nada."*
--
-- NAO ESTAVA ERRADO, MAS ESTAVA INUTIL — e para quem usa isso e a mesma coisa.
-- A coluna `origem` nasceu vazia para as 6 mil notas que ja existiam, porque
-- vazio e honesto: ninguem registrou por onde elas entraram. So que uma tela
-- com dois recortes que nao devolvem NADA nao se le como "ainda nao sei"; se
-- le como "quebrou".
--
-- O QUE DA PARA AFIRMAR, e so isto: a busca na Receita NAO PREENCHE o
-- destinatario (o resumo que ela entrega nao traz), e o relatorio do FSist
-- traz — e uma coluna propria para ele. Entao:
--
--   destinatario preenchido  =>  veio do relatorio. Certeza.
--   destinatario vazio       =>  nao da para saber. Fica vazio MESMO.
--
-- ⚠️ A REGRA SO ANDA PARA UM LADO DE PROPOSITO. Ela nunca marca como 'fsist'
-- uma nota que a busca trouxe (essas tem destinatario vazio, sempre), entao no
-- pior caso ela deixa de marcar — nao mente. Preencher o resto com chute faria
-- a tela responder com confianca uma pergunta que ninguem sabe responder, e
-- este modulo inteiro existe para o contrario disso.
--
-- As notas que sobrarem sem origem ganham a marca na proxima vez que uma das
-- duas portas passar por elas.
-- ---------------------------------------------------------------------------
UPDATE analisesps.notas_fiscais
   SET origem = 'fsist'
 WHERE coalesce(origem, '') = ''
   AND (coalesce(destinatario_doc, '') <> '' OR coalesce(destinatario, '') <> '');
