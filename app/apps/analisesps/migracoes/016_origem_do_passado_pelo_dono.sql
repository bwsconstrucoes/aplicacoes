-- ---------------------------------------------------------------------------
-- O QUE O DONO SABE E O BANCO NAO — 15/09/2026
--
-- Ele contou, e isto e informacao que nenhuma consulta produz:
--
--   *"Em relacao as notas, so pra explicar: tudo que ja tem, que foi
--   importado, e tudo da planilha. So nao foi importado em relatorio dentro do
--   Analise de SPs."*
--
-- Ou seja: TODA nota que ja estava aqui antes de hoje veio pela porta do
-- relatorio (a aba da planilha). A migracao 015 marcou as que davam para
-- afirmar pelo dado — as que tinham destinatario preenchido. O resto ficou
-- vazio porque o BANCO nao sabia; o dono sabe.
--
-- ⚠️ A DATA E A LINHA DIVISORIA, e ela e defensavel: a busca na Receita NUNCA
-- gravou uma nota antes de 15/09/2026. Todas as tentativas anteriores falharam
-- — primeiro no certificado (base64), depois na chamada da biblioteca, depois
-- no evento que derrubava o lote. Isso esta registrado no proprio ponteiro da
-- busca, que so passou a contar documentos hoje.
--
-- Entao:
--   entrou ANTES de 15/09/2026           =>  'fsist'   (o dono afirma)
--   entrou HOJE, sem destinatario        =>  'receita' (so a busca grava assim)
--
-- O UNICO CASO QUE ESTA REGRA PODE ERRAR e uma nota que o relatorio tenha
-- trazido HOJE sem destinatario preenchido — ela seria marcada como da
-- Receita. E uma janela de horas, e o erro se conserta sozinho: na proxima
-- passagem do relatorio ela vira 'receita+fsist'.
-- ---------------------------------------------------------------------------
UPDATE analisesps.notas_fiscais
   SET origem = 'fsist'
 WHERE coalesce(origem, '') = ''
   AND importada_em < TIMESTAMPTZ '2026-09-15 00:00:00-03';

UPDATE analisesps.notas_fiscais
   SET origem = 'receita'
 WHERE coalesce(origem, '') = ''
   AND importada_em >= TIMESTAMPTZ '2026-09-15 00:00:00-03'
   AND coalesce(destinatario_doc, '') = ''
   AND coalesce(destinatario, '') = '';

-- ---------------------------------------------------------------------------
-- E O INDICE QUE FALTAVA PARA "NOTA SEM LANCAMENTO"
--
-- A pergunta "esta nota tem lancamento?" compara os digitos da chave do diario
-- com a chave da nota. O indice que existia era sobre a coluna crua
-- (`ix_fiscal_chave`), e a consulta usa `regexp_replace` — o Postgres nao
-- consegue usar um pelo outro, entao cada pergunta varria o diario inteiro.
--
-- Com o KPI do alto da tela, essa conta passa a rodar a CADA abertura da
-- planilha das notas. Sem este indice, seria uma varredura por visita.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ix_fiscal_chave_digitos
    ON analisesps.sp_fiscal_analise
       (regexp_replace(coalesce(chave, ''), '\D', '', 'g'));
