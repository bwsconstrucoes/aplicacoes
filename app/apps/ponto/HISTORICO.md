# Ponto eletrônico — HISTÓRICO

Memória da área. Escrito para quem nunca viu a conversa. Ler antes de mexer,
junto com o `README.md` e o `PLANO.md`.

## Pendente AGORA

1. **Juntar na `main`** — espera o "pode" do dono. A junção exige:
   - `PONTO_API_KEY` criada no Render **antes** da junção (sem ela a API
     responde 503, de propósito);
   - `PONTO_DRIVE_PASTA` com o id da pasta do Drive para as fotos, compartilhada
     com `contato@bwsconstrucoes.com.br` (quem a conta de serviço personifica;
     muda em `PONTO_DRIVE_IMPERSONAR`). Sem ela, as fotos esperam na fila do
     banco até a pasta existir — a batida funciona do mesmo jeito;
   - rodar as migrações do ponto logo depois: `POST /ponto/api/admin/migrar`
     com a chave, ou `python -m app.apps.ponto.scripts.migrar --aplicar` no
     Shell;
   - ✔ as 2 linhas no `main.py`, a linha no `CLAUDE.md`, as seções do
     `CONTEXTO.md` e o `PERGUNTAS.md` do ERP **já estão no ramo** desde a
     segunda rodada de 03/10/2026 — o dono pediu para juntar, e sem elas a
     junção não ligaria nada.
2. **Confirmar com a contabilidade/advogado** o que o REP-P exige além do
   software (registro do programa, termo de responsabilidade, AFD/AEJ) antes de
   desligar o Mobponto. Não dá para confirmar daqui.
3. **Fase 2**: PWA de bater ponto e telas no ERP (aprovar aparelho, analisar
   batida EM_ANALISE, decidir ajuste). **Fase 3**: iDFace, AFD/AEJ, espelho,
   horas, Drive.

## 03/10/2026 — Fase 1 entregue no ramo `feature/modulo-ponto`

**O que existe:** schema `ponto` (migração 001), API REST completa, três
scripts (migrar, importar obras, importar colaboradores), 71 testes (puros e
com banco), README. Nada fora da pasta `app/apps/ponto/` e dos dois arquivos de
teste. O serviço de verdade (`app.main`) foi carregado com o blueprint
registrado e todas as rotas respondem; o curinga do encurtador não as engole.

**A especificação de partida foi colada pelo dono de outro chat** (o nome
"Vitor" que apareceu na mensagem dele era erro de ditado — ninguém com esse nome
desenhou nada), e foi confrontada com o repositório antes de qualquer código — o `PLANO.md` registra os achados.
Decisões, com o motivo:

1. **Obras e pessoas são do ERP, não do ponto.** A especificação pedia
   `ponto_obras` e `ponto_colaboradores`. Seria a 2ª cópia de obras e a 3ª de
   pessoas (a Análise de SPs já espelha o Pipefy). O dono decidiu em 18/09/2026
   "100% no ERP, nada de sistema paralelo", e em 03/10/2026 que o ponto é a
   solução definitiva à qual as outras se conectam — as duas coisas combinam:
   cadastro no ERP, ponto como fonte da verdade do PONTO. Preço: amarra entre
   áreas; `test_contrato_com_o_erp` acusa se o ERP renomear uma coluna lida.
   O importador cria no ERP pessoa/obra que não existe (única escrita do ponto
   em tabela do ERP) e **não altera** pessoa que já existe — obra divergente é
   aviso, porque o ERP manda.
2. **Foto no Google Drive, reduzida, opcional.** O disco do Render é apagado
   a cada publicação; "salvar localmente" perderia tudo. A primeira versão
   guardava no banco; o dono corrigiu no mesmo dia: *"as fotos a gente pode
   armazenar no Google Drive, lá o espaço é virtualmente infinito; na base de
   dados não"*. Sobe pela rotina de Drive do ERP (`erp/core/documentos/drive.py`,
   importada), fechada (nunca pública por link), em subpasta por mês, na pasta
   `PONTO_DRIVE_PASTA`. No banco fica só a ficha (hash, tamanho, id no Drive).
   Se o Drive falhar na hora, a batida NÃO falha: os bytes esperam em
   `ponto.fotos.conteudo` até a rota/script de reenvio levá-los. JPEG de
   800 px, teto de 300 KB. Expurgo por prazo é rotina futura (coluna já existe).
3. **Identidade recusa; lugar e relógio vão para análise.** A especificação
   dizia "fora das regras é rejeitada". A Portaria 671 veda impedir a marcação
   do empregado; recusar quem está a 250 m da obra cria passivo. Recusa só por
   aparelho/pessoa/obra inválidos, com registro em `ponto.recusas`.
4. **Token por aparelho, não chave de API no celular.** Acréscimo de
   segurança: chave no PWA vazaria. Token entregue uma vez, hash no banco,
   morre com o bloqueio.
5. **NSR e hash encadeado desde já**, para o AFD da fase 3 não exigir refazer
   tabela com 400 pessoas batendo.
6. **Pessoa sem config no ponto bate com padrões** (PADRAO_4, ativa): o ERP
   diz quem existe; o ponto só afina.
7. **Repetição em 60 s não grava** (toque duplo). **Relógio** do aparelho
   difere >5 min → análise. **Consulta** até 62 dias e 20.000 linhas por
   chamada (instância de 2 GB).
8. **Formato da consulta é o do ponto**, não o do Mobponto (4 colunas por dia).
   Correção do dono no mesmo dia: "não adaptar o que temos ao Mobponto; o
   inverso". A Análise de SPs vai ler o ponto direto no banco (mesmo Postgres)
   quando chegar a hora — a API é para celular, iDFace e sistemas de fora.

**Incidentes durante a entrega** (para não repetir):
- A recusa era gravada na mesma transação da batida recusada e sumia com o
  rollback. Passou a ter transação própria (`marcacoes._recusar`).
- `aprovar`/`bloquear` devolviam o aparelho sem o nome do dono; passaram a usar
  a consulta detalhada.
- O Flask não aceita rota nova num blueprint já registrado; o teste do guarda
  usa um blueprint de mentira com o mesmo `before_request`.
- Os testes com banco gravam de verdade e deixavam três obras `PT-*` no banco
  ao terminar; com a suíte em paralelo, outro arquivo do mesmo trabalhador
  (`test_obra_do_documento_banco.py`) contava obras esperando zero e caía. A
  fixture passou a limpar também na SAÍDA. Regra para teste novo com banco neste
  módulo: o que grava, apaga ao sair.

**O que ficou de fora e por quê:** `conftest.py` não foi tocado: as fixtures
de banco do ponto moram no próprio arquivo de teste. Expurgo de fotos por prazo
e o conector do iDFace ficam para as próximas fases.

**Esclarecimentos dados ao dono em 03/10/2026, para constar:**
- **REP-P** = Registrador Eletrônico de Ponto via Programa. É a categoria da
  Portaria 671/2021 para ponto feito por software (celular, computador), em
  vez da máquina de parede (REP-C). O que a lei pede do REP-P: registrar cada
  marcação com número sequencial sem furo, não permitir alterar nem apagar,
  gerar o arquivo fiscal (AFD) e o espelho (AEJ), dar comprovante ao
  empregado e ter registro do programa com termo de responsabilidade do
  empregador. O schema já nasce com o que os arquivos precisam; os arquivos e
  o registro são fase 3 e assunto da contabilidade.
- **Juntar na `main` É publicar**: o Render publica na hora. Não existe
  "juntar sem publicar" neste repositório.

**Como foi verificado:** `tests/test_ponto.py` + `tests/test_ponto_banco.py`
(71 passando contra Postgres 16 local); os três scripts rodados de ponta a
ponta num banco descartável (simulação e gravação, com linhas boas e ruins
misturadas); `app.main` carregado com o blueprint e as rotas exercitadas sem
`DATABASE_URL` (health responde, chave ausente fecha com 503/401). A suíte
inteira foi rodada em paralelo antes do envio: 157 falhas na primeira rodada,
TODAS por biblioteca que faltava no ambiente de desenvolvimento (dropbox, pypdf,
gspread, erpbrasil…), nenhuma no ponto nem causada por ele; com as bibliotecas
instaladas, os arquivos que falharam passaram todos.
