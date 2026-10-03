# Ponto eletrônico próprio (REP-P) — `app/apps/ponto/`

O sistema de ponto da BWS, feito em casa, para substituir o Mobponto. Portaria
671/2021. ~400 pessoas em obras de construção civil. **É a fonte da verdade do
ponto**: a folha, a Análise de SPs e qualquer outro sistema se adaptam a ele, e
não o contrário (decisão do dono, 03/10/2026).

Batidas chegam por três caminhos: celular (PWA, fase futura), equipamentos
Control iD iDFace (fase futura) e lançamento manual. **A fase 1 é só schema, API
e importadores** — não há tela.

## Onde cada coisa mora

```
__init__.py              exporta `bp` (import leve: só Flask)
routes.py                o blueprint /ponto — só valida entrada e chama o core
auth.py                  chave de API, token de aparelho, o guarda das rotas
db.py                    conexão: REUSA a engine do ERP; schema `ponto`
horario.py               America/Fortaleza; a regra do dia de trabalho por jornada
erros.py                 os erros e o status HTTP de cada um
migracoes_runner.py      aplica os .sql de `migracoes/` (nunca no start)
migracoes/001_ponto_base.sql
core/cadastros.py        obras e pessoas: lê o ERP, afina com as tabelas do ponto
core/dispositivos.py     registrar, aprovar, bloquear, autorizar; regra por perfil
core/geo.py              distância e cerca (função pura)
core/marcacoes.py        a batida: validações, NSR, hash encadeado, foto, ajuste
core/consultas.py        marcações por período, no formato do ponto
core/fotos.py            base64 → JPEG reduzido → Google Drive (ficha no banco)
core/recusas.py          batida recusada vira registro
core/importacao.py       leitura de xlsx/csv e as regras dos importadores
scripts/migrar.py                    estado / aplicar migrações
scripts/importar_obras.py            planilha → ERP obras + ponto.obra_config
scripts/importar_colaboradores.py    planilha → ERP colaboradores + ponto.colaborador_config
scripts/enviar_fotos.py              leva ao Drive as fotos que ficaram na fila
PLANO.md                 o plano aprovado, com as decisões e o porquê
HISTORICO.md             a memória da área — leia antes de mexer
```

Testes: `tests/test_ponto.py` (regras puras, sem banco) e
`tests/test_ponto_banco.py` (fluxo inteiro, `@pytest.mark.banco`).

## O desenho em cinco frases

1. **Obra e pessoa são do ERP.** `public.obras` (com latitude/longitude) e
   `public.colaboradores` (CPF único, obra principal, situação). O ponto guarda
   só o que o ERP não tem: raio da cerca, tipo de jornada, obras adicionais.
   Pessoa sem linha no ponto bate com os padrões (PADRAO_4, ativa).
2. **Identidade recusa; lugar e relógio vão para análise.** Aparelho não
   aprovado, pessoa não autorizada, pessoa desligada, obra encerrada → 403 e
   linha em `ponto.recusas`. Fora da cerca, obra sem coordenada, celular sem
   localização, relógio errado, pessoa afastada, obra fora da lista da pessoa →
   batida ACEITA com status `EM_ANALISE` e o motivo. A Portaria 671 veda impedir
   a marcação do empregado.
3. **A hora oficial é a do servidor**, em UTC no banco e em Fortaleza na saída.
   `data_referencia` é o dia de trabalho: para `VIGIA_NOTURNO_2`, batida antes
   das 10h pertence ao dia anterior.
4. **NSR sem furo e hash encadeado** em toda marcação, sob trava do Postgres.
   Marcação não se altera nem se apaga; ajuste aprovado (fase 2) gera marcação
   nova com status `AJUSTADA` apontando para a original.
5. **Mesma pessoa em menos de 60 s não gera batida nova**: a resposta devolve a
   que já existe, com `repetida: true`.
6. **A foto vai para o Google Drive**, fechada, em subpasta por mês, pela
   rotina de Drive do ERP. No banco fica a ficha (hash, tamanho, id no Drive).
   Se o Drive falhar na hora, a batida não falha: a foto espera na fila do
   banco e a rota de reenvio (ou o script) a leva depois. O `health` mostra
   quantas esperam.

## Variáveis de ambiente

| Variável | Para quê |
|---|---|
| `DATABASE_URL` | a do ERP; o ponto não tem conexão própria |
| `PONTO_API_KEY` | a chave dos sistemas (`X-API-Key`). **Sem ela, toda rota com chave responde 503** — falha fechado. Gere com `python -c "import secrets; print(secrets.token_urlsafe(32))"` e guarde só no Render |
| `PONTO_DRIVE_PASTA` | id da pasta do Google Drive onde as fotos de batida ficam (subpastas `AAAA-MM` são criadas sozinhas). Pasta comum serve, desde que compartilhada com o e-mail personificado. Sem ela, as fotos esperam na fila do banco. **Pode ser uma pasta `Ponto` dentro da pasta do Drive do ERP** (conferido em 03/10/2026): o ERP só procura e mexe nas pastas `Obras` e `Arquivo` e nos arquivos que ele mesmo registrou; não lista nem move o resto. Só não use os nomes `Obras` ou `Arquivo` |
| `PONTO_DRIVE_IMPERSONAR` | em nome de quem a conta de serviço grava (padrão `contato@bwsconstrucoes.com.br`, o mesmo da emissão de NFS-e e da Análise de SPs). Vazio = a própria conta de serviço, que só funciona em Drive Compartilhado |
| `GOOGLE_CREDENTIALS_BASE64` | a credencial Google de toda a casa; nada novo |

## Endpoints (todos em JSON `{"ok": true|false, ...}`)

| Rota | Credencial | O que faz |
|---|---|---|
| `GET /ponto/health` | nenhuma | módulo no ar, fuso, chave configurada, migrações pendentes |
| `POST /ponto/api/dispositivo/registrar` | nenhuma (teto de 30/h por IP) | `{device_uuid, descricao}` → aparelho PENDENTE e o **token** (mostrado uma vez). Repetir o mesmo uuid não gera token novo |
| `POST /ponto/api/marcacao` | `X-Device-Token` + `device_uuid` (celular) **ou** `X-API-Key` (iDFace, manual) | `{cpf, obra, latitude, longitude, timestamp_dispositivo, foto_base64, origem, registrado_por}` → 201 com a marcação; 200 se repetida; 400 entrada ruim (`campo`); 403 recusada |
| `GET /ponto/api/marcacoes?data_inicio=&data_fim=&cpf=&obra=&status=` | chave | até 62 dias; `marcacoes` (uma linha por batida) + `dias` (resumo pessoa/dia) |
| `GET /ponto/api/colaboradores?obra=&todos=1` | chave | pessoas ativas no ponto, com obra principal e adicionais |
| `GET /ponto/api/obras?todas=1` | chave | obras ativas com coordenada e raio |
| `GET /ponto/api/dispositivos?status=` | chave | a fila (PENDENTE / APROVADO / BLOQUEADO) |
| `POST /ponto/api/dispositivos/<id>/aprovar` | chave | `{perfil, aprovado_por, cpf (dono, se INDIVIDUAL), autorizados: [cpfs], obras: [códigos], descricao}` |
| `POST /ponto/api/dispositivos/<id>/bloquear` | chave | `{motivo, por}` — mata o token do aparelho |
| `POST /ponto/api/dispositivos/<id>/autorizar` | chave | troca `autorizados` e/ou `obras` |
| `POST /ponto/api/ajustes` | chave | pedido de ajuste com justificativa (decisão é fase 2) |
| `GET /ponto/api/recusas?limite=` | chave | as batidas recusadas e o motivo |
| `GET /ponto/api/admin/migracoes` · `POST /ponto/api/admin/migrar` | chave | estado / aplicar migrações do schema `ponto` |
| `POST /ponto/api/admin/fotos/enviar-pendentes?limite=50` | chave | leva ao Drive as fotos que ficaram na fila |

`obra` aceita número, código ou nome exato. CPF com ou sem máscara.

## Importadores

```
python -m app.apps.ponto.scripts.migrar --aplicar
python -m app.apps.ponto.scripts.importar_obras obras.xlsx            # simula
python -m app.apps.ponto.scripts.importar_obras obras.xlsx --gravar
python -m app.apps.ponto.scripts.importar_colaboradores pessoas.xlsx --gravar
```

Colunas reconhecidas sem acento/caixa. Obras: `codigo, nome, centro_custo,
latitude, longitude, raio_metros`. Pessoas: `cpf, nome, obra, centro_custo,
tipo_jornada`. Linha com erro é listada e não impede as boas. Reexecutar não
duplica. Pessoa/obra que não existe no ERP é **criada** lá com o mínimo; pessoa
que existe não tem nada do ERP alterado (obra divergente vira aviso: o ERP
manda). Coordenada que já existe no ERP só troca com `--sobrescrever-coordenadas`.

## Perguntas que o ponto passa a responder (para o assistente do ERP)

Também estão no `app/apps/erp/PERGUNTAS.md`, seção "Ponto eletrônico".

- Quem bateu ponto hoje na obra X? *(pela `data_referencia`, não pela hora — vigia noturno conta no dia anterior)*
- Quantos dias o Fulano trabalhou em setembro? *("trabalhou" = dia com batida; a regra de presença/jornada é fase 3)*
- Quais batidas estão em análise, e por quê?
- Quais aparelhos estão pendentes de aprovação?
- Quantas batidas foram recusadas esta semana, e de que aparelho?
- Qual a última batida da Maria? Em que obra?
- **Ainda não responde:** horas trabalhadas, atrasos, faltas, banco de horas, espelho de ponto — fase 3. "Quem está na obra agora" — precisa da regra de entrada/saída por jornada, fase 3.

## O que NÃO está na fase 1

PWA e tela de bater; conector do iDFace (a API já aceita `origem=IDFACE`);
telas no ERP (aprovar aparelho, analisar batida, decidir ajuste); AFD/AEJ e
comprovante do empregado; espelho de ponto e horas; expurgo automático de fotos
do Drive pelo prazo de guarda (a coluna `expurgada_em` já existe).
