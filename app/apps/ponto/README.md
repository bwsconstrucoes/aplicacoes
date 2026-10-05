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
core/apuracao.py         o cálculo do dia pela CLT: falta, atraso, extra, intervalo, noturno (função pura)
core/marcacoes.py        a batida: validações, NSR, hash encadeado, foto, ajuste
core/consultas.py        marcações por período, no formato do ponto
core/fotos.py            base64 → JPEG reduzido → Google Drive (ficha no banco)
core/recusas.py          batida recusada vira registro
core/importacao.py       leitura de xlsx/csv e as regras dos importadores
scripts/migrar.py                    estado / aplicar migrações
scripts/importar_obras.py            planilha → ERP obras + ponto.obra_config
scripts/importar_colaboradores.py    planilha → ERP colaboradores + ponto.colaborador_config
scripts/enviar_fotos.py              leva ao Drive as fotos que ficaram na fila
gestao.py                as telas e consultas da gestão, dentro do ERP (fase 2)
app_colaborador.py       o "Meu ponto" do celular, em /ponto/app (fase 2)
core/escalas.py · feriados.py · espelho.py · painel.py · ocorrencias.py · banco.py
core/competencias.py · alertas.py · rotina.py · acesso.py · documentos.py · leitura_atestado.py
PLANO.md                 o plano aprovado da fase 1, com as decisões e o porquê
PLANO_FASE2.md           gestão, app do colaborador, ocorrências, banco de horas e alertas
HISTORICO.md             a memória da área — leia antes de mexer
```

Testes: `tests/test_ponto.py` (regras puras, sem banco) e
`tests/test_ponto_banco.py` (fluxo inteiro, `@pytest.mark.banco`).

## O desenho em cinco frases

1. **Obra e pessoa são do ERP.** `public.obras` (com latitude/longitude) e
   `public.colaboradores` (CPF único, obra principal, situação). O ponto guarda
   só o que o ERP não tem: raio da cerca, tipo de jornada, obras adicionais.
   Pessoa sem linha no ponto bate com os padrões (PADRAO_4, ativa).
2. **Identidade e lugar recusam; relógio e cadastro vão para análise.**
   Aparelho não aprovado, pessoa não autorizada, pessoa desligada, obra
   encerrada → 403 e linha em `ponto.recusas`. **Desde 04/10/2026 (decisão do
   dono), fora da área de qualquer obra também é 403** — e a obra da batida é a
   da cerca em que o aparelho está, detectada sozinha (ver a seção "A cerca que
   bloqueia"). Relógio errado, pessoa afastada, obra fora da lista da pessoa,
   obra sem coordenada, GPS na borda da cerca, tablet sem localização ou sem
   foto → batida ACEITA com status `EM_ANALISE` e o motivo.
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

## Fase 2 — a gestão no ERP e o "Meu ponto" do celular (03/10/2026)

**Gestão: ERP › Ponto** (rotas em `gestao.py`, penduradas no blueprint do ERP;
telas em `templates/ponto/gestao.html`). Sete abas:

| Aba | O que faz |
|---|---|
| Hoje | por obra: quem bateu, quem não bateu, afastados, folga; atualiza sozinha |
| Espelho | o mês da pessoa, dia a dia; ajuste, compensação, afastamento; imprimir para assinar; CSV para a folha |
| Validações | tudo o que espera decisão, numa lista com filtros (ver "Validações", abaixo) |
| Pessoas | escala com data de início, jornada, obras adicionais, banco de horas (DP) |
| Banco de horas | saldo, meses, vencimentos, lançamentos (DP) |
| Alertas | regras da CLT e padrões do histórico; resolver/dispensar; resumo do dia |
| Configuração | atualizações do banco do ponto, escalas, feriados, fechamento do mês, aparelhos, telefones do resumo, fila do Drive, endereço do app |

**Quem pode o quê** — ações do ERP, ajustáveis em Configurações › Perfis (área
"Ponto") e pessoa a pessoa:

| Ação | Padrão por cargo | Libera |
|---|---|---|
| `ver_ponto` | admin, diretor, gestor, supervisor, administrativo de obra, DP | olhar, sempre nas obras da pessoa |
| `tratar_ponto` | admin, gestor, supervisor, DP | batida em análise, ajuste de batida, 1ª etapa de compensação/folga, escala, alertas |
| `aprovar_afastamento` | admin, DP | atestado, licença, férias; ABRIR o atestado; banco de horas; 2ª etapa |
| `fechar_competencia` | admin, DP | fechar e reabrir o mês |
| `configurar_ponto` | admin, DP | escalas, feriados, aparelhos, resumo, atualizações |

⚠️ O DP só enxerga todo mundo se o cadastro dele no ERP estiver com **"todas as
obras"** marcado — o recorte por obra é do ERP e vale para o ponto também.

**Fluxo dos pedidos** (`core/ocorrencias.py`): atestado/licença/férias → DP;
ajuste de batida → supervisor; compensação e folga do banco → supervisor e
depois DP. Negar exige motivo. Mês fechado não aceita pedido nem decisão.

**Meu ponto: `/ponto/app`** (`app_colaborador.py`, `templates/ponto/app.html`).
No celular da pessoa: CPF + PIN (criado com código de 6 números pelo WhatsApp do
cadastro do ERP; 5 erros bloqueiam 15 min); bater com foto e localização (a obra
mais perto vem escolhida); comprovante de cada batida; "Meu mês"; pedidos com
foto do atestado. No **tablet da obra** (aparelho COMPARTILHADO aprovado): só
bater, com CPF — não mostra nada de ninguém. Todo aparelho novo espera
aprovação em Pendências; o celular se identifica como "Celular de Fulano" depois
que a pessoa entra.

**Regras novas do cálculo** (`core/apuracao.py`, `espelho.py`, `banco.py`):
tolerância de 5 min por batida e 10 no dia (passou, conta tudo — Súmula 366);
intervalo mínimo; extra acima de 2 h; menos de 11 h entre jornadas; hora noturna
reduzida; 12x36 atravessando a meia-noite; batida faltando não vira extra nem
débito; sem escala não se julga falta; falta de dia inteiro não entra no banco;
o crédito mais antigo do banco é o primeiro a ser usado.

**Rotina do dia** (`core/rotina.py`): a primeira requisição do ponto depois das
6h gera os alertas e manda o resumo por WhatsApp aos telefones da Configuração —
uma vez por dia, numa linha separada, sem ninguém apertar botão.

## A base de pessoas: o Registro de Colaboradores (04/10/2026)

Pedido do dono: *"por enquanto utilizar como base de colaboradores a planilha de
Registro de Colaboradores (…) tem critério de uso dela de exibição no processo de
Análise de SPs."* (`core/registro.py`)

- **De onde:** a cópia que a Análise de SPs guarda no banco (`analisesps.colaborador`,
  atualizada pelo botão "Atualizar cadastro" de lá). O ponto só lê.
- **O que vem de lá:** nome, celular (WhatsApp do PIN e do QR), cargo (a função no
  tablet), obra pelo código, admissão, saída e a situação.
- **O critério, igual ao da Análise de SPs:** DESLIGADO = data de saída já chegou ou
  "Fase Atual" com "desligad" → batida recusada; AFASTADO = fase com "afastad" →
  bate, para conferência; o resto, ATIVO (aviso prévio inclusive).
- **Quem está só no ERP:** bate, mas para conferência ("fora do Registro de
  Colaboradores").
- **Quem está só no Registro:** precisa existir no ERP para ter onde pendurar a
  batida — botão "Cadastrar no ERP os que faltam" na Configuração (nome, CPF, obra;
  CPF com dígito errado fica de fora).
- **Chave** na Configuração para voltar ao cadastro do ERP. Sem a cópia no banco, o
  ponto usa o ERP sozinho.
- **A base se mantém sozinha** (05/10/2026, "é algo que precisa ser contínuo"): de
  2 em 2 horas, das 6h às 20h, o ponto pede à Análise de SPs a tarefa do botão
  "Atualizar cadastro" (`tarefas.disparar("colaboradores")`, processo separado,
  com a trava de lá); e a cada 15 minutos cadastra no ERP quem falta. Acordado
  pelas requisições do ponto. Chave "Manter a base em dia sozinha" na Configuração.
- **Período de contrato:** antes da data de início (a MENOR entre "Data de Início"
  e "Data de Admissão" — o diarista começa antes da carteira) e depois da data de
  saída, a batida é recusada (no tablet, já na identificação); o próprio dia da
  saída ainda se bate. Pedido de ajuste em dia fora do contrato também é recusado.

## Validações: tudo o que espera alguém, e quem valida o quê (04/10/2026)

**Quem valida** (`core/validacao.py`, Ponto › Configuração › Quem valida cada
pedido): para ajuste de batida, compensação, folga do banco, licença e batida em
conferência, escolhe-se **Encarregado da obra**, **DP**, ou **Encarregado e
depois o DP**. **Padrão: DP em tudo** (sugestão do dono). Fixos: atestado e
afastamento só no DP (saúde); férias e abono nascem no DP; o mosaico é do
responsável de cada obra. Trocar a regra realinha a fila: o que esperava uma
etapa que deixou de existir passa para quem valida agora.

**A aba Validações** (antes "Pendências"; `core/validacoes.py`,
`GET /erp/api/ponto/validacoes`): pedidos, batidas em conferência, mosaicos
obrigatórios sem conferência e aparelhos esperando, numa lista só, do mais antigo
para o mais novo. Filtros: obra, período, pessoa (nome ou CPF), tipo e "só o que
eu valido". Cada linha diz quem valida, há quantos dias espera, e traz os botões
de quem pode decidir; aprovar em lote; negar e rejeitar pedem motivo, um por um.
Batida em conferência tem duas rotas — `/decidir` (encarregado) e `/decidir-dp`
(DP) — e só a da regra em vigor aceita.

## Ajuste de batida a partir do dia, e a batida fora do normal (04/10/2026)

**"Corrigir este dia"** (`core/ajustes.py`): no "Meu mês", o dia com batida
faltando tem o botão, e o pedido abre com o que já foi batido, o que a escala
previa e **só o que falta** ("Volta do intervalo — 12:00"), com o horário
sugerido para conferir. A pessoa marca, escolhe o motivo (celular quebrado ou
sem bateria, sem internet, aparelho da obra com problema, esqueci, serviço fora
da obra, outro) e manda: um pedido por horário, todos de uma vez ou nenhum
(`POST /ponto/app/api/pedidos/ajuste-do-dia`). Na aba Pedidos, "Esqueci de bater"
pede só o dia e abre o mesmo quadro. O servidor recusa o que no Pipefy passava:
horário a menos de 30 min de batida que já existe ou de pedido esperando, dia já
completo, dia justificado por atestado/férias/licença, dia ou horário futuro, mês
fechado. Quem decide é o encarregado (`tratar_ponto`); aprovado, a batida entra
como AJUSTADA, e o espelho original continua lá.

**Batida fora do normal**: quando a obra não é detectada e a lista aparece, a tela
avisa em amarelo e, ao bater, pede a explicação (mínimo 10 letras). A batida vai
para conferência com "justificativa da pessoa: …" no motivo.

## A cerca que bloqueia e a obra detectada sozinha (04/10/2026)

Decisão do dono: *"não queremos permitir que a pessoa bata ponto fora das áreas
de obra. E quero ainda que a obra seja detectada automaticamente."*

- **A obra é a da cerca** (`core/geo.py::localizar_obra`): o servidor mede a
  distância do celular a todas as obras ativas com coordenada (no tablet, só às
  obras do aparelho) e fica com a cerca em que ele está. A obra que vem da tela
  é só sugestão — escolher outra não muda nada. No celular, **a lista de obras só
  aparece quando a obra não foi detectada** (fora das cercas, sem localização, ou
  obra sem coordenada); achou a cerca, a tela diz "Você está na obra X" e pronto.
- **Fora de todas as cercas: recusada**, com a distância na mensagem ("fora da
  área da obra: 1,4 km da obra PG-A"), linha em `recusas` e, no dia seguinte, o
  alerta "Tentou bater fora da área da obra" — quem estava em serviço fora
  recebe o ajuste da batida pelo encarregado.
- **Na borda** (fora do raio, mas dentro da precisão que o GPS informou, até
  150 m de folga): entra, para conferência. Debaixo de laje o GPS erra.
- **Obra sem coordenada não bloqueia ninguém** (vai para conferência), senão a
  obra inteira ficaria sem bater. As coordenadas são do cadastro da obra no ERP.
- **Celular sem localização: recusado** ("ligue a localização"). Tablet da obra
  sem localização: aceito para conferência — o aparelho já é da obra.
- **Por obra** (Ponto › Configuração › Cerca das obras): raio (20 m a 5 km) e
  "fora da cerca": BLOQUEAR (padrão) ou ANALISAR (o jeito de antes — para obra
  espalhada, estrada, rede).
- **Antes de apertar o botão da 003**, vale o jeito antigo (fora da cerca vai
  para análise): o código não muda de comportamento pela metade.
- **O que a cerca não pega:** celular com GPS falsificado (aplicativo de
  "localização falsa"). Quem pega isso é a foto e o mosaico.

## QR Code, tablet de câmera ligada, mosaico e sinais de fraude (03/10/2026)

Migração **003** (`migracoes/003_qr_mosaico_e_sinais.sql`). Decisão do dono: no
tablet da obra a pessoa se identifica **só por CPF ou QR Code** — sem número de
funcionário e **sem crachá impresso** (crachá se empresta).

**O tablet** (`/ponto/app` num aparelho COMPARTILHADO ou de LISTA): abre direto na
batida, tela cheia, câmera frontal sempre ligada e teclado numérico grande. A
câmera procura QR Code o tempo todo (leitor do navegador, `BarcodeDetector`, ou o
**jsQR** — biblioteca aberta, Apache 2.0, em `static/jsQR.js`); ao mesmo tempo, quem
quiser digita o CPF — 11 números com dígito certo já identificam, sem apertar OK.
Identificada, a tela mostra **nome e função**, conta 2 segundos, tira a foto do
próprio vídeo (sem abrir o aplicativo de câmera), bate, mostra o comprovante e
volta sozinha. "Não sou eu" cancela. A localização é lida de 10 em 10 minutos, não
a cada pessoa. A tela fica acesa (`wakeLock`). Rotas:
`POST /ponto/app/api/tablet/identificar` (devolve o **bilhete**: assinado, 2 min,
só naquele aparelho) e `POST /ponto/app/api/bater` com o bilhete.
**No tablet, batida sem foto é aceita e vai para análise** ("sem foto no aparelho
da obra"). No celular da própria pessoa, não (vira só o alerta "batida sem foto").

**Dois QR, os dois aceitos** (`core/qr.py`):
- **o do WhatsApp** (`BWSP1.…`): imagem que vai para o WhatsApp do cadastro. O banco
  guarda **só o hash**. Troca a cada **7 a 14 dias, sorteado por pessoa**; o antigo
  vale até o novo ser usado pela 1ª vez, ou 3 dias. QR antigo mostrado no tablet é
  recusado, registrado e vira alerta (pode haver cópia com outra pessoa).
  "Cancelar QR (celular perdido)" em Pessoas mata todos os da pessoa.
- **o do "Meu ponto"** (`BWSP2.…`): na tela do celular de quem entrou com CPF+PIN,
  muda a cada 30 s — print de tela não serve.

**"Esqueci meu QR"**: no próprio tablet (digita o CPF, resposta igual exista ou
não) ou no "Meu ponto". Até 3 pedidos por dia por pessoa.

**A fila de WhatsApp com ritmo** (`core/envios.py`, tabela `ponto.envios`) — a API
de WhatsApp da casa não é a oficial, e lote derruba o número:

| Regra | Valor | Por quê |
|---|---|---|
| troca do QR | 7 a 14 dias, sorteado por pessoa | ~38 mensagens/dia com 400 pessoas, espalhadas |
| janela automática | seg. a sáb., 7h30–17h30, hora sorteada | mensagem de madrugada é padrão de robô |
| entre uma e outra | 30 a 90 s, sorteado | nada de rajada |
| teto | 40 por hora, 200 por dia (150 automáticas) | sobra para pedido e aviso de mosaico |
| pedido da pessoa | na hora, 6h–22h, qualquer dia | quem esqueceu não espera |
| primeiro envio a todos | no máximo 120 por dia | 400 pessoas viram 4 dias |

**Nada sai sozinho até alguém ligar** "Enviar e trocar o QR Code automaticamente"
em Ponto › Configuração (desligado de fábrica). O envio roda numa linha separada,
acordada pelas próprias batidas (uma olhada por minuto, no máximo); se o serviço
reiniciar, a fila está no banco. O QR é gerado na hora do envio; se o WhatsApp
falhar (3 tentativas, de 10 em 10 min), nenhum QR fica valendo.

**Mosaico** (`core/mosaico.py`, aba **Mosaico**): uma linha por pessoa — foto de
cadastro, depois as batidas do dia — por obra e dia. Opcional sempre; **obrigatório**
por obra, com um responsável (usuário do ERP com `tratar_ponto`): na manhã seguinte
ele recebe o link pelo WhatsApp; sem conferência até o fim do dia seguinte, abre o
alerta "Mosaico de fotos sem conferência". Conferir pode marcar foto **suspeita** →
a batida volta para análise (decisão em `marcacao_decisoes`, nada se apaga). As
miniaturas ficam na memória do serviço (32 MB), não no banco.

**A batida não espera o Drive**: a foto entra na sala de espera e sobe numa linha
separada logo depois da resposta (antes, 1 a 3 s por pessoa na fila).

**Sinais medidos em cada foto** (sem IA, sem custo): brilho, contraste e uma
impressão de 256 bits (`dhash`). **Alertas novos**: batida sem foto (e, se ligado,
aviso por WhatsApp à pessoa no dia seguinte), foto escura ou sem rosto, a mesma foto
em batidas diferentes (da mesma pessoa em 4 semanas, ou de pessoas diferentes na
mesma obra e dia), 5+ pessoas em sequência com menos de 10 s no mesmo aparelho, QR
antigo usado, 5+ CPFs que não são de ninguém no mesmo tablet no dia, mosaico
obrigatório sem conferência. **São convite a olhar, não prova** — os limiares estão
no topo de `core/alertas.py` e `core/mosaico.py`, para calibrar no piloto.

**Antes de apertar o botão da 003**, o código já publicado funciona: a batida
confere se a coluna nova existe (`db.tem_coluna`) antes de usá-la, e o QR do
WhatsApp responde "ainda não foi ativado — digite o CPF".

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
