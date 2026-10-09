# Emissão de NFS-e — Eusébio/CE

Blueprint do monorepo, em **`/emissao`**. Emite a nota fiscal de serviço da BWS a
partir do card da medição no Pipefy, e faz sozinho todo o resto: a planilha de
numeração, o título no Omie, o card, os PDFs no Drive e o aviso no WhatsApp.

Quem usa não abre esta tela pelo menu: o card do Pipefy tem um link que já vem
com o número do card e o token dentro dele.

> **Pegando este trabalho agora?** Leia antes o `HISTORICO.md` ao lado. Ele
> guarda as decisões já tomadas, os incidentes que custaram caro e o que está
> pendente. Aqui está **como a coisa é**; lá está **por que é assim**.

> ⚠️ **O formato da nota mudou em 07/10/2026.** A prefeitura desativou o modelo
> antigo (ABRASF) por causa da obrigatoriedade do IBS/CBS e passou a aceitar só
> a **DPS**, do padrão nacional. A emissão foi migrada no mesmo dia. O que está
> escrito aqui já é o modelo novo; onde o antigo ainda aparece, é porque as
> notas emitidas até aquela data estão nele e continuam tendo de ser lidas.

---

## Os dois riscos que não existem nas outras áreas

Isto vem antes de qualquer coisa, e é o motivo de esta área ter chat próprio.

**1. Nota fiscal emitida não se apaga.** Cancelar ou substituir tem prazo e
regra da prefeitura, e o município só aceita substituir por valor **igual ou
maior**. É o risco mais alto do repositório — maior que escrever no Omie, onde
no pior caso se apaga um título. Cada clique em "Confirmar e Emitir" cria um
documento fiscal de verdade, com efeito tributário.

**2. Ela usa o certificado digital A1 da empresa.** O arquivo do certificado e
a senha **nunca entram no chat** — diagnóstico se faz pela tela `/emissao/diag`,
que diz o que está errado sem mostrar o segredo. Já houve credencial vazada
justamente aqui (`CONTEXTO.md` §9).

---

## Como está montado

Os arquivos desta pasta se importam de forma **plana** (`import worker`, não
`from .worker import`), porque começaram como scripts de linha de comando e
continuam rodando assim. O `web.py` injeta a pasta no `sys.path` para os imports
funcionarem dentro do Flask. Não "arrume" isso sem ler o `HISTORICO.md`.

```
web.py                as telas e as rotas (é o único arquivo que o Flask enxerga)
worker.py             o pipeline: lê o card, calcula, monta e assina o XML
  pipefy.py           busca o card e extrai os campos
  cdiarios.py         lê a obra na planilha "C. Diários" (tributação, ISS, CNO)
  tributacao.py       a conta das retenções — o motor fiscal
  tomador.py          endereço do tomador pelo CNPJ (BrasilAPI, com cache)
  municipios_ibge.py  nome do município -> código IBGE (cache local)
  preview.py          a discriminação (o corpo da nota) e o espelho visual
  montar_emissao.py   junta tudo no formato do XML
  validacao.py        o que BARRA a emissão (teto de valor, campos, período)

montar_dps.py         traduz a nota calculada para o formato nacional (DPS)
el_nfse_nacional.py   monta, assina e envia a declaração; consulta a nota
emitir_dps.py         o envio: declara, espera a nota ficar pronta, lê o retorno
xsd_nacional/         os schemas OFICIAIS, para conferir a nota antes de emitir
nfse_exemplo.py       monta a resposta que a prefeitura daria (só para teste)

do modelo ANTIGO, desativado pela prefeitura em 07/10/2026 — só leitura:
  el_nfse_abrasf.py   monta e assina o XML no padrão ABRASF 2.04
  el_nfse_envio.py    enviava ao webservice (hoje devolve sempre o erro E999)

concluir.py           o pós-emissão imediato (os 10 passos, abaixo)
  notas_bws.py        a linha na planilha "Notas BWS" e na "Notas BWS Links"
  omie.py             retenções e nº do documento no título a receber
  pipefy_update.py    preenche o slot A–E no card e limpa os campos de entrada
  drive_upload.py     sobe XML, recibo e PDFs para a pasta do Drive
  recibo.py           o recibo/ofício em PDF
  nota_municipal.py   o PDF da NFS-e no layout da prefeitura
  zapi.py             o aviso no WhatsApp (e espelho no Telegram)

job_nacional.py       fecha a parte NACIONAL depois (é assíncrono)
  adn_nfse.py         cliente do ADN e da SEFIN (certificado, sem senha de portal)
  controle_nacional.py a fila das notas "aguardando nacional", numa aba
  danfse.py           a DANFSe nacional em PDF

substituicao.py       os efeitos internos de uma nota que substitui outra
completar_imediato.py refaz só a ENTREGA (Drive/links) sem tocar no bookkeeping
credenciais.py        Google + leitura dos segredos da aba "Credenciais"
gspread_retry.py      espera e tenta de novo quando o Sheets devolve 429
```

### Código que está na pasta e NÃO roda em produção

Fica registrado para ninguém perder tempo procurando bug em coisa morta:

- **`app_emissao.py`** — a tela antiga em **Streamlit**, de antes de virar
  blueprint. Ninguém importa. É a origem do `web.py`.
- **`emitir_real.py`, `consultar_status.py`, `fechar_nacional_manual.py`,
  `organizar.py`** — scripts de linha de comando, do tempo em que tudo rodava no
  PC. Continuam funcionando, e é de propósito: as credenciais vêm da planilha,
  então dá para rodar de fora sem o Render.
- **`efeitos.py`** — nasceu como "simulador" (mostra o que faria sem fazer).
  Sobrou dele o que o `concluir.py` de fato usa: os dados do recibo e a lista de
  destinatários do WhatsApp.
- **`dropbox_client.py`** — o arquivo ia para o Dropbox antes de ir para o
  Drive. Nada no caminho ativo o chama.
- **`el_nfse_abrasf.py` e `el_nfse_envio.py`** — o modelo antigo. **Não emitem
  mais nada**: desde 07/10/2026 a prefeitura responde a qualquer envio deles com
  *"o modelo Abrasf foi desativado"*. Continuam aqui porque o
  `el_nfse_abrasf.DadosRps` é a estrutura onde o resto do sistema guarda os
  dados da nota já resolvidos (tomador, discriminação, códigos de serviço) — o
  `montar_dps.py` lê dela. Ou seja: o formato morreu, a estrutura de dados não.

⚠️ **`el_nfse_nacional.py` é compartilhado com o ERP.** Ele agora é o coração da
emissão daqui, e ao mesmo tempo o ERP o importa na emissão automática
(`CONTEXTO.md` §9). Mexer nele atinge as duas áreas — e há uma divergência
entre elas registrada no `HISTORICO.md` (07/10/2026) que o ERP precisa
resolver do lado dele.

---

## O caminho de uma emissão

**Antes de emitir** (`GET /emissao?card_id=...&token=...`), o `worker.preparar`
faz tudo o que não tem efeito: lê o card, acha a obra na C. Diários, calcula as
retenções, resolve o endereço do tomador pelo CNPJ, pega o **próximo número**, e
monta e **assina** o XML. A tela mostra o espelho da nota e a discriminação,
que é editável — o que estiver ali é o corpo que vai ser emitido.

Nada foi enviado ainda. Sair da página não deixa rastro.

> ⚠️ **TENTE O ENSAIO ANTES DE EMITIR.** Até 08/10/2026 ele nunca foi concluído:
> na primeira tentativa a tela demorava 150s (defeito nosso, consertado), na
> segunda faltava o token da prefeitura (também consertado) — e a memória da área
> passou a afirmar, sem evidência, que ele estava bloqueado. Isso fez quatro
> recusas do padrão nacional serem descobertas em **emissão de verdade**, uma por
> tentativa. Ensaio não cria documento fiscal: tentar não custa nada.
>
> **O ensaio depende de a prefeitura ter habilitado o ambiente de teste**, e é o
> erro **E0037** que diria que não está. Se ele aparecer, a tela explica: apesar do texto oficial falar
> de "município inexistente", o manual diz que na prática significa que o
> município **não configurou a Produção Restrita** na Plataforma Nacional. Não há
> o que corrigir aqui — é um pedido à prefeitura. Enquanto isso, a conferência de
> uma emissão real é a tela "Conferir declaração".

**Antes de emitir de verdade, dá para ensaiar.** A caixa "Ensaiar primeiro"
manda a **mesma** nota para o ambiente de homologação da prefeitura: ela volta
inteira, com número e chave, para ser conferida — e **não vale como documento
fiscal**, nem grava nada na planilha, no Omie, no card ou no Drive. É o único
jeito de ver o resultado antes, porque não existe "quase emitir" em produção.

**O botão "Confirmar e Emitir"** (`POST /emissao/emitir`) revalida tudo no
servidor — não confia no que veio do navegador —, monta a **declaração (DPS)**,
assina com o certificado e manda à prefeitura.

Aqui há uma diferença importante em relação ao modelo antigo: **a nota não volta
na mesma resposta.** A prefeitura confirma que recebeu a declaração e devolve um
protocolo; a nota fica pronta segundos depois, e o sistema fica perguntando por
ela até aparecer. Da nota pronta saem o **número**, a **chave de acesso** e a
**data**.

O código de verificação **não existe mais**: no modelo nacional quem identifica
a nota é a chave de acesso de 50 dígitos, que vai no PDF com o QR ao lado.

### A identificação da obra é obrigatória, e não está no schema

A plataforma nacional **exige o grupo de obra** quando o serviço é de construção
civil — treze subitens da lista, entre eles o **07.02.02**, que é o único que a
BWS usa. Sem ele a nota não sai, e o modo de falhar é o pior possível: **o
município aceita a declaração e o nacional recusa depois**, do outro lado da
fila, numa tela de pendências que a nossa consulta não alcança. Foi o que
aconteceu com a nota 3281 em 07/10/2026 (erro **E0370**).

**Isto não é pegável pelo schema.** No XSD o grupo é opcional (`minOccurs="0"`):
a declaração sem ele é um arquivo válido. A obrigatoriedade é regra de negócio
da plataforma, condicionada ao código do serviço. Toda a conferência contra o
schema oficial — que pega campo fora de ordem, valor fora do domínio e casa
decimal sobrando — passava batido por esta.

**O que vai:** o **CNO** da obra, lido da coluna "CNO" da C. Diários, **sem
pontuação** (na planilha `90.025.25410/76`, na declaração `900252541076`) — como
todo documento neste layout. O layout aceita três identificações e exige
exatamente uma: o CNO/CEI, o CIB, ou o endereço da obra. As três estão
implementadas; a BWS usa o CNO, porque é o dado que ela tem.

**Obra sem CNO barra a emissão antes de enviar**, dizendo onde resolver. A conta
é assimétrica: barrar custa um aviso na tela, deixar passar custa um número de
nota queimado e uma declaração presa na fila.

### O CST do IBS/CBS sai da classificação, e não de uma escolha

**O CST são os três primeiros dígitos do `cClassTrib`.** Para a BWS a
classificação é `200046` ("Operações com bens imóveis", item 07.02 do Anexo
VIII), então o CST é `200`. Mandar o par sem casar é o erro **E0959**, que custou
uma emissão em 08/10/2026 — ia `000` com `200046`.

Por isso o CST é **derivado** no código, e a montagem da declaração **recusa** um
par que não casa. A regra, os valores que a BWS manda e a procedência de cada um
estão em `xsd_nacional/IBSCBS_CLASSIFICACAO.md` — inclusive os dois campos
opcionais (`tpOper`, `tpEnteGov`) que ficaram implementados e **desligados**, por
serem os próximos suspeitos de uma recusa.

Como o grupo de obra, **isto também não é pegável pelo schema**: os dois campos
são válidos sozinhos, e quem confere a combinação é a plataforma.

### Número de nota já enviado não volta a ser usado

A identificação da declaração é construída **a partir do número da nota**. Então
reusar o número reusa a identificação — e identificação já enviada à prefeitura
não serve mais: ela responde **EL99** ("chave informada para a DPS não existe no
repositório municipal"). Aconteceu em 08/10/2026, com o número 3281.

O manual da prefeitura diz o contrário — que a declaração recusada pode ser
reenviada com a mesma identificação. **Em Eusébio essa frase não se sustentou**, e
a prática ganhou do manual. Então:

- a numeração conta **todas** as declarações já enviadas, qualquer que seja o
  desfecho (a aba `Declaracoes` só recebe declaração **depois** do aceite, então
  todo número que está nela já foi enviado);
- não há exceção para o mesmo card;
- buraco na sequência é normal — nota cancelada faz o mesmo.

### Imposto que não foi retido NÃO vai, nem como zero

A plataforma trata **zero** e **ausente** como coisas diferentes: campo opcional
com valor zero é recusado. Mandar `0,00` declara uma retenção **de** valor zero,
que é diferente de não haver retenção. É o erro **E0699** ("o valor do tributo CP
deve ser maior que zero…"), e CP é o INSS.

Então: dos três campos federais (INSS, IR, CSLL) só vão os que foram de fato
retidos; se nenhum foi, o grupo inteiro não sai. O total aproximado de tributos
vai como **"não informado"** (`indTotTrib=0`, a opção que o layout dá) em vez de
três zeros. E retenção maior que o valor do serviço **derruba a montagem**, com
os dois números na mensagem — é erro de dado.

**O que vigia isso de verdade não é a memória de quem mexe:** um teste varre a
declaração inteira contra o XSD e acusa qualquer elemento que o layout permita
omitir e que esteja indo com zero. A exceção são os indicadores (`indFinal`,
`indDest`, `indTotTrib`, `finNFSe`, `regEspTrib`), onde zero é um **significado**
e não um valor. O mesmo defeito já tinha aparecido em três campos diferentes
antes de virar varredura.

### O número da nota agora tem o ano na frente — e a numeração sabe disso

No padrão nacional a nota volta com **13 dígitos**: ano (26) + o nosso sequencial
em 11. As duas primeiras, de 08/10/2026, são a **`2600000003283`** e a
**`2600000003284`** — sequenciais 3283 e 3284, confirmando que o número longo é
montado a partir do nosso. Tanto `nNFSe` como `nDFSe` vêm assim; **não existe um
número municipal curto separado** no XML.

É esse número oficial que fica na planilha, no Omie e no documento do cliente —
guardar o `3283` ali seria mais cômodo, mas faria o sistema divergir do que a
prefeitura e o cliente veem, e conciliar é para que a coluna serve.

**O cuidado que isso exige, e ele não é opcional:** o próximo número sai do maior
da planilha mais um. Lido cru, `2600000003283 + 1` pediria a nota
**2.600.000.003.284**, e a sequência nunca mais voltaria. Então o número gravado é
traduzido de volta ao sequencial antes de qualquer conta
(`worker.sequencial_da_nota`): 13 dígitos é nacional e tem o ano na frente,
qualquer outro é do modelo antigo. A planilha tem os dois formatos convivendo.

### A declaração é gravada antes de qualquer espera

Entre a prefeitura **aceitar** a declaração e a nota **ficar pronta** passa um
tempo que não depende de nós — o portal dela chama esse estado de *"Aguardando
Transmissão"*, com o número já reservado e a chave nacional ainda vazia.

Nesse intervalo, a declaração é gravada na aba **`Declaracoes`**, e isso acontece
**antes** de o sistema esperar um segundo. É o que garante que nada se perde se a
tela fechar, o serviço for publicado ou a conexão cair.

A tela **"Conferir declaração"** lista o que está em aberto, com um botão em cada
— então ninguém precisa guardar a identificação de 45 caracteres.

**E a espera é curta de propósito, por um motivo que não é de conforto:** o
serviço atende 4 pedidos ao mesmo tempo e é compartilhado com o ERP e o painel.
Cada emissão esperando prende uma dessas quatro linhas. Com a espera longa, umas
poucas tentativas seguidas derrubaram o monorepo inteiro com "Bad Gateway".

**Se a espera estourar, o sistema NÃO oferece "tentar de novo".** É de propósito:
a declaração já está com a prefeitura, e a nota pode ter saído. A tela mostra a
identificação da declaração e manda para a tela **"Conferir declaração"**, que
pergunta à prefeitura se a nota saiu e, se saiu, **termina o serviço** (planilha,
Omie, card, Drive, avisos) sem emitir nada.

Isso já aconteceu de verdade, na primeira emissão real (nota 3281, 07/10/2026): o
processamento da declaração é uma **fila do lado da prefeitura**, e o manual diz
que o aceite dela significa "recebi", não "autorizei". Demorar mais que a nossa
espera é normal, não é defeito.

### Os TRÊS desfechos de uma declaração enviada

Confundir dois deles custou uma ida e volta inteira. São três, e cada um tem uma
ação diferente:

| Desfecho | Existe nota? | O que fazer |
|---|---|---|
| **virou nota** | sim | terminar o serviço (a tela "Conferir declaração" faz) |
| **ainda processando** | ainda não | esperar e consultar de novo |
| **recusada** pela plataforma | **não** | corrigir e **reenviar com o MESMO número** |

O terceiro é o mais fácil, e o que mais assusta quando mal explicado: quando a
plataforma devolve a lista de erros, **nada foi criado**. O manual diz que a mesma
declaração pode ser reenviada com a correção, **mantendo a mesma identificação** —
então reemitir com o mesmo número não é risco de nota duplicada: é o caminho
previsto.

**O número que vai na declaração é o NOSSO pedido**, tirado da planilha. O número
de verdade da nota só existe quando a prefeitura autoriza. Por isso "não existe a
nota N" e "a declaração da nota N foi enviada" podem ser as duas verdadeiras ao
mesmo tempo.

**Há exatamente uma situação em que o envio é repetido:** quando a prefeitura
responde que o **endereço não existe** (404 ou 405). Aí ela não recebeu
declaração nenhuma, nada foi criado, e o sistema tenta o outro jeito de escrever
o mesmo endereço — porque o manual e o portal da prefeitura discordam sobre ele.
Qualquer outra resposta, inclusive erro de rede, **não** é repetida: pode ter
chegado.

**A partir daqui a nota existe e não se desfaz.** Por isso o pós-emissão
(`concluir.py`) é todo em blocos separados, cada um com seu `try`: se o Drive
falhar, a planilha já gravou; se o WhatsApp falhar, o Omie já foi ajustado.
Nenhum passo derruba o seguinte, e o log de cada um aparece na tela do
resultado. São dez:

| | Passo | O que faz |
|---|---|---|
| 1 | Notas BWS | grava a linha A–P (as colunas Q em diante são fórmulas da planilha) |
| 2 | Omie | na **1ª** nota da medição, grava as retenções da medição **integral**; da 2ª em diante só acrescenta o número ao documento (`3001/3072`) |
| 3 | Pipefy | preenche o primeiro slot A–E livre e **limpa os 12 campos de entrada** |
| 4 | Drive | arquiva o XML da nota |
| 5 | Recibo | gera o PDF e sobe |
| 6 | NFS-e municipal | gera o PDF no layout da prefeitura e sobe |
| 7 | Notas BWS Links | grava a linha com os links |
| 8 | Pipefy | escreve os links no topo da Descrição do card |
| 9 | WhatsApp | avisa quem está na lista de destinatários |
| 6b | DANFSe nacional | gera o documento do padrão federal e sobe |
| 10 | Controle Nacional | registra a nota como **concluída**, com a chave |

**A trava contra emissão dupla** é o passo 1: se o número já está na coluna F da
"Notas BWS", o `concluir` para e avisa. Só repete com `FORCAR` explícito.

**A nota nacional não é mais um segundo ato.** No modelo antigo ela saía minutos
depois, por um job que ficava perguntando à SEFIN se a nota havia subido. Agora a
emissão JÁ acontece pelo nacional: a chave e o XML vêm na própria resposta, e a
nota nasce completa. O job e as telas de busca nacional continuam de pé **só para
as notas antigas**.

---

## A obra tem DOIS códigos

Na C. Diários a mesma obra é identificada por dois códigos: o **primário**, na
coluna "Código Primário", e o **secundário**, na primeira coluna (A). O card do
Pipefy pode trazer qualquer um dos dois.

A busca tenta o primário e, só se não achar, o secundário — e o primário nunca é
encoberto: se o secundário de uma linha repetir o primário de outra, quem vale é
o primário, senão a nota sairia com a tributação da obra errada. Quando a obra é
achada pelo secundário, o log diz isso.

## A conta das retenções

Vem da coluna **Tributação** da obra, na C. Diários, em quatro blocos:

```
ONERADA - <Ded.INSS> - <Ded.ISS> - <Impostos Retidos>
   ex.:  ONERADA - 50/50 - 80/20 - IR
```

- **`NN/MM`** = quanto é serviço / quanto é material. `80/20` quer dizer que o
  imposto incide sobre 80% do valor.
- **`SD`** = **NÃO retém** aquele imposto (base zero). Para reter sobre o valor
  inteiro, escreve-se **`100/0`** — não `SD`. Isso já foi entendido ao contrário
  uma vez; está no `HISTORICO.md`.
- **Impostos retidos** = qualquer combinação de `IR, PIS, COFINS, CSLL`, ou
  `SEM RETENÇÃO`.

**Categoria fora desse padrão BARRA a emissão**, com a crítica na tela. É de
propósito: chutar o significado de uma categoria estranha é errar imposto.

O card pode **sobrepor** o padrão da obra, campo a campo, na fase Medições:
"Tipo de Medição = Reajuste (Sem Dedução)" força 100% serviço, e o dropdown
"Informar Alíquota e ou Dedução" libera alíquotas e a divisão do ISS digitadas
na mão. Sem esses campos preenchidos, vale a C. Diários.

Duas coisas são **fixas no código** e hoje não dá para variar pela tela: o ISS
sai sempre como **Retido na Fonte**, e a tributação do ISS vai sempre como
"Operação tributável". Se precisar de imunidade, exportação ou não incidência, é
mudança de código — está anotado como pendência.

**No formato nacional, "retido" é o número 2, e "não retido" é o 1.** É
contraintuitivo, e já esteve invertido no código (ver `HISTORICO.md`,
07/10/2026). Existem constantes com nome para isso — `RET_ISS_TOMADOR` e
`RET_ISS_NAO_RETIDO` — justamente para ninguém mais precisar lembrar qual número
é qual.

**O teto da alíquota de ISS é 9,99%.** Não é escolha nossa: o formato nacional
reserva um dígito só para a parte inteira. Obra com alíquota de 10% ou mais tem a
emissão barrada, com o motivo escrito na tela, em vez de levar erro da
prefeitura.

**Imposto sem retenção não aparece** na discriminação nem na tabela de apuração.
Nota com valor zero ao lado do nome do imposto confunde quem lê.

**PIS, COFINS e CSLL são declarados juntos, por um código só.** O formato
nacional não tem um campo por imposto: tem um código que diz, de uma vez, quais
dos três foram retidos. Por isso o grupo deles vai na declaração sempre que
**algum** dos três for retido — inclusive quando só a CSLL for. Alíquota e valor
só entram para o que foi de fato retido: mandar "0,00" num imposto não retido
declara uma retenção de valor zero, o que é diferente de não declarar nada.

---

## A numeração vem da planilha, MAIS as declarações em aberto

O próximo número é o maior entre o **maior número da coluna F da "Notas BWS"** e
os **números presos a declarações em aberto**, mais um.

A segunda parte não é refinamento: a planilha só recebe **nota pronta**, então
uma declaração que a prefeitura aceitou e ainda não virou nota não entra lá — e o
número dela ficava livre do nosso lado enquanto a prefeitura o mantinha
**reservado**. A nota seguinte sairia pedindo o mesmo número, e a prefeitura leria
isso como **reenvio da declaração anterior**, não como nota nova: dois serviços
num documento só, sem erro na tela. Aconteceu de verdade em 07/10/2026 (ver o
`HISTORICO.md`).

**Para o mesmo card o número é reaproveitado de propósito** — ali é o reenvio que
o manual da prefeitura prevê, com a mesma identificação. A
prefeitura devolve o número que ela gravou; se os dois divergirem, a tela avisa
— mas a nota já foi emitida. Numeração é a parte do sistema que mais depende da
planilha estar íntegra.

---

## A parte nacional, e o que sobrou do jeito antigo

**Para nota emitida de 07/10/2026 em diante não há "parte nacional" separada.**
A emissão acontece pelo nacional: a chave e o XML vêm na resposta, e a DANFSe sai
junto dos outros documentos. Nada fica pendente.

**Para as notas anteriores**, a maquinaria antiga continua inteira, porque ainda
há notas a reencontrar e PDFs a regerar. Ela funcionava assim: logo após emitir,
o sistema tentava em 60s, 180s e 300s achar a nota na **SEFIN** pela
identificação da declaração (derivada do número da nota) — usando só o
certificado, sem depender do portal da prefeitura, cujo login expira em cerca de
uma hora. Quando achava, gerava a DANFSe, regerava a municipal já com a chave e
completava os links. O **ADN por NSU** era a rede de segurança.

A disparada automática dessa busca depois de emitir **foi desligada** (não há
mais o que buscar). As telas `/emissao/nacional`, `/emissao/nacional_chave` e
`/emissao/nacional_xml` continuam de pé, para as notas antigas.

**A identificação da declaração foi mantida igual de propósito:** ano em dois
dígitos + número da nota em treze, série 1. É por ela que a SEFIN reencontra uma
nota. Mudar o formato faria o sistema perder de vista todas as notas antigas — há
teste provando que a emissão e o job antigo montam a mesma identificação.

---

## As telas

Todas pedem o mesmo `token` na URL. Não há login: quem tem o link, entra.

| Rota | Para quê |
|---|---|
| `/emissao?card_id=…` | a tela principal: espelho, discriminação editável, emitir |
| `/emissao/recuperar` | nota **já emitida** cujo pós-emissão falhou ou nem rodou. Cola-se o XML; com "completo" marcado refaz tudo (tem trava anti-duplicação), sem marcar refaz só a entrega no Drive |
| `/emissao/regerar` | regrava os PDFs de notas já concluídas com o layout atual, no mesmo link |
| `/emissao/nacional` | roda o fechamento nacional na mão |
| `/emissao/nacional_chave` | fecha uma nota colando a **chave** de 50 dígitos |
| `/emissao/nacional_xml` | fecha uma nota colando o **XML nacional** baixado do portal |
| `/emissao/manual` | **"Nota emitida no portal".** Para nota emitida à mão no portal da prefeitura (canal fora do ar, ou caso que só dá por lá). Recebe o **XML** — dele saem os dados, exatos — e, opcionalmente, o **PDF oficial**, que entra como o documento em vez da nossa réplica. Faz todo o resto: planilha, Omie, card, Drive e avisos. **Não emite nada** |
| `/emissao/planilha` | **"Só a linha da planilha".** Para a nota que saiu certa em TUDO — Omie, card, arquivos, cliente — e cuja linha da "Notas BWS" não entrou. Grava a linha e **não toca em mais nada**. Os valores saem do **XML**, não do card: a conclusão limpa doze campos de entrada do card, então recalcular a nota depois daria números diferentes dos emitidos. Usar `/emissao/recuperar` ou `/emissao/manual` neste caso preencheria um **segundo slot** no card, mexeria no Omie de novo e mandaria o WhatsApp outra vez |
| `/emissao/faturamento` | **"Base de Faturamento".** Consolida numa aba só (`Base Faturamento`) o que hoje está em cinco planilhas — e é ela que vai alimentar a tela de Faturamento do Análise de SPs. Roda em **lotes** e diz quantas faltam; repetir não duplica. Não apaga nada e não emite nada. O desenho está em `FATURAMENTO.md` |
| `/emissao/omie` | **"Tributos no Omie".** As três operações que sobraram do Apps Script: **consultar** o título, **equalizar** os tributos e **atualizar** o Omie. Confere por padrão (leitura); alterar o Omie exige **confirmação marcada**. A nota manda e o título obedece; nota cancelada fica fora |
| `/emissao/declaracao?…&diagnostico=1` | **"Diagnóstico completo desta declaração".** Pergunta sobre ela na prefeitura E direto na plataforma nacional, e mostra as respostas cruas. A pergunta que decide é a terceira: se o nacional **não conhece** a declaração e a prefeitura diz que transmitiu, as versões não fecham — e a transmissão é ela que faz. O texto é feito para ser copiado e mandado a ela; nunca mostra token nem certificado |
| `/emissao/declaracao?…&encerrar=1` | **"Encerrar".** Aparece só nas declarações **paradas** da lista. Tira a declaração da lista, para o caso em que a API nunca conta a recusa (a 3281 passou um dia respondendo "em processamento" enquanto o portal já a dava como recusada). **Não libera o número** — número enviado fica gasto (ver EL99 acima). Não emite, não cancela e não apaga nada |
| `/emissao/declaracao` | **"Conferir declaração".** A saída do único aperto desta área: a prefeitura aceitou a declaração e a nota não ficou pronta na hora. Pergunta a ela se a nota saiu e, se saiu, **termina o serviço** — sem emitir nada. Consultar não cria nada, então pode repetir |
| `/emissao/diag` | diz **por que** o certificado não carregou, qual token chegou e de onde, e qual conta do Google está sendo usada — sem mostrar segredo |
| `/emissao/diag_nacional_chave` | só leitura: testa quais endpoints federais respondem por chave |

---

## A substituição de uma nota

**Não passa mais pela tela — nenhuma, desde 07/10/2026.**

No modelo antigo a nota nova carregava, dentro dela, a identificação da nota que
substituía. O modelo nacional não tem esse campo: lá a substituição é um
**evento** registrado sobre a nota já emitida, por outra operação da API.

Esse caminho ainda não está pronto aqui, e a razão de não ter sido feito às
pressas está no `HISTORICO.md`: um evento de substituição não dá para ser
ensaiado sem antes emitir uma nota de verdade para substituir. Fazer isso no
escuro, num documento que não se apaga, é pior do que não fazer.

**Então hoje substituir é:** o botão **"Substituir" do portal** da prefeitura e,
depois, o `/emissao/recuperar` com o número da nota antiga no campo "nota
substituída" — ele refaz os efeitos internos (Pipefy, Omie, Drive, planilha,
WhatsApp). É o mesmo caminho que já se usava para nota emitida manualmente, e a
tela de emissão explica isso em vez de deixar tentar e falhar.

### O RPS morreu com o modelo antigo — e com ele um impedimento antigo

Vale saber, porque incomodou por meses: no modelo antigo, **nota emitida à mão no
portal não podia ser substituída pela aplicação**. A substituição exigia apontar
o **RPS** da nota antiga, e nota manual tinha RPS com série vazia e tipo 0 — que a
prefeitura guardava mas o XSD de envio recusava.

**No modelo nacional não existe RPS.** Quem identifica a nota é a **chave de
acesso**, e nota manual tem chave como qualquer outra. Ou seja: quando a
substituição por evento for implementada, **nota manual vai ser substituível** —
o impedimento não foi consertado, ele deixou de existir junto com o formato.

Quando a substituição dá certo, a nota antiga é marcada **Cancelada** no slot do
card, ganha a observação na "Notas BWS" e, no Omie, o número antigo sai e o novo
entra **numa única chamada** — o Omie trava o registro por alguns segundos
depois de cada escrita.

---

## Onde as coisas moram

| Onde | O quê |
|---|---|
| Planilha **Notas BWS** (`1NOEzey3…PpEbU`) | aba `Notas BWS` (numeração e apuração), `Notas BWS Links` (links dos arquivos), `Declaracoes` (as declarações enviadas e ainda sem nota), `Controle Nacional` (a fila do nacional antigo + o último NSU na célula P1) |
| Planilha **C. Diários** (`1C7MWQmr…PsBk`) | aba `Centro de Custo`: obra, município, alíquota de ISS, tributação, CNO |
| Planilha **Credenciais** (`1D4aVC7w…B9i-U`) | aba `Credenciais` (chave/valor) e `Destinatarios WhatsApp` |
| **Google Drive** (`1-NxQ1Q35…QtZyh`) | XML, recibo, NFS-e municipal e DANFSe nacional de cada nota |
| **Pipefy** | o card da medição: origem dos dados e destino dos slots A–E |
| **Omie** | o título a receber: retenções e número do documento fiscal |

Não há banco de dados nesta área. Tudo o que persiste está em planilha, no
Drive, no Pipefy ou no Omie.

---

## Credenciais e variáveis de ambiente

O padrão é o mesmo do `analisesps`: **a variável de ambiente ganha da planilha.**
A aba `Credenciais` existe para os scripts rodarem fora do Render; em produção,
o que vale é o Render.

| Variável | Para quê |
|---|---|
| `EL_NFSE_TOKEN` | **o token de integração da prefeitura.** É ele que autentica o canal da emissão. Sem ele **nenhuma nota sai** — nem em ensaio. Gerado no portal do município, em Configurações › APIs de Integração. **Não é o `EMISSAO_NF_TOKEN`** — ver o aviso abaixo da tabela |
| `EMISSAO_NF_ESPERA_S` | segundos de espera pela nota numa emissão de verdade (padrão **25**). **Não aumente sem pensar:** o serviço atende 4 pedidos por vez e é compartilhado com o ERP e o painel — espera longa prende uma das quatro linhas e já derrubou o monorepo inteiro |
| `EMISSAO_NF_ESPERA_ENSAIO_S` | o mesmo, para o ensaio (padrão 15) |
| `EMISSAO_NF_AMBIENTE` | `HOMOLOGACAO` trava o serviço inteiro em teste: nenhuma nota tem validade fiscal, mesmo sem marcar o ensaio, e a tela avisa em letras grandes. Qualquer outro valor (ou vazio) = produção |
| `EMISSAO_NF_TOKEN` | o token do link. **Sem ela configurada, a tela fica aberta a qualquer um** — falha ABERTO, ao contrário do resto do repositório |
| `EMISSAO_NF_CERTIFICADO_P12_BASE64` | o certificado A1 da empresa, em base64 |
| `EMISSAO_NF_CERTIFICADO_SENHA` | a senha do certificado |
| `EMISSAO_NF_BASE_URL` | o endereço do serviço, usado para montar o link da busca nacional que vai na planilha |
| `EMISSAO_NF_DRIVE_IMPERSONAR` | de quem os arquivos ficam no Drive (padrão `contato@bwsconstrucoes.com.br`); vazio desliga |
| `GOOGLE_CREDENTIALS_BASE64` | a conta de serviço do Google — já existe, é a mesma do resto |
| `TELEGRAM_SECRET_TOKEN` | espelha o aviso do WhatsApp no Telegram |

Da aba `Credenciais` vêm ainda `PIPEFY_TOKEN`, `OMIE_KEY`/`OMIE_SECRET` e os três
tokens da Z-API.

### ⚠️ São DOIS tokens, e eles não se substituem

Isto custou tempo em 07/10/2026, então fica em destaque:

| | `EMISSAO_NF_TOKEN` | `EL_NFSE_TOKEN` |
|---|---|---|
| de quem é | **nosso** | **da prefeitura** |
| para que serve | proteger o endereço da tela | autenticar o canal da emissão |
| onde se consegue | foi escolhido por nós | portal do município › Configurações › APIs de Integração |
| sem ele | a tela fica aberta a quem tiver o link | **nenhuma nota sai** |

**Por que o token da prefeitura nunca foi necessário antes:** o modelo antigo
(ABRASF) autenticava pelo **certificado digital**, no próprio aperto de mão da
conexão — não havia token nenhum no caminho. O modelo nacional exige
**certificado E token**. Então um serviço que emitiu notas por meses sem esse
token não está mal configurado: ele é exigência nova.

A tela de **Diagnóstico** mostra os dois lado a lado, diz se cada um chegou e de
onde veio, e lista os **nomes** das credenciais da planilha (nunca os valores) —
para achar o token quando ele está lá com outro rótulo. Chega-se a ela pelo link
no pé da tela de emissão, que já leva o token dentro.

---

## A base consolidada de faturamento

A gestão das notas emitidas está saindo da aba "Notas BWS" para uma **base
consolidada** (`Base Faturamento`), que alimenta uma tela de Faturamento no
Análise de SPs. **Todo o desenho está em `FATURAMENTO.md`**: o que o emissor usa
de cada planilha hoje, as 71 colunas da base e de onde cada uma vem, o inventário
do Apps Script da planilha, e o que depende de decisão do dono.

Enquanto a transição não acabar, **o emissor grava nos dois lugares**.

## O que esta área NÃO tem

Dito em voz alta porque muda o jeito de trabalhar aqui:

- **Teste, agora existe — e é o que substitui "emitir para ver".** São três
  arquivos, 46 casos:
  - `tests/test_emissaonf_dps.py` — a declaração que vai para a prefeitura,
    conferida contra o **schema oficial** (`xsd_nacional/`), nas quatro formas de
    tributação que a BWS usa. Vigia os três campos que, errados, fazem a nota
    sair errada sem ninguém notar: o tipo de retenção do ISS, a dedução de
    material e o código que diz quais federais foram retidos.
  - `tests/test_emissaonf_resposta_nacional.py` — o que acontece DEPOIS de
    emitir: o PDF da nota, a DANFSe, o valor do recibo, a tela de recuperação.
    Tudo a partir de uma resposta de prefeitura montada aqui e validada contra o
    schema oficial da NFS-e.
  - `tests/test_emissaonf_codigo_obra.py` — os dois códigos da obra.

  O que eles **não** cobrem: a conversa com a prefeitura de verdade. Nenhum teste
  faz rede. Conferência final continua sendo o **ensaio em homologação** e o olho
  na tela antes de clicar.
- **Nenhum login.** A porta é o token na URL. E, se o token não estiver
  configurado, não há porta nenhuma.
- **Nenhum banco e nenhuma migração.** Nada a apertar ao publicar.
- **Nenhuma dependência nova** nem na migração para o modelo nacional: `lxml`,
  `signxml` e `cryptography` já estavam no serviço, e o cliente da API nacional
  já existia nesta pasta desde setembro. `gspread`,
  `requests`, `lxml`, `signxml`, `cryptography`, `fpdf2`, `num2words` e o
  cliente do Google já estão no `requirements.txt` da raiz. O
  `requirements.txt` desta pasta é herança de quando ela rodava sozinha.
