# Emissão de NFS-e — Eusébio/CE

Blueprint do monorepo, em **`/emissao`**. Emite a nota fiscal de serviço da BWS a
partir do card da medição no Pipefy, e faz sozinho todo o resto: a planilha de
numeração, o título no Omie, o card, os PDFs no Drive e o aviso no WhatsApp.

Quem usa não abre esta tela pelo menu: o card do Pipefy tem um link que já vem
com o número do card e o token dentro dele.

> **Pegando este trabalho agora?** Leia antes o `HISTORICO.md` ao lado. Ele
> guarda as decisões já tomadas, os incidentes que custaram caro e o que está
> pendente. Aqui está **como a coisa é**; lá está **por que é assim**.

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

el_nfse_abrasf.py     monta e assina o XML no padrão ABRASF 2.04
el_nfse_envio.py      envia ao webservice da prefeitura (SOAP GerarNfse)

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
- **`el_nfse_nacional.py`** — **nada aqui dentro o importa.** Ele está vivo
  porque o **ERP** o usa na emissão automática (`CONTEXTO.md` §9, 10/09/2026).
  Mexer nele é mexer no ERP.

---

## O caminho de uma emissão

**Antes de emitir** (`GET /emissao?card_id=...&token=...`), o `worker.preparar`
faz tudo o que não tem efeito: lê o card, acha a obra na C. Diários, calcula as
retenções, resolve o endereço do tomador pelo CNPJ, pega o **próximo número**, e
monta e **assina** o XML. A tela mostra o espelho da nota e a discriminação,
que é editável — o que estiver ali é o corpo que vai ser emitido.

Nada foi enviado ainda. Sair da página não deixa rastro.

**O botão "Confirmar e Emitir"** (`POST /emissao/emitir`) revalida tudo no
servidor — não confia no que veio do navegador —, assina de novo com a
discriminação final e envia o `GerarNfse` à prefeitura. Da resposta saem o
**número**, o **código de verificação** e a **data**.

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
| 10 | Controle Nacional | registra a nota como **aguardando nacional** |

**A trava contra emissão dupla** é o passo 1: se o número já está na coluna F da
"Notas BWS", o `concluir` para e avisa. Só repete com `FORCAR` explícito.

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
sai sempre como **Retido na Fonte**, e a **Exigibilidade do ISS** é sempre
"Exigível". Se precisar de "Não incidência" ou outra, é mudança de código — está
anotado como pendência.

**Imposto sem retenção não aparece** na discriminação nem na tabela de apuração.
Nota com valor zero ao lado do nome do imposto confunde quem lê.

---

## A numeração vem da planilha, não da prefeitura

O próximo número é o **maior número da coluna F da "Notas BWS" mais um**. A
prefeitura devolve o número que ela gravou; se os dois divergirem, a tela avisa
— mas a nota já foi emitida. Numeração é a parte do sistema que mais depende da
planilha estar íntegra.

---

## A parte nacional (o padrão federal)

A NFS-e municipal sai na hora. A **nota nacional** demora alguns minutos e é
fechada depois, sozinha:

1. logo após emitir, uma thread tenta em **60s, 180s e 300s** buscar a nota na
   **SEFIN** pelo ID da DPS (que é derivado do número da nota). Isso usa só o
   certificado — não depende do portal da prefeitura, cujo login expira em
   cerca de uma hora;
2. quando acha, gera a **DANFSe nacional**, regera a municipal **já com a
   chave**, sobe as duas com o mesmo nome (mesmo link, sem duplicar) e completa
   os links na planilha e na Descrição do card;
3. o **ADN por NSU** ficou como rede de segurança, para o caso de a SEFIN não
   responder.

Uma nota só sai da fila quando o XML nacional **casa** com ela em CNPJ do
tomador, competência e valor. Sem isso, continua pendente — e é melhor assim.

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
| `/emissao/diag` | diz **por que** o certificado não carregou, e qual conta do Google está sendo usada — sem mostrar segredo |
| `/emissao/diag_nacional_chave` | só leitura: testa quais endpoints federais respondem por chave |

---

## A substituição de uma nota

**Só funciona pela tela para nota emitida por este sistema.** O XML carrega um
bloco que aponta o RPS da nota antiga, e as notas do sistema têm RPS "de
verdade" (série 1, tipo 1). Nota emitida **manualmente no portal** tem RPS com
série vazia e tipo 0, que a própria prefeitura guarda mas o XSD de envio recusa
— então não há como substituí-la por aqui. O caminho dela é o botão
**"Substituir" do portal** e, depois, o `/emissao/recuperar` para fazer só os
efeitos internos.

A tela barra antes de tentar quando o valor novo é **menor** que o da nota
antiga (o município não aceita), e a própria página explica os dois erros que
denunciam a nota manual (`E76` e erro de schema).

Quando a substituição dá certo, a nota antiga é marcada **Cancelada** no slot do
card, ganha a observação na "Notas BWS" e, no Omie, o número antigo sai e o novo
entra **numa única chamada** — o Omie trava o registro por alguns segundos
depois de cada escrita.

---

## Onde as coisas moram

| Onde | O quê |
|---|---|
| Planilha **Notas BWS** (`1NOEzey3…PpEbU`) | aba `Notas BWS` (numeração e apuração), `Notas BWS Links` (links dos arquivos), `Controle Nacional` (a fila do nacional + o último NSU na célula P1) |
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
| `EMISSAO_NF_TOKEN` | o token do link. **Sem ela configurada, a tela fica aberta a qualquer um** — falha ABERTO, ao contrário do resto do repositório |
| `EMISSAO_NF_CERTIFICADO_P12_BASE64` | o certificado A1 da empresa, em base64 |
| `EMISSAO_NF_CERTIFICADO_SENHA` | a senha do certificado |
| `EMISSAO_NF_BASE_URL` | o endereço do serviço, usado para montar o link da busca nacional que vai na planilha |
| `EMISSAO_NF_DRIVE_IMPERSONAR` | de quem os arquivos ficam no Drive (padrão `contato@bwsconstrucoes.com.br`); vazio desliga |
| `GOOGLE_CREDENTIALS_BASE64` | a conta de serviço do Google — já existe, é a mesma do resto |
| `EL_NFSE_TOKEN` | token da prefeitura, usado só pelo script de consulta crua |
| `TELEGRAM_SECRET_TOKEN` | espelha o aviso do WhatsApp no Telegram |

Da aba `Credenciais` vêm ainda `PIPEFY_TOKEN`, `OMIE_KEY`/`OMIE_SECRET` e os três
tokens da Z-API.

---

## O que esta área NÃO tem

Dito em voz alta porque muda o jeito de trabalhar aqui:

- **Quase nenhum teste automatizado.** A suíte da raiz praticamente não encosta
  nesta pasta: o único teste dela é o `tests/test_emissaonf_codigo_obra.py`
  (os dois códigos da obra), criado em 21/09/2026. O
  `test_emissao_automatica_banco.py` é do **ERP**, e só dubla o cliente nacional
  daqui. Conferência, aqui, continua sendo olhar a tela e o espelho antes de
  clicar — o motor fiscal, a montagem do XML e o pós-emissão não têm rede.
- **Nenhum login.** A porta é o token na URL. E, se o token não estiver
  configurado, não há porta nenhuma.
- **Nenhum banco e nenhuma migração.** Nada a apertar ao publicar.
- **Nenhuma dependência nova em relação ao resto do serviço** — `gspread`,
  `requests`, `lxml`, `signxml`, `cryptography`, `fpdf2`, `num2words` e o
  cliente do Google já estão no `requirements.txt` da raiz. O
  `requirements.txt` desta pasta é herança de quando ela rodava sozinha.
