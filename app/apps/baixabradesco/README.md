# BaixaBradesco — o robô que dá baixa nos comprovantes bancários

Este arquivo explica **o que a aplicação faz e como ela é**. O `HISTORICO.md` ao
lado explica **por que ela é assim** e o que já deu errado. Leia os dois antes de
mexer.

---

## Em uma frase

Chega o PDF do comprovante de um pagamento; o robô descobre a qual Solicitação
de Pagamento (SP) aquele comprovante pertence, dá a baixa do título no Omie,
marca a SP como paga na planilha, move o card no Pipefy, guarda o comprovante no
Dropbox e, se pedirem, avisa por WhatsApp.

Antes disso tudo era um cenário do Make.com montado peça por peça. A aplicação
substituiu esse cenário.

## Quem chama

| Quem | O que chama | Quando |
|---|---|---|
| Cenário do **Make.com** | `POST /api/baixabradesco/executar` | a cada lote de comprovantes que chega |
| **cron-job.org** | `POST /api/baixabradesco/processar-fila-tardia` | de 5 em 5 minutos, para retomar o que ficou parado |
| Quem estiver investigando | `POST /api/baixabradesco/diagnostico` | quando um comprovante não casou e ninguém sabe por quê |
| Quem estiver investigando | `POST /api/baixabradesco/reprocessar-fila` | para tentar de novo o que falhou depois do casamento |
| Quem estiver investigando | `GET /api/baixabradesco/fila-resumo` | para contar a fila de falhas por situação |
| Quem estiver investigando | `GET /api/baixabradesco/conferir-omie` | para comparar a SPsBD com o Omie e achar baixa pela metade |
| Monitor | `GET /api/baixabradesco/health` | sinal de vida |

Ninguém abre tela aqui: **não existe interface**. A aplicação só responde a
chamadas de máquina.

### Senha de entrada

Todas as rotas conferem a senha `BAIXABRADESCO_SECRET`, que vem no corpo do
pedido (campo `secret`) ou no cabeçalho `X-BaixaBradesco-Secret`. ⚠️ **Se a
variável estiver vazia no Render, a porta fica aberta**: o código libera a
passagem quando não há senha configurada.

### O freio de mão: `modo_teste`

O padrão é **`modo_teste: true`** — o robô lê tudo, decide tudo e devolve o
relatório do que faria, **sem escrever em lugar nenhum**. Para valer, o Make
precisa mandar `modo_teste: false` explicitamente. Além disso dá para ligar e
desligar cada etapa: `executar_omie`, `atualizar_spsbd`, `atualizar_pipefy`,
`enviar_whatsapp`, `salvar_comprovante`, e escolher a pasta do Dropbox em
`pasta_dropbox`.

## De onde vêm os dados

**O comprovante** chega dentro do próprio pedido, de uma de duas formas: o PDF
codificado em texto (`base64`) ou um endereço para baixar (`url`). Baixa por
endereço tem teto de 50 MB — acima disso o pedido é recusado em vez de a
instância morrer de falta de memória.

Um PDF pode ter várias páginas, e **cada página é tratada como um comprovante
separado**.

**As bases de consulta** são três planilhas do Google, lidas uma vez por lote:

| Planilha / aba | Para que serve |
|---|---|
| **SPsBD** (aba `SPsBD`, ~52 mil linhas) | a fonte da verdade das SPs: valor, credor, conta, status, código de barras |
| **SPsAgendar** (mesma planilha) | as SPs que ainda estão na fila de agendamento |
| **BaseBancos** (planilha própria) | de qual conta bancária saiu o dinheiro e qual é o código dessa conta no Omie e no Pipefy |
| **LogBaixaBradesco** (aba na SPsBD) | o registro do que já foi processado, para não pagar duas vezes |
| **BaixaBradescoFila** (aba na SPsBD) | a fila de falhas para tentar de novo |

Da SPsBD o robô carrega só duas fatias, e não a planilha inteira:

- as SPs **a pagar e agendadas** (coluna O = `Pagar`, coluna AB entre
  `agendar`, `agendado` e `falhaagendar`) — é onde ele procura o comprovante;
- as SPs **já marcadas como pagas mas sem data de pagamento e com comprovante
  guardado** — são as que a planilha já resolveu e o Omie ficou para trás.

## Como ele descobre de qual SP é o comprovante

Esta é a parte onde um erro custa dinheiro: casar o comprovante com a SP errada
baixa o título errado. As tentativas acontecem nesta ordem, e a primeira que
resolver ganha:

1. **O número da SP escrito no comprovante** (o campo "Descrição"). É o caminho
   mais confiável. Um cuidado: o QR Code do Pix começa com `000201` e já foi
   confundido com número de SP — números assim são ignorados de propósito.
2. **Depósito da Somapay** (rescisão paga direto da conta Somapay): pelo **nome
   de quem recebeu + valor exato**. Esse comprovante é emitido pela própria
   Somapay e não traz o número da SP nem a conta da empresa.
   ⚠️ **Por que o nome e não o CPF:** numa SP de rescisão o credor é a
   **empresa**, e a coluna CPF/CNPJ traz o CNPJ dela — não o do funcionário.
   Quem identifica a pessoa é o nome escrito na descrição, no formato
   "TRCT <NOME>". E o valor sozinho não serve: em 09/09/2026 havia **quatro**
   rescisões de R$ 452,40, de quatro pessoas diferentes.
3. **Somapay via transferência** (o dinheiro sai do Bradesco para a Somapay):
   por valor, entre as SPs a pagar e agendadas, e só para despesas de rescisão,
   férias, gratificação ou participação nos lucros. Empatando, desempata pela
   conta que foi debitada. ⚠️ O comprovante **não traz o nome do funcionário**,
   então duas rescisões pendentes de mesmo valor na mesma conta ficam paradas
   para conferência — e isso acontece de verdade (quatro de R$ 452,40 em
   09/09/2026).
4. **BeeVale** (vale-alimentação): por valor, aceitando o valor da SP com 1,5%
   de acréscimo (é a taxa da BeeVale) ou o valor exato.
5. **FGTS/Caixa**: por valor, entre as SPs a pagar e agendadas; se não achar,
   tenta por palavra-chave no nome do credor.
6. **Boleto**: pelo código de barras, comparado só pelos números, mais o valor.
7. **Valor + conta + tipo de pagamento**, entre as SPs a agendar.
8. **Valor + conta + status agendado**, na SPsBD. Se sobrar mais de uma
   candidata, o desempate procura o nome do credor **no texto bruto do PDF**.
   ⚠️ Este é o **único** caminho que exige o status exatamente `agendado`: uma
   SP em `falhaagendar` não é encontrada por aqui. Todos os outros aceitam
   `agendar`, `agendado` e `falhaagendar`.
9. **Última tentativa**: as SPs que a planilha já marcou como pagas e que o Omie
   não baixou. Aqui ele executa **só o Omie** e não mexe em mais nada.

**Se sobrar mais de uma candidata e o desempate não resolver, ele não executa
nada** — marca como `pendente_validacao` e alguém precisa olhar. Com uma exceção,
abaixo.

### Comprovantes iguais para SPs iguais: ele distribui

Duas rescisões do mesmo valor, das duas pessoas, ambas agendadas — e dois
comprovantes daquele valor no mesmo PDF. Um a um, cada comprovante vê duas SPs
possíveis e para. Olhando o PDF inteiro, são **dois pagamentos para duas SPs**:
dá para baixar as duas.

Qual comprovante fica com qual SP **não importa**: são do mesmo valor, do mesmo
dia, da mesma conta, e o comprovante de transferência nem traz o nome do
funcionário. O que importa é baixar. Vale para dois, três, quantos forem.

Três travas, e as três são necessárias:

1. **Mesma quantidade dos dois lados.** Dois comprovantes para três SPs não
   distribui — sobraria uma SP paga sem ter sido.
2. **Pagamentos comprovadamente diferentes.** Cada comprovante traz um
   identificador próprio — no Bradesco o `Identificador` ou o `Documento`; no
   Sicredi o `ID da transação`, a `Autenticação Eletrônica` ou o
   `Número de Controle`. Se eles se repetem, é o mesmo comprovante mandado duas
   vezes, e distribuir baixaria duas SPs para um pagamento só. Sem
   identificador, também não distribui.
   ⚠️ Os rótulos de "controle" ficam por último na busca: no Bradesco esse
   número é do **lote inteiro** e se repete entre as páginas; no Sicredi muda a
   cada pagamento. Mesma palavra, semânticas opostas.
3. **Emparelhamento estável.** Página na ordem, SP na ordem — o mesmo lote
   reprocessado dá sempre o mesmo resultado.

⚠️ **Só enxerga o PDF atual.** Dois comprovantes do mesmo valor em arquivos
separados, ainda que no mesmo envio, não se encontram e continuam pendentes.

Quando a distribuição **não** acontece, os comprovantes entram no aviso com a
explicação do porquê — quantidades diferentes, ou identificador repetido —, e não
com o motivo técnico do casador.

### Os dois caminhos da Somapay, que não podem ser confundidos

A folha de pagamento passa pela Somapay de duas formas, e cada uma lança coisa
diferente no Omie:

| Situação | O que o Omie recebe |
|---|---|
| O dinheiro **sai do Bradesco** para a Somapay | a transferência entre as contas **e depois** a baixa do título na conta Somapay |
| O pagamento **já saiu da conta Somapay** (este é o comprovante que a Somapay emite) | **só a baixa** na conta Somapay — não há transferência que tenha acontecido |

**Qual conta Somapay recebeu** vem da **chave PIX impressa no comprovante**,
casada com a coluna Chave PIX da BaseBancos. Cada uma das três contas tem a sua,
então não há o que adivinhar. Se a chave não estiver cadastrada, o robô não
escolhe: deixa pendente e diz o motivo.

Os dois comprovantes podem chegar para a mesma rescisão, e **os dois dão baixa**.
Quem chegar primeiro baixa; o segundo encontra o título já pago no Omie e para
sozinho. O comprovante que fica anexado na SP é o do primeiro que chegou —
decisão do dono em 11/09/2026, ciente de que costuma ser o do Bradesco e não o
da Somapay.

Lançar a transferência no segundo caso criaria no Omie um dinheiro que não
andou. Por isso os dois são tipos separados no código.

**O comprovante emitido pela Somapay não traz a conta da empresa**, só a do
funcionário que recebeu. A conta em que a baixa é lançada vem da **BaseBancos**,
pelo **nome do depositante**: "BWS CONSTRUÇÕES" casa com a conta "Somapay BWS".
A comparação ignora espaços e acentos, então "IFPE SANTA CRUZ" acha
"Somapay IFPESANTACRUZ".

⚠️ **Não dá para usar o CNPJ aqui.** As três contas Somapay da BaseBancos
(BWS, INFRADENDE e IFPESANTACRUZ) têm o **mesmo CNPJ** — o da própria Somapay,
não o da empresa do grupo. Conferido com a planilha real em 11/09/2026.

Se nenhum nome bater, ou mais de um bater, o robô **não escolhe**: deixa o
comprovante pendente de validação e diz o motivo. Errar entre as três contas
jogaria o dinheiro na contabilidade errada, e isso ninguém percebe olhando a
tela.

## O que ele escreve quando casa

Nesta ordem:

1. **Omie**: consulta o título, altera (para garantir que o valor lançado é o
   valor real do comprovante, mesmo que alguém tenha digitado errado) e baixa.
   Juros e multa vão como acréscimo, não como valor do documento. Se o título já
   estiver pago, ele para e registra que já estava.
2. **Registro na LogBaixaBradesco**, para aquele mesmo comprovante não voltar.
3. **SPsBD**: coluna O = `Pago`, V = carimbo de hora, X = data do pagamento,
   AG = link do comprovante no Dropbox, AK = número da conta que pagou.
4. **Pipefy**: preenche os campos do card e move para a fase "Pago/Alimentar
   Omie".
5. **WhatsApp** (Z-API), só se pedirem. Falha de WhatsApp nunca derruba a baixa.

O comprovante **só é guardado no Dropbox depois de casar** com uma SP — assim
não se acumulam arquivos órfãos de comprovante que ninguém sabe de quem é. Se a
SP já tiver comprovante, o link antigo é reaproveitado e nada novo é enviado.

Qualquer falha depois do casamento (Omie, Pipefy ou WhatsApp) vai para a aba
**BaixaBradescoFila**, para ser tentada de novo pela rota de reprocessar.

## Quando o Google diz "chega"

A planilha tem cota por minuto, e a mesma conta de serviço é usada por todos os
módulos do monorepo. Quando o Google recusa por excesso de pedidos, o robô
**não devolve erro para o Make** — grava o pedido em disco (`/tmp`), responde
"recebido, adiado" e o cron reprocessa a cada 5 minutos, até 10 tentativas.

⚠️ O que está em `/tmp` **se perde quando o serviço reinicia** (ou seja, a cada
publicação). Foi aceito assim: o Make pode reenviar.

### O que acontece se o mesmo comprovante for enviado de novo

Antes de tudo: o robô **não consulta o Omie** para saber se a baixa está lá. Se
o comprovante trouxer o **número da SP**, o reenvio funciona sempre — ele vai
direto ao Omie por aquele número. Sem o número, ele depende de achar a SP na
planilha, e aí valem as regras abaixo.

O robô confere **só a lista dele** — a aba `LogBaixaBradesco`, pela impressão
digital da página. Ele **não** consulta o Omie nem lê a linha da SP para saber
se a baixa está lá.

Isso importa porque a baixa acontece em duas etapas separadas: primeiro o Omie,
depois a planilha. A impressão digital é registrada assim que o Omie aceita.

| O que falhou | O reenvio resolve? |
|---|---|
| **O Omie falhou** (planilha não foi tocada) | **Sim** — nada foi registrado, o comprovante passa normalmente |
| **A planilha já dizia Pago e o Omie ficou pendente** | **Sim** — existe um caminho próprio, que executa só o Omie. ⚠️ Até 13/09/2026 ele só enxergava SP **sem data de pagamento**; hoje enxerga também as pagas nos últimos **30 dias** |
| **O Omie baixou e a planilha falhou** | **Sim, desde 13/09/2026** — antes era barrado como repetido, e a SP ficava "Pagar" para sempre |

E a gravação na planilha **deixou de falhar em silêncio**: erro ali vai para a
fila de tentativas, como já acontecia com o Pipefy, e aparece no aviso.

### As planilhas que ele escreve, e como a gravação é garantida

Ele escreve em **uma planilha só** — a *Registro de SPs* (a mesma da SPsBD) — e
dentro dela em **três abas**:

| Aba | O que ele grava |
|---|---|
| `SPsBD` | a baixa: status Pago, carimbo, data, link do comprovante e conta |
| `LogBaixaBradesco` | a impressão digital do comprovante já baixado |
| `BaixaBradescoFila` | a falha, quando alguma etapa não foi |

A **BaseBancos** e a **SPsAgendar** ele só **lê** — nunca escreve.

**A gravação na SPsBD é confirmada antes de a resposta sair** (desde 07/10/2026):
ele grava, **lê de volta** a coluna de status e só então segue. Se não confirmar,
tenta outra vez; se falhar de novo, vai para a fila e entra no aviso.

⚠️ Três coisas que **não** podem voltar, porque eram a causa da não-atualização
silenciosa:

1. **Gravar em thread solta.** A resposta saía antes da gravação terminar, e o
   trabalhador do serviço é reciclado a cada mil pedidos (e reinicia a cada
   publicação) — a thread morria no meio, sem erro em lugar nenhum.
2. **Não conferir.** O comando de gravação não reclama quando a escrita não
   vale.
3. **Deixar a fila parada.** Ela só andava se alguém chamasse a rota à mão.
   Agora cada lote drena cinco pendências junto — e o acumulado se zera pela
   rota de reprocessar com `limite` alto (ver a seção da fila, abaixo).

## A fila de falhas: como contar e como drenar

A aba `BaixaBradescoFila` guarda **tudo** que já passou por ela, concluído
inclusive. Então o número de linhas da aba **não** é o número de pendências — e
essa diferença é a primeira coisa a saber antes de mandar drenar.

| O que você quer | Como |
|---|---|
| saber quantas pendências de verdade existem | `GET /api/baixabradesco/fila-resumo` |
| drenar o acumulado | `POST /api/baixabradesco/reprocessar-fila` com `limite` alto |
| drenar incluindo o que esgotou as cinco tentativas | o mesmo, com `incluir_falhados: true` |
| drenar só uma etapa (o dinheiro primeiro) | o mesmo, com `etapas: ["omie"]` |
| limpar o acumulado de avisos antigos | o mesmo, com `etapas: ["zapi"]` |
| dispensar tudo que é anterior a uma data | `POST /api/baixabradesco/zerar-fila-antiga` com `antes_de` |

**As duas respostas trazem um campo `em_portugues`**, com uma frase dizendo o que
os números querem dizer — quem lê isto costuma estar no celular. A frase vem
junto com os números, nunca em lugar deles.

O **resumo** não grava nada e não reprocessa nada: conta por situação
(`PENDENTE` vencido, `PENDENTE` agendado para depois, `FALHOU`, `CONCLUIDO`),
por etapa, por tipo de falha, diz a data do registro mais antigo e quantos lotes
de cinco seriam necessários no ritmo automático.

**Quem anda com a fila, hoje:**

1. **O cron de 5 em 5 minutos** (`/processar-fila-tardia`, que já existia e já
   era autenticado). Ele drena **por etapa, na ordem da importância** — `omie`
   (dinheiro), `sheets` (planilha), `pipefy` (cartão), `zapi` (recado) —, 15, 15,
   15 e 10 por disparo. Dá ~180 itens por hora sem encostar na cota.
2. **Cada lote de comprovantes**, cinco itens, para pendência nova não
   envelhecer.
3. **Uma chamada à mão** com `limite` alto, para zerar acumulado.

⚠️ **A ordem das etapas não é alfabética, e não pode virar.** Em 08/10/2026 a
fila tinha 1.943 recados de WhatsApp na frente de 238 baixas no Omie. Drenar na
ordem da planilha deixaria o dinheiro para o fim.

**O cron NÃO limpa o acumulado de avisos antigos.** Aviso com mais de três dias
nem é carregado por ele — e isso tem de acontecer na **escolha** das linhas, não
depois. A fila é lida em ordem: com 1.943 avisos de junho na frente, pedir "dez
avisos" devolvia sempre os dez mais velhos, que seriam descartados por idade, e
o aviso de ontem nunca era alcançado. A fila entupia com o que ela mesma ia
jogar fora. Marcar 1.943 linhas de uma vez é decisão
do dono — pede-se explicitamente, com `etapas: ["zapi"]`, e aí a marcação sai em
blocos de 50 linhas por chamada, não uma por linha.

**As credenciais que a drenagem automática usa vêm do ambiente do Render**, não
do pedido — o cron não manda credencial nenhuma. São `OMIE_KEY` / `OMIE_SECRET`
(confirmados no Render em 08/10/2026), `PIPEFY_API_TOKEN` e as três `ZAPI_*`. O
nome da variável é parte do contrato: trocar em silêncio pararia a drenagem
inteira, e há teste travando cada um.

⚠️ **Falta de credencial NÃO consome tentativa.** Era o jeito mais rápido de
apagar a fila sem resolver nada: cinco passadas sem credencial marcariam as 238
baixas como `FALHOU`. Hoje a linha fica intacta e o relatório diz o que falta
configurar (`o_que_falta_configurar`).

⚠️ **A cota do Google é por minuto e é do mesmo usuário de serviço que o ERP, o
painel e o Análise de SPs usam.** Por isso, em lote grande:

- entra **pausa entre itens** (1,2 s acima de 20 itens; `pausa_ms` muda isso);
- a varredura **para sozinha** na terceira recusa seguida por cota, devolve o
  que fez e deixa o resto `PENDENTE` para a próxima passada — insistir aqui
  tiraria os outros sistemas do ar.

**O que a fila lê da planilha:** só as colunas A:L para filtrar, e o payload
(coluna N, um JSON por linha) apenas das linhas escolhidas. Antes ela lia a aba
inteira a cada chamada — com duas mil linhas isso era megabytes por chamada, e
é justamente o que `CONTEXTO.md` §3.7 proíbe.

**Aviso de WhatsApp parado na fila há mais de três dias não é reenviado.** Ele
avisaria de um problema provavelmente já resolvido na mão, e iria para dois
celulares; sai da fila com o motivo escrito na linha. Quem quiser o contrário
manda `reenviar_avisos_antigos: true`. A trava vale **só** para aviso: baixa de
dois meses atrás continua sendo baixa.

**Repetir uma baixa não baixa duas vezes.** O reprocessamento do Omie consulta o
título primeiro e, se já estiver `PAGO`, dá a pendência por resolvida sem lançar
nada.

### A nova tentativa do Omie TERMINA o serviço

⚠️ **Não é opcional, e esquecer isso foi um furo real.** Em 08/10/2026 o dono
relatou *"várias baixas que não aconteceram na planilha"*, e a contagem mostrava
182 pendências de `omie` com **zero** de `sheets`. As duas coisas são o mesmo
fato: a baixa falha no Omie **antes** de a planilha ser gravada — então a
planilha nunca foi escrita, e nunca houve pendência de planilha para enfileirar.
A nova tentativa resolvia o Omie, marcava concluído, e deixava a planilha
desatualizada **para sempre**.

Hoje, quando o título está pago (inclusive quando já estava, pela conciliação
bancária), a nova tentativa segue o resto do plano:

| Etapa | Segura o item na fila? | Por quê |
|---|---|---|
| gravar a planilha | **sim** | é o registro do pagamento, e é o que o dono lê |
| mover o cartão do Pipefy | não — ganha pendência própria | registro certo nos dois sistemas não fica preso por um cartão |

**Repetir é seguro**, e é isso que sustenta o desenho: a consulta ao Omie no
início devolve "já pago" e não lança nada de novo, e a regravação escreve os
mesmos valores nas mesmas células.

### Zerar o que ficou para trás

`POST /api/baixabradesco/zerar-fila-antiga` com `antes_de: "01/10/2026"` dispensa
as pendências registradas antes dessa data. Opcionalmente `etapas` e `limite`.

**Nada é apagado.** A linha fica onde está, marcada concluída, com o motivo e a
data da decisão escritos — quem abrir a planilha depois entende por quê.

`antes_de` é **obrigatório**: um "zerar tudo" sem data é fácil de disparar por
engano, e desfazer linha por linha seria trabalho de horas. A rota é **só POST**
pelo mesmo motivo — um endereço que o navegador ou a prévia de um aplicativo de
mensagem possa buscar sozinho dispararia isso por acidente.

A razão de existir, registrada porque é decisão de negócio: o dono faz
**conciliação bancária diária**, então o que ficou para trás já foi resolvido na
mão — a pendência é de registro, não de dinheiro.

**O cron já faz isso sozinho**, em blocos de 500 por disparo, com corte fixo em
`01/10/2026` (`ZERAR_ANTES_DE` em `fila_tardia.py`, ou a variável de ambiente
`BAIXABRADESCO_ZERAR_ANTES_DE`; vazia desliga). E ele **zera antes de drenar** —
senão a drenagem gastaria a passada inteira nas linhas que vão ser dispensadas
dois segundos depois.

⚠️ **A data é fixa de propósito, não "o mês corrente".** O dono autorizou zerar
o que estava para trás *naquele dia*. Uma regra que andasse com o calendário
dispensaria pendência nova todo dia primeiro — a forma mais silenciosa possível
de perder trabalho. É um mutirão que se encerra sozinho: depois que as linhas
antigas estão marcadas, nenhuma casa com o critério.

### ⚠️ O cabeçalho da fila: `update('A1:O1')`, nunca `append_row`

`append_row` acrescenta no **fim** da aba, não na linha 1. Em 08/10/2026
apareceram três linhas com Status = "STATUS" no meio da fila, contadas como
pendência: bastou a leitura de `A1:O1` voltar vazia uma vez — um soluço de rede —
para nascer lixo. E leitura que falha **não** autoriza escrita nenhuma: escrever
por cima do que não se conseguiu ler é como o lixo nasceu. A seleção também
ignora linha cujo Status seja "STATUS", para o estrago não voltar a contar.

## O conferidor SPsBD × Omie

São **duas** direções, e elas não valem o mesmo. O dono explicou por quê em
08/10/2026: *"fazemos conciliação bancária diária. No sistema Omie vai estar tudo
atualizado. O furo pode ser mais na planilha e na movimentação do card."*

| Direção | O que significa | Probabilidade |
|---|---|---|
| **Omie pago, planilha não** | o dinheiro saiu, o Omie sabe, e a SP continua aparecendo como "a pagar" para quem usa a planilha | **o furo de verdade** |
| **Planilha paga, Omie aberto** | baixa pela metade: o dinheiro saiu e o título não baixou | menos provável, pela conciliação diária |

A primeira direção **não aparece em lugar nenhum** sem este conferidor: a fila de
falhas tinha ZERO pendências de planilha, porque a gravação morria antes de
chegar nela — o sistema não tinha como saber que deixou de gravar. Ela também era
invisível para a primeira versão deste módulo, que partia das linhas marcadas
"Pago"; o furo mora justamente nas linhas marcadas "Pagar".

`GET /api/baixabradesco/conferir-omie` faz a comparação. Parâmetros, todos
opcionais, aceitos pela barra do navegador: `dias` (janela, 60 por padrão),
`limite` (consultas ao Omie por chamada e **por direção**, 50 por padrão),
`pular` (continua de onde a chamada anterior parou), `sentido` (`ambos`,
`omie_pago` ou `planilha_paga`) e `apenas_contar=1`.

⚠️ **`pular` não é conveniência, é correção.** O conferidor não grava nada, então
nada sai do conjunto entre uma chamada e a seguinte: sem `pular`, chamar de novo
reconsultaria as mesmas primeiras linhas, para sempre. A resposta devolve
`proximo_pular` pronto, e a frase em português já traz o número.

**Ele não grava nada.** Nem na planilha, nem no Omie. É relatório. Corrigir é
decisão de quem lê — um conferidor que também corrigisse erraria em silêncio na
primeira divergência de valor, e aí seria pior que não ter conferidor.

O relatório separa quatro coisas que **não** são a mesma:

| No relatório | O que é | O que fazer |
|---|---|---|
| `planilha_atrasada` | Omie pago, planilha não | **o furo apontado pelo dono** — a SP aparece como a pagar sem ser; traz o link do cartão do Pipefy junto |
| `divergentes` | planilha paga, Omie aberto | **é a baixa pela metade** — dinheiro saiu, título não baixou |
| `titulos_nao_encontrados` | o código de integração não existe no Omie | cadastro errado na planilha, outro problema |
| `erros_de_consulta` | a chamada falhou (rede, cota) | tentar de novo |

Misturar os quatro faria o relatório mentir, e só os dois primeiros têm a ver com
dinheiro.

⚠️ **As duas janelas usam datas diferentes, e têm de usar.** A direção
"planilha paga" tem data de pagamento na planilha. A direção "Omie pago" não tem
— a planilha nem sabe que foi paga —, então a janela é pelo **vencimento**. Sem
janela seriam ~52 mil consultas ao Omie. Vencimento muito à frente também fica de
fora: título que vence no ano que vem não é planilha atrasada.

**O conferidor não consulta o Pipefy.** Seria outra volta de API por item; ele
entrega o link do cartão para quem for olhar.

**Ele lê só nove colunas da SPsBD** (A, C, D, G, O, P, R, X, AG). Ler `A:AK`
inteiro custa 150–250 MB e foi assim que o serviço caiu por memória em julho de
2026.

`apenas_contar=1` mede o tamanho do problema **sem** gastar uma consulta ao Omie
por linha — é por onde começar quando a janela é grande.

## O aviso do que NÃO foi baixado

Comprovante que baixa normalmente não gera aviso nenhum — é o esperado. O que
**não** baixa gera: no fim de cada lote, o robô manda **uma** mensagem pelo
Telegram com a lista do que ficou de fora e o motivo de cada um.

Cada linha traz a página, o valor, o nome de quem recebeu (quando o comprovante
tem), e **o número da SP** — o escolhido, quando já se sabe qual é, ou a lista
das candidatas, quando o robô parou justamente por não saber. É por esse número
que se procura na planilha e no Omie.

Entram no aviso:

- comprovante que não achou SP, ou achou mais de uma e parou;
- comprovante que o banco não efetivou;
- baixa que falhou no Omie (foi para a fila de nova tentativa).

**Não** entram, de propósito: o que baixou (é o esperado) e o que foi barrado por
já ter sido baixado (a trava fez o trabalho dela). Aviso demais faz a pessoa
parar de ler, e aí o que importava se perde.

⚠️ **O resultado do aviso diz QUEM recebeu, e por onde** — e isso não era
visível antes. O envio devolvia sucesso quando **qualquer** canal entregava, e o
Telegram do dono entrega quase sempre; então o WhatsApp podia falhar para o
financeiro, que não tem Telegram, e tudo reportava sucesso. É o mesmo defeito da
gravação silenciosa, com outra roupa — e num aviso de falha ele é pior, porque
falha em silêncio justamente quando algo já deu errado.

Hoje a resposta traz `entregues_no_whatsapp`, `so_pelo_telegram` e `sem_entrega`,
com um alerta em português quando alguém ficou só no Telegram. **E credencial
presente com envio falhando ganha segunda tentativa** pelo notificador comum —
antes a segunda tentativa só existia quando a credencial estava *ausente*, então
instância do Z-API fora do ar significava financeiro sem aviso e sem retentativa.

É **um aviso por lote**, não um por comprovante, com no máximo dez itens
listados — acima disso ele diz quantos ficaram de fora.

**Por onde vai:** WhatsApp, pelo mesmo envio que o robô já usa para avisar o
responsável pela SP — aquele funciona em produção e aceita as credenciais Z-API
vindas dentro do próprio pedido do Make, que é como elas chegam hoje. O
Telegram vai junto, de espelho. Se as credenciais não vierem nem no pedido nem
no ambiente, cai no notificador comum, que tem as suas próprias.

**Para quem vai:** **dois números** — o do financeiro, que é quem resolve, e o
do dono, que é quem decide se a regra muda. Os dois recebem a mesma mensagem, e
falha em um não impede o outro. `BAIXABRADESCO_AVISO_TELEFONE` substitui a lista
inteira e aceita vários números separados por vírgula ou ponto e vírgula.

**Só esses dois.** Não confundir com o WhatsApp que o
robô manda ao **responsável pela SP** quando a baixa dá certo — aquele é outra
coisa, existe desde antes, vai para quem pediu o pagamento e não tem relação com
este aviso. Há teste travando os dois destinos separados.

Falha de aviso nunca derruba a baixa: ela já aconteceu.

## Variáveis de ambiente

| Variável | Para quê | Sem ela |
|---|---|---|
| `BAIXABRADESCO_SECRET` | senha das rotas | **as rotas ficam abertas** |
| `BAIXABRADESCO_DEBUG` | devolve o rastro do erro na resposta | erro sai só com a mensagem |
| `GOOGLE_CREDENTIALS_BASE64` | acesso às planilhas | nada funciona |
| `OMIE_KEY` / `OMIE_SECRET` | acesso ao Omie. **São estes os nomes no Render.** `OMIE_BWS_APP_KEY`/`OMIE_BWS_APP_SECRET` são apelidos antigos, também aceitos. Pelo Make as chaves vêm no próprio pedido; pela tela do Análise de SPs, vêm daqui | a baixa no Omie para antes de começar, dizendo qual variável falta |
| `PIPEFY_API_TOKEN` | acesso ao Pipefy | o card não é atualizado |
| `DROPBOX_APP_KEY` / `DROPBOX_APP_SECRET` / `DROPBOX_REFRESH_TOKEN` | guardar o comprovante | o comprovante não é salvo |
| `ZAPI_INSTANCE_ID` / `ZAPI_API_TOKEN` / `ZAPI_CLIENT_TOKEN` | WhatsApp | o aviso é pulado |
| `NOTIFICAR_WHATSAPP` | desliga o WhatsApp de vez | ligado |
| `BAIXABRADESCO_AVISO_TELEFONE` | para quem vai o aviso do que não baixou | usa `CHATBOT_MASTER_PHONE` |
| `NOTIFICAR_TELEGRAM` | desliga o aviso de vez | ligado |

## Serviços que ele toca

Google Sheets (leitura e escrita), **Omie** (títulos a pagar e lançamento em
conta corrente), **Pipefy** (cards), **Dropbox** (arquivo do comprovante),
**Z-API** (WhatsApp).

## Os arquivos

| Arquivo | Papel |
|---|---|
| `routes.py` | as quatro rotas e a senha |
| `core.py` | o maestro: lê o PDF, casa, executa, monta o relatório |
| `parser_pdf.py` | tira o texto de cada página do PDF |
| `parser_bradesco.py` | entende o comprovante do Bradesco |
| `parser_sicredi.py` | entende o comprovante do Sicredi (ver a ressalva abaixo) |
| `matcher.py` | decide de qual SP é o comprovante |
| `omie.py` | as chamadas ao Omie, inclusive a transferência da folha Somapay |
| `pipefy.py` | busca e atualização dos cards |
| `sheets.py` | leitura e escrita nas planilhas, e o mapa das colunas |
| `storage.py` | Dropbox |
| `zapi.py` | WhatsApp |
| `fila.py` | fila de falhas depois do casamento |
| `fila_tardia.py` | fila de pedidos adiados por cota do Google |
| `diagnostico.py` | analisa sem executar |
| `models.py` | os formatos de dado que circulam entre os arquivos |
| `utils.py` | conversões de dinheiro, data, conta e texto |

## Cuidados que não são opcionais

- **Memória.** Esta aplicação divide 2 GB com os outros 15 blueprints do monorepo e já derrubou o
  serviço inteiro em julho de 2026. Nunca voltar a ler a planilha inteira
  (`get_all_values()`), nunca baixar arquivo sem teto, nunca duplicar a leitura
  da SPsBD. Detalhe no `CONTEXTO.md` §9.
- **Trabalhar sempre em cima do arquivo que está publicado.** Já se perderam
  correções por aplicar remendo sobre versão velha.
- **Comparar status da planilha sempre normalizado** (`Agendado` e `agendado`
  são a mesma coisa; tratar como diferentes já custou dias).
- **Conferir o mapa de colunas com o dono**, pelo cabeçalho de verdade — nunca
  contando células de uma linha copiada.

## Ressalvas do código de hoje (conferidas em 04/09/2026, na `main`)

**O leitor do Sicredi está ligado** (desde 13/09/2026). O robô escolhe o leitor
pelo formato do papel: comprovante com cooperativa + conta de origem vai para o
leitor do Sicredi, o resto vai para o do Bradesco. ⚠️ A palavra "sicredi"
sozinha **não** manda para lá: ela aparece em comprovante do Bradesco quando o
destino é uma conta Sicredi, e ler com o leitor errado pode sair com valor
errado.

Duas coisas foram corrigidas em 04/09/2026 e estão descritas no `HISTORICO.md`:

1. **Comprovante recusado pelo banco.** Antes, só a frase exata "Operação Não
   Realizada" barrava. Um comprovante real que dizia "Transação Não Realizada"
   passava como boleto comum. Hoje a recusa é uma lista de frases
   (`FRASES_RECUSA` no `parser_bradesco.py`), conferida **antes** de extrair
   qualquer campo — um comprovante recusado não entrega nem valor nem código de
   barras ao casador — e o que foi barrado aparece no resumo da resposta, em
   `recusados_nao_efetivados`.
2. **A trava contra pagar duas vezes.** A lista de comprovantes já baixados é
   lida **uma vez por lote** e conferida página a página; o que foi barrado
   aparece em `duplicados_ja_baixados`.
   **Com uma exceção importante (13/09/2026):** se a SP daquele comprovante
   ainda estiver como "Pagar" na planilha, a baixa anterior ficou pela metade —
   o Omie baixou, a planilha não — e o reenvio **passa**, para concluir. O Omie
   responde "título já pago", o robô pula essa parte e termina o que faltava.
   Esses casos aparecem em `baixas_concluidas`.
   ⚠️ Nunca trocar essa leitura única por uma consulta por página: um lote de
   dez comprovantes viraria dez leituras da mesma coluna, que é o padrão que
   derrubou a instância em julho de 2026.

Um limite que fica, e é bom saber: a impressão digital do comprovante é feita
com o conteúdo do arquivo **mais o nome dele**. O mesmo PDF reenviado com outro
nome conta como comprovante novo. Quem segura, nesse caso, é o Omie respondendo
"título já pago".
