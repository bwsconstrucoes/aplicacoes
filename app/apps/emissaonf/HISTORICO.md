# Emissão de NFS-e — decisões, incidentes e o que falta

Este arquivo existe para uma sessão nova (ou uma pessoa nova) pegar o trabalho
sem repetir o que já foi discutido e sem repetir o que já deu errado.

O `README.md` ao lado explica **como a emissão é**. Este aqui explica **por que
ela é assim**, e o que aconteceu no caminho. Leia os dois antes de mexer.

---

## Como esta memória nasceu — 21/09/2026

Esta área entrou na tabela do `CLAUDE.md` **sem memória nenhuma**: era a única
das cinco sem `README.md` e sem `HISTORICO.md`, com ~7.000 linhas em 48
arquivos, e a dívida estava anotada no `CONTEXTO.md` há meses (*"ainda não
documentado aqui"*, *"documentar na próxima vez que mexer"*).

Os dois arquivos foram escritos a partir de **duas fontes**, e vale saber qual é
qual antes de confiar em cada linha:

- **O código**, lido de ponta a ponta nesta sessão. Tudo o que está no
  `README.md` foi conferido no código — se divergir, o código é que manda, e
  o arquivo é que está velho.
- **O relato do dono**, colado por ele de um chat antigo que se perdeu. É a
  única fonte do que aconteceu com notas de verdade: os incidentes, os valores,
  o que já foi corrigido no mundo e o que não foi. Onde o relato não trouxe
  data, este arquivo **não inventou uma** — diz "sem data registrada".

Onde os dois se contradisseram, está escrito qual venceu e por quê.

---

## Onde o trabalho está

### ✅ Estado em 08/10/2026 — leia isto primeiro

**A EMISSÃO VOLTOU A FUNCIONAR.** A nota `2600000003283` (obra IFSPSAOJOSE,
medição 11) saiu em 08/10/2026 no padrão nacional, com o pós-emissão inteiro —
planilha, Omie, card, Drive, recibo e WhatsApp. É a primeira nota do modelo novo,
e o XML oficial dela foi lido campo por campo: a seção "A PRIMEIRA NOTA DO PADRÃO
NACIONAL SAIU" tem a conferência e os dois defeitos que ela revelou (o número que
quase destruiu a numeração, e o `vLiq` que não é o líquido).

**Foram QUATRO recusas antes dela, todas em 08/10**, uma por tentativa, porque
não havia onde ensaiar. Em ordem: E0370 (faltava o grupo de obra), EL99 (número já
enviado não se reusa), E0959 (CST não casava com a classificação), E0699 (imposto
não retido ia como zero). O detalhe de cada uma está nas seções abaixo.

---

#### Como a emissão chegou até aqui

A prefeitura **desligou o formato de nota** que o sistema usava em 07/10/2026, e
a emissão ficou parada. A migração para o formato novo (DPS, padrão nacional) foi
feita e publicada no mesmo dia.

**A primeira tentativa real (nota 3281) não virou nota, e já se sabe por quê.**
A prefeitura aceitou a declaração e a plataforma nacional a recusou com o erro
**E0370: faltava o grupo de informações da obra**, obrigatório para serviço de
construção civil. A recusa ficou numa tela de pendências que a nossa consulta não
alcança — por um dia a consulta respondeu "em processamento" para uma declaração
já morta. A TI da prefeitura mostrou o erro ao dono em 08/10/2026.

**Consertado no mesmo dia:** a declaração passou a levar o **CNO da obra** (da
C. Diários, coluna "CNO"), e obra sem CNO barra a emissão antes de enviar. A
seção "O VEREDITO DA 3281" explica por que isso não apareceu em nada que havia
sido conferido — a declaração sem o grupo é **válida no schema oficial**, e a
obrigatoriedade é regra de negócio da plataforma, não do arquivo.

**E as emissões seguintes revelaram mais três, no mesmo dia** — uma por
tentativa, e todas com seção própria abaixo:

| Erro | O que era | Consertado |
|---|---|---|
| **EL99** | número já enviado não se reusa; a frase do manual sobre reenviar com a mesma identificação não vale em Eusébio | a numeração nunca reusa número enviado; o "liberar o número" publicado de manhã virou "encerrar", que não libera |
| **E0959** | o CST do IBS/CBS são os três primeiros dígitos da classificação; ia `000` com `200046` | o CST é derivado da classificação, e a montagem recusa um par que não casa |
| **E0699** | imposto não retido ia com `0,00`; a plataforma recusa zero e o campo é opcional | só vai o que foi retido; e uma **varredura** passou a acusar qualquer campo opcional indo com zero |

**A causa comum das três, e ela é a lição do dia:** a plataforma valida
combinações e regras de negócio que o **schema oficial aceita**. Conferir contra
o schema — que é a única conferência que dá para fazer aqui dentro — não é
suficiente, e cada descoberta custou **uma emissão real**.

### ⚠️ CORREÇÃO a este próprio arquivo: o ensaio NUNCA foi testado

**Escrito em 08/10/2026, depois de o dono perguntar "não é isso que você
precisa?" e colar o endereço de homologação.** Era, e o sistema já o usa —
palavra por palavra, incluindo o `/nfse40`. O endereço nunca foi o problema.

**O que este arquivo passou a afirmar, e não devia:** que *"o ensaio está
indisponível"*, que *"em Eusébio ela devolve o E0037"*, que *"a Produção Restrita
segue desabilitada"*. **Nada disso foi observado.** O que existe de verdade é
uma seção do manual da prefeitura dizendo que o erro E0037, *quando acontece*,
significa Produção Restrita não habilitada. Eu transformei essa possibilidade em
fato e repeti como fato em quatro seções.

**O que de fato aconteceu com o ensaio, em ordem:** na primeira tentativa a tela
girou 150s e o dono desistiu (*"não consegui concluir o ensaio, tá demorando
muito"*) — defeito nosso, consertado; na segunda, faltava o token da prefeitura
(*"deu Token de integração da prefeitura ausente"*) — também consertado. Depois
disso **o ensaio não foi tentado mais nenhuma vez**, porque eu já havia escrito
aqui que ele não funcionava.

**O custo desse erro:** as quatro recusas do dia (E0370, EL99, E0959, E0699)
foram descobertas em **emissão de verdade**, uma por tentativa, horas de espera
do dono cada. Se o ensaio funcionar — e não há evidência de que não funcione —
todas as quatro teriam custado cliques.

**Então a sequência certa é tentar o ensaio ANTES de emitir**, e só concluir que
ele não serve se ele devolver E0037 de fato. Ensaio não cria documento fiscal:
tentar não custa nada.

**A sequência, com o que se aprendeu da 3281:**

1. **ensaiar primeiro** (caixa "Ensaiar primeiro" na tela de emissão). Se
   devolver **E0037**, aí sim o ambiente de teste não está habilitado e o caminho
   é pedir isso à prefeitura — e emitir de verdade;
2. se a nota não ficar pronta na hora, **"Conferir declaração"** — e, se a
   consulta insistir em "em processamento" por horas, **olhar o portal da
   prefeitura**, na tela de pendências de transmissão da DPS Nacional. Foi lá, e
   só lá, que a recusa da 3281 apareceu. A nossa consulta não alcança essa tela;
3. conferir o número devolvido, os PDFs, a planilha, o Omie e o card.

**O que NÃO se sabe, e só a primeira emissão responde:** se a plataforma aceita o
formato do CNO que mandamos, e qual número a prefeitura devolve.

### O histórico até aqui

O sistema **emite em produção há meses** e faz o ciclo inteiro sozinho: lê a
medição no Pipefy, calcula as retenções pela tributação da obra, assina com o
certificado A1, envia à prefeitura de Eusébio, e depois grava a planilha, ajusta
o título no Omie, preenche o card, arquiva os PDFs no Drive e avisa no WhatsApp.
A nota nacional fecha sozinha em minutos, pela SEFIN.

Não é um projeto em construção: é uma ferramenta em uso. O que existe de
trabalho pendente é **conserto e faxina**, não funcionalidade nova.

### O que está pendente AGORA

**O que está na frente de tudo (08/10/2026):**

1. ✅ **FEITO — a nota `2600000003283` saiu** (obra IFSPSAOJOSE, medição 11), com
   o pós-emissão inteiro. O que falta é **conferir as outras tributações**: essa
   nota tem PIS, COFINS, IR e CSLL retidos, ISS retido e **sem INSS**. As outras
   combinações da BWS seguem provadas só contra o schema — e foi justamente uma
   combinação diferente (sem INSS) que revelou o E0699.
2. **Se vier recusa nova**, os suspeitos já estão mapeados, nesta ordem:
   **(a)** `tpOper` — "Tipo de Operação com Entes Governamentais ou outros
   serviços sobre bens imóveis"; o candidato é `1` (fornecimento com pagamento
   posterior). **(b)** `tpEnteGov` — União/Estado/DF/Município; **isto o sistema
   não pode deduzir**, porque não dá para saber do CNPJ de que esfera é o órgão:
   é pergunta para o dono. **(c)** o formato do CNO: mandamos só os dígitos, que
   é a convenção do layout, mas o manual não traz exemplo de `<cObra>`. Os dois
   primeiros já estão implementados e desligados (ver
   `xsd_nacional/IBSCBS_CLASSIFICACAO.md`); ligar é uma linha.
3. **TENTAR O ENSAIO** (caixa "Ensaiar primeiro"). Nunca foi concluído, e eu
   havia escrito aqui que ele não funcionava sem nunca ter visto isso acontecer
   — ver a correção na seção de estado, acima. Se ele funcionar, toda correção
   futura passa a custar um clique em vez de uma emissão de verdade. Só se ele
   devolver **E0037** é que o caminho passa a ser pedir a habilitação da Produção
   Restrita à prefeitura.
4. **Levar ao chat do ERP a inversão do ISS retido** (detalhe na seção de
   07/10/2026). Lá a emissão automática manda o número errado, e não foi mexido
   porque é outra área.
5. **Implementar a substituição pelo evento nacional**, ou decidir que o caminho
   pelo portal basta. Hoje substituir pela tela está bloqueado, com explicação.
6. **Decidir se a tributação do ISS vira campo** (imunidade, exportação, não
   incidência), hoje fixa em "operação tributável".

**Também confirmado, de 21/09:** conferir na tela que o card 1447316614 (obra
AREFORTAL09) carrega, agora que a busca olha os dois códigos da obra.

**Todo o resto abaixo é pista, não tarefa.** Veio de um relato que o dono colou
de um chat antigo, e **nada disso foi conferido contra o mundo real** — nenhuma
planilha foi aberta, nenhuma nota foi consultada. Mais do que isso: perguntado
em 21/09/2026, **ele não recorda os itens 1 e 2** (ver a seção logo abaixo).

1. **O passivo de notas com ISS a maior na prefeitura.** É o item mais caro, e
   está explicado no incidente do `ValorDeducoes`, mais abaixo. Falta
   **levantar quais notas** foram emitidas com dedução de ISS durante o período
   do defeito e **substituí-las**. Notas `100/0` (sem dedução) não foram
   afetadas.
2. **Concluir a substituição da nota 3083** — pelo portal, seguida do
   `/emissao/recuperar`.
3. **Rodar o `/emissao/regerar`** nas notas que precisam do layout novo dos
   PDFs.
4. **Terminar o registro dos ~290 PDFs antigos** baixados do portal: ler o
   número de dentro do PDF, subir ao Drive com o nome no padrão e preencher a
   "Notas BWS Links". Junto disso, **deduplicar as ~20 mil linhas** dessa aba.
   Esses PDFs são anteriores à aplicação e **não têm DPS**, então a busca
   automática pela SEFIN nunca vai achá-los — é trabalho de script, uma vez.
5. **Decidir se a Exigibilidade do ISS vira campo** ("Não incidência" etc.),
   hoje fixa em "Exigível" no código. E, se virar, decidir se é **por obra** ou
   **por nota**.
6. **Uma das buscas de PDF deve varrer subpastas do Drive** — o dono pediu, mas
   ficou sem dizer qual das três. Pergunta em aberto.
7. **Vigiar o embaralhamento da "Notas BWS".** Se voltar mesmo com o
   `LockService`, o caminho é tirar a ordenação de cima do `onChange` e pôr num
   gatilho por tempo.

### O que o dono respondeu — 21/09/2026

Esta lista foi levada a ele no mesmo dia em que foi escrita. A resposta, na
íntegra:

> *"no mais tudo funciona normal. nao recordo de problema de iss, nem de
> substituicao."*

**Como ler isso, sem esticar nem encolher:**

- **O defeito do `ValorDeducoes` existiu** — não é lembrança, está provado no
  código, no comentário que explica por que o campo passou a ser enviado. E
  está corrigido desde 11/09/2026.
- **O passivo de notas afetadas NÃO está confirmado.** Ele não recorda o
  problema, e ninguém levantou a lista. Pode ser que o número de notas com
  dedução de ISS emitidas antes de 11/09 seja pequeno, ou zero; pode ser que
  não seja. **Ninguém olhou a planilha ainda.**
- **As substituições (3069→3081, 3070→3082, 3083) também não estão
  confirmadas.** Vieram do relato colado de um chat antigo, e ele não recorda.

**O que isso quer dizer na prática:** o que está na lista de pendências abaixo
veio do relato antigo, não de uma conferência. Antes de gastar tempo com
qualquer um daqueles itens, **abrir a planilha e ver se o problema existe** —
e não presumir que existe porque está escrito aqui. Um levantamento de meia
hora na "Notas BWS" decide se o item 1 é trabalho de dias ou não é trabalho
nenhum.

**E a notícia boa, que também é informação:** o sistema está rodando normal. O
único defeito que ele trouxe nesta sessão foi o card que não abria, e esse foi
corrigido.

### As perguntas que continuam em aberto

Estas ele não respondeu, e continuam valendo:

- **Qual das três buscas de PDF** deve varrer subpastas do Drive?
- **Existe um agendamento externo** chamando `/emissao/nacional` de tempos em
  tempos? O código fala num Cron "de 10 em 10 minutos" como rede de segurança,
  mas **não há nada no repositório que o configure** — se existe, é no
  cron-job.org, fora daqui. Se não existe, a rede de segurança não está armada,
  e uma nota que a busca automática não pegar nos primeiros cinco minutos fica
  esperando alguém clicar.

## Decisões tomadas, e por quê

### `SD` quer dizer NÃO RETER — e `100/0` é que retém sobre tudo

A categoria da obra tem quatro blocos
(`ONERADA - Ded.INSS - Ded.ISS - Impostos`). O bloco de dedução aceita `NN/MM`
(serviço/material) ou `SD`.

**`SD` significa base zero: aquele imposto não é retido.** Vale para o INSS e
para o ISS. Para reter sobre o valor inteiro, escreve-se **`100/0`**.

Isto corrigiu um entendimento anterior em que `SD` valia 100% — ou seja, o
sistema retinha onde não devia. Está escrito no próprio código
(`tributacao.py`), em comentário, para não se perder de novo.

### Imposto sem retenção não aparece na nota

Junto da correção acima: imposto com valor zero saiu da discriminação e da
tabela de apuração. Uma linha "ISS: 0,00" no corpo da nota faz quem lê procurar
erro onde não há.

### Categoria fora do padrão BARRA a emissão

O motor fiscal recusa qualquer coisa que não sejam os quatro blocos exatos, com
a crítica na tela. Chutar o significado de uma categoria estranha é errar
imposto numa nota que não se apaga — é melhor não emitir.

### O ISS é sempre retido, e a exigibilidade é sempre "Exigível"

As duas coisas estão fixas no código. Não foi decisão contra alternativa: foi o
único caso que apareceu até hoje. Tornar a exigibilidade configurável é uma das
pendências, e a pergunta que vem junto é **por obra ou por nota** — a resposta
muda onde o campo mora.

### Substituição: o caminho é o bloco dentro do XML (e ele tem limite)

Substituir uma nota na prefeitura foi implementado pelo bloco `RpsSubstituido`
dentro do XML ABRASF — confirmado contra o XSD do município.

**Só funciona para nota emitida por este sistema**, que tem RPS "de verdade"
(número igual ao da nota, série "1", tipo 1). Nota emitida **manualmente no
portal** tem RPS com série vazia e tipo 0, e aí a prefeitura se contradiz: ela
*guarda* o RPS assim, mas o XSD de *envio* recusa esses valores (a série exige
pelo menos um caractere, e o tipo só aceita 1, 2 ou 3).

O resultado prático, que custou tentativa: mandando série 1 dá **E76**; mandando
série vazia dá **erro de schema**. Não há terceira saída.

Chegou-se a criar uma opção "0" na tela para tentar mandar o padrão manual. **Foi
revertida** — gerava XML inválido. Hoje a tela orienta a ir pelo portal nesses
casos, e o `/recuperar` faz o resto.

> **Sobra de código a limpar:** o `web.py` ainda calcula duas variáveis
> (`_ss_0`/`_ss_1`) e carrega um campo escondido `serie_sub` que vieram daquela
> opção revertida. Não têm mais efeito nenhum — o seletor que as usava não
> existe. É lixo inofensivo, e fica anotado para quem for mexer ali não achar
> que está desativando alguma coisa.

**As substituições 3069→3081, 3070→3082 e a 3083 foram resolvidas assim:** botão
"Substituir" do portal (que usa o identificador interno dele) e, em seguida, o
`/recuperar` para os efeitos internos — Pipefy, Omie, Drive, WhatsApp, planilha.

### Substituir só por valor igual ou maior — e a tela barra antes de tentar

Regra do município. A tela confere e recusa antes de enviar, explicando que
valor menor (ou troca de competência) é caso de **cancelamento**, que é outro
assunto. Barrar antes vale mais que uma mensagem de erro da prefeitura depois.

### As retenções do Omie são da medição INTEGRAL, e só na primeira nota

Quando uma medição é faturada em várias notas parciais, só a **primeira** grava
as retenções no título — e grava as da medição inteira, não as da parcela. Da
segunda em diante, o sistema só **acrescenta o número** ao documento fiscal
(`3001/3072`). Duas notas gravando retenção no mesmo título dobrariam o valor.

### O nacional vem pela SEFIN, não pelo portal

O login do portal da prefeitura expira em cerca de uma hora, o que torna
impossível qualquer automação que dependa dele. A busca pela SEFIN usa **só o
certificado**, e acha a nota pelo ID da DPS, que é derivado do número. Por isso
a nota nacional fecha em minutos, sozinha, logo depois da emissão.

O ADN por NSU ficou como **rede de segurança** — é mais lento e entra na fila de
distribuição.

### Cada passo do pós-emissão falha sozinho

No `concluir.py`, cada um dos dez passos tem o próprio tratamento de erro e o
próprio log. Isso é deliberado: depois que a prefeitura devolve o número, a nota
**existe**, e o pior desfecho possível seria uma falha no Drive abortar a
gravação na planilha. Melhor dez passos independentes, com o log na tela,
e ferramentas para refazer o que faltou.

### As credenciais vêm da planilha, mas a variável de ambiente ganha

É o mesmo padrão do `analisesps` (que, aliás, copiou daqui). A aba `Credenciais`
existe para os scripts de linha de comando rodarem fora do Render; em produção,
o que vale é o que está no Render.

---

## A prefeitura desligou o modelo da nota — 07/10/2026

### O que aconteceu

O dono tentou emitir e a prefeitura respondeu:

> *[E999] Com a Obrigatoriedade do IBS CBS o modelo Abrasf foi desativado e deve
> ser migrado para o modelo de DPS.*

Não foi erro de dado, de certificado ou de nota. O sistema falava com a
prefeitura num formato — o **ABRASF** — e a prefeitura parou de aceitá-lo, por
causa da obrigatoriedade do IBS e da CBS da reforma tributária. Passou a aceitar
só a **DPS** (Declaração de Prestação de Serviço) do padrão nacional.

**A emissão ficou parada.** Nenhuma nota saiu errada e nenhuma ficou pela
metade: o envio era recusado na porta. Mas a empresa não conseguia faturar.

O dono baixou no portal o pacote `Layout_EL_DPS_Nacional` (manual, schemas
oficiais e exemplos) e passou aqui, com os dois endereços novos.

### O que salvou tempo

Metade do caminho já existia. O `el_nfse_nacional.py` — um cliente completo do
formato nacional, com montagem, assinatura, compactação e consulta — estava
nesta pasta **desde setembro**, escrito para a emissão automática do ERP e nunca
ligado aqui. A primeira coisa feita foi gerar uma declaração com ele e conferir
contra o schema oficial: **passou de primeira**, inclusive o grupo do IBS/CBS.

Então a migração não foi reescrever o emissor. Foi: ligar o que existia, traduzir
a nota calculada para o formato novo, e consertar o que a conferência contra o
schema revelou de errado.

### As duas armadilhas que a conferência revelou

Estas são a razão de a migração não ter sido "trocar o endereço e pronto".

**1. O tipo de retenção do ISS estava INVERTIDO no código.**

O campo tem este domínio oficial, no schema:

| valor | significado |
|---|---|
| 1 | **NÃO** retido |
| 2 | retido pelo **tomador** |
| 3 | retido pelo intermediário |

E o código dizia, em comentário, exatamente o contrário — *"1 = retido na fonte,
2 = não retido"* — com o default em 1.

**O que isso faria:** as notas da BWS têm ISS retido na fonte. Mandando 1, cada
nota declararia à prefeitura que **quem deve o ISS é a BWS**, e não o tomador que
já descontou. Imposto declarado no lugar errado, numa nota que não se apaga, e
sem nada na tela acusando — o PDF continuaria mostrando "Retido na Fonte",
porque o PDF é desenhado a partir dos nossos dados.

**Conserto:** o default passou a ser 2, e os números ganharam nome
(`RET_ISS_TOMADOR`, `RET_ISS_NAO_RETIDO`) para ninguém mais precisar lembrar qual
é qual. Há teste exigindo que nota retida saia como 2.

⚠️ **O ERP tem a MESMA inversão, e não foi mexido.** Em
`app/apps/erp/core/notas_emitidas/automatica.py` a emissão automática passa
`1 se a obra tem ISS retido, senão 2` — ou seja, o contrário do certo. Não foi
corrigido aqui porque é outra área, e a regra do `CLAUDE.md` é não mexer nas
outras. **Precisa ser levado ao chat do ERP.** Atenuante: aquela emissão pode
nunca ter sido usada em produção — vale conferir antes de assustar.

**2. A dedução de material não tinha para onde ir.**

Em setembro o repositório viveu o incidente mais caro desta área: o sistema não
enviava a dedução de material, e a prefeitura calculava o ISS sobre o valor
cheio. No formato nacional esse campo tem outro nome e outro lugar
(`vDedRed/vDR`), e o cliente que existia **não o montava** — porque fora escrito
para o ERP, que não usa dedução.

Ou seja: migrar sem notar isso **recriaria o incidente de setembro**, inteiro.

**Conserto:** a dedução entra na declaração, na posição que o schema exige, com o
mesmo cálculo de antes (valor total menos a base do ISS). Há teste conferindo que
o valor do serviço menos a dedução dá exatamente a base do ISS, e que em nota
`100/0` o grupo não é enviado em vez de ir zerado.

### A terceira divergência: o endereço de produção

O manual em PDF diz que o ambiente é sempre um segmento do caminho
(`/api/nacional/{ambiente}/nfse`). O portal da prefeitura, de onde o dono copiou,
publica a produção **sem** esse segmento: `/api/nacional/nfse`.

**Quem ganhou: o portal** — é o que está no ar hoje. O código monta o endereço
sem o segmento em produção e com ele em homologação, e isso está num teste, para
a decisão não se perder. Se um dia a produção aceitar os dois, nada precisa
mudar. Errar aqui é inofensivo: dá erro de endereço, não nota errada.

### O que mudou no comportamento, e o que NÃO mudou

**Não mudou nada do que importa para quem usa:** a conta das retenções, o corpo
da nota, o teto de valor, as críticas que barram a emissão, os documentos que o
cliente recebe, a planilha, o Omie, o card, o WhatsApp.

**Mudou o jeito de a nota voltar.** No modelo antigo o envio devolvia a nota na
mesma resposta. Agora a prefeitura confirma que recebeu a declaração e devolve um
protocolo; a nota fica pronta segundos depois e é preciso perguntar por ela.

Isso cria uma situação nova que precisou de decisão própria, abaixo.

**Mudou para melhor:** a nota nacional deixou de ser um segundo ato. Antes ela
saía minutos depois, por um job que ficava perguntando à SEFIN se a nota havia
subido. Agora a emissão JÁ é pelo nacional — a chave vem na resposta, a DANFSe
sai junto dos outros documentos, e nada fica pendente. O job e as telas de busca
nacional continuam de pé **só para as notas antigas**.

**Deixou de existir o código de verificação.** Era do modelo antigo. Quem
identifica a nota agora é a chave de acesso de 50 dígitos. No PDF municipal o
campo do código vai **vazio, de propósito** — inventar um número ali seria pior
do que deixá-lo em branco.

---

## Decisões tomadas na migração, e por quê

### Entre o envio e a resposta, a nota pode existir — então não se reenvia

É a decisão mais importante da migração, e é sobre o que fazer quando dá errado.

Se a prefeitura **recusa a declaração**, não existe nota: é seguro corrigir e
tentar de novo, e a tela diz isso.

Mas se ela **aceita** e a nota não fica pronta no tempo esperado, a nota **pode
ter saído**. Nesse caso a tela mostra a identificação da declaração e manda
consultar — e **não oferece "tentar de novo"**. Oferecer o botão ali seria
convidar a emitir a segunda nota do mesmo serviço, que é o pior desfecho possível
nesta área: a primeira não se apaga.

Os dois casos são tipos de erro diferentes no código justamente para que ninguém
os trate igual por descuido.

### Ensaio em homologação antes de emitir de verdade

A prefeitura tem um ambiente de teste. A tela ganhou uma caixa **"Ensaiar
primeiro"** que manda a mesma nota para lá: ela volta inteira, com número e
chave, e **não vale como documento fiscal** nem grava nada na planilha, no Omie,
no card ou no Drive.

Por que isso virou parte da entrega e não um extra: **não existe "quase emitir"
em produção.** Até aqui, a única forma de conferir uma mudança no caminho de
emissão era emitir uma nota de verdade. Com o ensaio, dá para ver o resultado
antes — e numa área onde o erro não se desfaz, isso vale mais que qualquer teste
automatizado.

Existe também a variável `EMISSAO_NF_AMBIENTE=HOMOLOGACAO`, que trava o serviço
inteiro em teste. A tela avisa em letras grandes quando está travada, porque uma
nota de teste que alguém pense ser real é ruim de outro jeito: cobra-se o cliente
por um documento que não existe.

### Os schemas oficiais entraram no repositório

Os arquivos `.xsd` do pacote da prefeitura estão versionados em
`app/apps/emissaonf/xsd_nacional/`. Não é documentação: é **a regra conferida
pelo teste**. Com eles dentro, um campo fora de ordem, um valor fora do domínio
ou uma casa decimal sobrando é pego aqui — não na prefeitura, não numa nota.

Foi assim que as duas armadilhas acima apareceram antes de qualquer envio.

### Uma resposta de prefeitura de mentira, para testar o que vem depois

Metade do risco desta migração não estava em emitir: estava em emitir e os
**documentos** saírem errados, com a nota já criada. O PDF da nota, a DANFSe e o
valor do recibo são todos desenhados a partir do XML que a prefeitura devolve — e
esse XML mudou.

Então o `nfse_exemplo.py` monta a resposta que a prefeitura daria, a partir de uma
declaração nossa. Ela é conferida contra o schema oficial da NFS-e (senão os
testes estariam provando que o sistema lida bem com algo que nunca chegaria) e
depois passa por todos os geradores.

Isso não é produção e não emite nada. Vive no módulo, e não dentro de `tests/`,
porque os módulos daqui se importam de forma plana e porque às vezes é preciso
gerar uma amostra à mão para conferir um layout de PDF.

### O PDF municipal continua existindo, traduzido

Dava para argumentar que no modelo nacional o documento oficial é a DANFSe e que
o PDF no layout da prefeitura podia ser aposentado. **Não foi essa a escolha:**
é o documento que o cliente da BWS está acostumado a receber, e trocá-lo sem
ninguém pedir seria mudar o que a empresa entrega por conveniência de quem
programa.

Em vez de refazer o desenho, foi escrita uma tradução: o mesmo PDF agora é
desenhado a partir do XML nacional. Ganhou de brinde uma coisa que antes só vinha
depois — a nota municipal já sai **com a chave de acesso e o QR**, porque a chave
existe desde a emissão.

### Substituição ficou de fora, e isso é escolha

No modelo antigo a nota nova carregava, dentro dela, a identificação da nota que
substituía. No nacional a substituição é um **evento** registrado sobre a nota já
emitida, por outra operação da API.

**Não foi implementado**, e a tela agora explica isso em vez de deixar tentar e
falhar. O motivo: um evento de substituição não dá para ser ensaiado sem antes
emitir uma nota de verdade para substituir — então a primeira prova de que o
código funciona seria em cima de uma nota real, com uma segunda nota real atrás.
Numa área onde nada se apaga, isso é caro demais para ser feito no mesmo dia de
uma emergência.

**O caminho de hoje:** botão "Substituir" do portal + `/emissao/recuperar`. É o
mesmo que já se usava para nota emitida manualmente, e funcionou nas
substituições de setembro. Implementar o evento é pendência registrada.

### Dois consertos feitos depois de publicar, no mesmo dia

**1. A CSLL retida sozinha não era declarada como retenção.**

No formato nacional existe **um código só** que diz quais dos três — PIS, COFINS
e CSLL — foram retidos. O código estava mandando esse código apenas quando PIS ou
COFINS entravam. Numa obra com categoria `IR,CSLL`, o valor da CSLL viajava
sozinho, sem nada na declaração dizendo que houve retenção.

Agora o grupo vai sempre que **algum dos três** foi retido, com o código certo
(há teste para as oito combinações). Duas sutilezas que ficaram escritas no
código, porque não são óbvias:

- **alíquota e valor só do que foi de fato retido.** Mandar "0,00" num imposto
  não retido não é o mesmo que não mandar: o primeiro declara uma retenção de
  valor zero;
- **quando nenhum dos três é retido, o grupo não vai** — mesmo comportamento do
  modelo antigo, que só mandava imposto retido.

**2. O endereço de produção passou a ter plano B.**

A divergência entre o manual e o portal (acima) só se resolveria na primeira
emissão de verdade, com um erro de endereço. Agora, se o endereço do portal
responder que **não existe**, o sistema tenta o do manual.

**Isto só é seguro por um motivo, e ele é o que importa:** as respostas 404 e 405
provam que o endereço não existe — a prefeitura não recebeu declaração nenhuma e
**nada foi criado**. Repetir aí não arrisca uma segunda nota.

**Em qualquer outra resposta não se repete nada.** Um 400, um 500 ou um erro de
rede podem ter chegado à prefeitura; repetir o envio nesses casos arriscaria a
segunda nota do mesmo serviço, que é o pior desfecho possível nesta área. Há
teste percorrendo os códigos de resposta justamente para que essa trava não seja
afrouxada por descuido depois.

Quando o plano B funciona, o log diz qual caminho era o certo — então a primeira
emissão de verdade também responde a pergunta do manual contra o portal.

### O PDF em branco que teria apagado o documento bom

Terceiro conserto do dia, e é o que mais assusta de todos.

Os arquivos sobem no Drive **com o mesmo nome**, de propósito: mesmo link, nada
duplicado na planilha nem no card. O outro lado disso é que **um PDF ruim não
fica ao lado do bom — ele toma o lugar dele.**

E havia um caminho para gerar um PDF ruim. A tela `/emissao/regerar` (usada para
regravar PDFs com o layout novo) lê o XML arquivado no Drive **sem saber de qual
modelo ele é** — e, para nota emitida de 07/10/2026 em diante, o que está
arquivado é o nacional. O leitor do modelo antigo, recebendo um XML nacional,
**não dá erro**: ele não acha nenhum campo e devolve tudo vazio. O resultado
seria um PDF em branco subindo com o nome do documento que o cliente recebeu.

Ninguém teria visto acontecer: a tela diria "regravada".

**Dois consertos, e os dois na raiz, para valer em todo caminho:**

1. **a decisão de qual leitor usar passou a ser pelo CONTEÚDO do XML**, dentro do
   gerador do PDF — não por quem chama. Assim qualquer caminho que leia XML do
   Drive acerta, inclusive os que ninguém pensou ainda;
2. **nota sem número não gera PDF nenhum.** É o sinal de que a leitura não
   entendeu o XML; melhor falhar alto, com o motivo escrito, do que entregar um
   arquivo vazio. A mensagem diz exatamente isso: *"gerar aqui sobrescreveria,
   no Drive, o documento bom por um em branco"*.

**A lição, que vale além desta área:** "mesmo nome = mesmo link" é uma boa
decisão de arquivo e uma armadilha de segurança. Onde um arquivo substitui outro,
o código tem de se recusar a escrever lixo — não basta ele não dar erro.

### A varredura pelo resto da mesma armadilha

Os três consertos acima eram do MESMO tipo: código que recebe o formato novo e
**não reclama** — devolve vazio e segue. Em nota fiscal isso é pior que erro,
porque ninguém vê acontecer. Então valeu varrer o módulo procurando os outros.

Achou mais um: **o valor bruto que vai para o recibo.** Ele era lido por busca de
texto no campo do modelo antigo; recebendo o XML nacional, devolvia nada, e o
recibo sairia sem valor. Passou a decidir pelo conteúdo, como o gerador do PDF.

E ficou marcado, em letras grandes no topo dos arquivos, que **`emitir_real.py` e
`app_emissao.py` falam o modelo desativado** — qualquer envio por eles volta com
o E999. Não é defeito deles; é o canal que não existe mais. São scripts de linha
de comando, fora do caminho da tela, e ficam como registro.

**A regra que saiu disso, e vale para a próxima vez que um formato mudar:** não
basta trocar quem escreve. Tem de varrer quem LÊ — e, em cada leitor, perguntar
*"o que este código faz se receber o formato errado?"*. Se a resposta for "devolve
vazio e segue", ele é um defeito esperando a hora.

### Os dois tokens, e a tela que "não funcionou" — 07/10/2026

Primeira tentativa de ensaio depois de publicar, e ela parou em duas coisas.

**O que o dono viu:** o `/emissao/diag` "não funcionou", e o ensaio respondeu
*"Token de integração da prefeitura ausente"*. A dúvida dele foi exata: *"eu já
emiti várias notas e sempre funcionou, e no Render tem a chave
EMISSAO_NF_TOKEN — essa EL_NFSE_TOKEN é outra?"*

**São outras, sim, e a pergunta estava mais certa do que o código.** São dois
tokens que não se substituem:

- **`EMISSAO_NF_TOKEN`** é **nosso**: protege o endereço da tela de emissão, para
  que o link do card não abra para qualquer um;
- **`EL_NFSE_TOKEN`** é **da prefeitura**: autentica o canal da emissão, e é
  gerado no portal do município, em Configurações › APIs de Integração.

**Por que ele nunca foi necessário antes — e isto é o ponto:** o modelo antigo
autenticava pelo **certificado digital**, no aperto de mão da conexão. Não havia
token nenhum no caminho da emissão. O modelo nacional exige **certificado E
token**. Ou seja: um serviço que emitiu notas por meses sem esse token não estava
mal configurado; a exigência é nova. A pergunta dele ("sempre funcionou") não era
confusão — era a observação correta de alguém que conhece o sistema.

**E o diagnóstico não estava quebrado.** Ele responde 200. O que aconteceu: a
tela exige o token do link no endereço, e **não havia link nenhum para ela na
tela de emissão** — a única forma de chegar lá era digitar o endereço e saber o
token de cor. Sem o token, a resposta é "acesso não autorizado", que **parece
defeito e não é**.

**O que mudou por causa disso:**

1. **a tela de emissão ganhou um rodapé com links** para Diagnóstico, Recuperar
   entrega e Regravar PDFs, cada um já com o token dentro. Ferramenta que só se
   alcança decorando endereço não existe na prática;
2. **o diagnóstico passou a explicar os dois tokens lado a lado** — de quem é
   cada um, para que serve, onde se consegue, e o que acontece sem ele. Mais a
   informação que faltava: **de onde** o token veio (variável de ambiente ou
   planilha), porque "está configurado" sem dizer onde não ajuda a consertar;
3. **ele lista os NOMES das credenciais da planilha**, nunca os valores, para
   achar o token quando está lá com outro rótulo. A busca também passou a aceitar
   vários nomes (`EL_TOKEN`, `NFSE_TOKEN`, `TOKEN_PREFEITURA`…), porque o token é
   anterior a este código;
4. **a mensagem de erro da emissão passou a ensinar**: diz que são dois tokens
   diferentes, por que o antigo não precisava, e onde conseguir o novo.

**A lição, e ela não é sobre token:** *"não funcionou"* numa ferramenta de
diagnóstico quase sempre é a ferramenta sendo inalcançável, não quebrada. E
**nenhum teste tocava nas telas** — por isso um 403 numa página sem link para
ela passou batido por toda a migração. Agora há `tests/test_emissaonf_telas.py`,
que abre as telas de verdade e exige, entre outras coisas, que o diagnóstico
nunca mostre valor de credencial.

### O caminho inteiro da emissão passou a ser exercitado sem prefeitura

Depois de perder uma ida e volta do dono com o diagnóstico inalcançável, ficou
claro o que faltava: **nenhum teste clicava no botão.** Os testes provavam que a
declaração estava certa e que os documentos saíam certos, mas a *ligação* entre
as peças — nome de campo, ordem de argumento, ordem das conferências — só era
exercitada quando ele emitia.

Agora o `tests/test_emissaonf_emissao_ponta_a_ponta.py` roda o caminho de
verdade: o motor fiscal calcula, a declaração é montada e **assinada de verdade**
(com um certificado descartável criado no próprio teste), e uma prefeitura
dublada recebe o envio — conferindo que ele chegou compactado como o manual
manda — e devolve a nota. Só o pós-emissão é dublado, porque ele escreve em
planilha, Omie, card e Drive.

**Ele achou dois defeitos na primeira execução, e os dois eram reais:**

1. **A explicação da substituição era inalcançável.** O aviso de que substituir
   pela tela não funciona mais vinha DEPOIS de carregar o card e conferir os
   slots. Quem tentasse substituir uma nota que não estivesse nos slots recebia
   *"confira o número no parâmetro nota_substituida do link"* — uma mensagem
   sobre um parâmetro, quando a resposta certa é "isto não funciona mais, use o
   portal". A explicação subiu para antes de tudo, e a conferência de slots e a
   regra de "valor igual ou maior" saíram: elas só existiam para decidir se a
   substituição podia ser feita aqui, e aqui ela não é mais feita.

2. **O texto do aviso apareceria com as marcações cruas.** A tela de erro escapa
   o texto — e com razão, porque quase sempre ele vem de uma exceção ou de uma
   resposta de fora. Mas aquele aviso é escrito por nós, com negrito e
   parágrafos. Virou uma função separada (`_pagina_explicacao`), e a separação é
   de propósito: a diferença entre as duas é escapar ou não, e isso não pode
   depender de alguém lembrar de passar um parâmetro.

**E um detalhe de estrutura que vale saber antes de escrever teste de tela:** o
`web.py` é carregado **duas vezes**, com dois nomes — `web` (o import plano, como
os módulos desta pasta se importam entre si) e `app.apps.emissaonf.web` (o
pacote, de onde o Flask registra o blueprint). Quem atende a requisição é o
segundo. Trocar uma função no primeiro não tem efeito nenhum sobre o que roda —
foi o que fez o primeiro teste do token passar quando não devia.

### A nota 3281: a prefeitura aceitou e a nota não ficou pronta — 07/10/2026

**O que aconteceu.** Primeira emissão real pelo modelo novo. A prefeitura
aceitou a declaração `DPS...260000000003281` (nota **3281**) e ainda estava
processando quando a espera de 150s acabou. A tela mostrou o aviso previsto:
*"NÃO emita de novo: a nota pode ter saído."*

**Isto não é defeito do nosso lado.** O processamento da declaração é uma fila da
prefeitura, e o manual diz isso com todas as letras: o retorno HTTP 201 significa
que ela RECEBEU, não que a nota já foi autorizada. Às vezes a fila demora mais
que a nossa espera.

**Mas a mensagem mandava para o lugar errado, e isso era defeito meu.** Ela dizia
para usar a tela *"Fechar nacional pela chave"* — e essa tela só trabalha com
notas que ficaram na fila do **Controle Nacional**. Uma nota nesta situação não
está lá: o pós-emissão nunca rodou, porque a emissão não chegou até ele. A tela
indicada não tinha o que fechar.

Pior: a mensagem pedia para "consultar no portal", o que deixava o trabalho todo
na mão do dono — e, se a nota tivesse saído, **nada** estaria feito: nem planilha,
nem Omie, nem card, nem Drive, nem aviso.

**O que foi feito: a tela "Conferir declaração".** Ela recebe a identificação da
declaração, pergunta à prefeitura se aquilo já virou nota, e:

- se **ainda não** virou, diz isso e manda esperar — deixando claro que isto
  **não** autoriza emitir de novo;
- se **virou** e o card foi informado, **termina o serviço**: planilha, Omie,
  card, Drive e avisos, exatamente como a emissão teria feito. Sem emitir nada;
- se virou e o card não foi informado, mostra o número e a chave, e avisa que
  falta terminar.

Três decisões dentro dela, cada uma por um motivo:

1. **Consultar não cria nada, então pode repetir à vontade** — e o passo que
   termina o serviço tem a trava anti-duplicação do `concluir`. Uma ferramenta de
   emergência que a pessoa tem medo de usar duas vezes não serve.
2. **O número da nota é extraído da identificação** e mostrado na tela. Ler 45
   dígitos à mão para descobrir de que nota se trata é pedir erro.
3. **A tela avisa, em vermelho, para não emitir enquanto não souber a resposta.**
   É o único lugar do sistema onde a pressa cria uma segunda nota fiscal do mesmo
   serviço.

**E um defeito de código que isso revelou:** a espera (`ESPERA_TOTAL_S`) era valor
padrão de argumento — congelado quando a função nasce. Mudar a constante não
tinha efeito nenhum, e um teste que tentou encurtar a espera rodou os 150
segundos inteiros. Agora o teto é lido dentro da função.

**O que ficou em aberto, e precisa de dado real:** se a fila da prefeitura
costuma passar de 150s, a espera deve subir. Não mexi no valor sem saber: subir
cegamente pendura a tela por minutos e ocupa uma das quatro linhas de atendimento
do serviço. A tela de conferir resolve o caso sem esse custo — e, com algumas
emissões, dá para saber se vale subir.

### O terceiro desfecho que faltava: a declaração RECUSADA — 07/10/2026

Depois do aviso da nota 3281, o dono foi conferir e disse: **"não existe nota
3281 emitida, você fala da próxima?"**

Duas coisas saíram daí, e as duas eram defeito de comunicação do sistema.

**1. O número da declaração é o NOSSO pedido, não o número da nota.**

O número que vai dentro da declaração vem da nossa planilha (o maior da coluna F
mais um). O número de verdade da nota só existe quando a prefeitura **autoriza**.
Então "não existe nota 3281" é perfeitamente compatível com "a declaração da 3281
foi enviada" — e a tela não deixava isso claro.

**2. Havia um terceiro desfecho, e o sistema o tratava como falha nossa.**

O manual é explícito: quando a resposta da plataforma traz a lista `erros`, a
solicitação **não foi processada**, alguma correção é necessária, e **a mesma
declaração pode ser reenviada com a correção, mantendo a mesma identificação**.

Ou seja, são **três** desfechos, não dois:

| | O que é | Existe nota? | O que fazer |
|---|---|---|---|
| **Pronta** | virou nota | sim | terminar o serviço |
| **Ainda processando** | na fila da prefeitura | ainda não | esperar e consultar de novo |
| **Recusada** | a plataforma rejeitou | **não** | corrigir e **reenviar, com o mesmo número** |

O sistema conhecia os dois primeiros. O terceiro caía no `except Exception` e
aparecia como *"não consegui consultar"* — transformando **"a prefeitura recusou,
e aqui está o motivo"** em **"algo deu errado aqui"**. Pior: na emissão, a recusa
era engolida e a espera de 150s rodava **inteira**, para no fim mostrar um aviso
que não dizia o motivo.

**O que mudou:**

- a consulta passou a devolver a recusa como **resposta**, não como erro de
  consulta. São coisas diferentes, e tratá-las igual foi o que mandou o dono
  para a tela errada;
- **a emissão para na hora** quando a declaração é recusada — poupa a espera
  inteira e diz o motivo de verdade, com o código de erro da prefeitura;
- as duas telas ganharam a mensagem certa: **"nenhuma nota foi criada, pode
  corrigir e emitir de novo — inclusive com o mesmo número, que é o caminho
  previsto pela prefeitura"**;
- o aviso do estouro de espera deixou de insinuar que a nota existe. Ele agora
  diz que **pode** existir ou **pode** ter sido recusada, e que só a consulta
  diz qual é.

**A lição, e ela vale para qualquer integração:** o desfecho que o código não
conhece não desaparece — ele vira a mensagem genérica, e a mensagem genérica
manda a pessoa para o lugar errado. Aqui custou uma espera de 150 segundos e uma
ida e volta do dono para descobrir que **não havia problema nenhum**: só uma
declaração recusada, que é o caso mais fácil dos três.

### O ensaio que não terminava, e o erro cujo texto engana — 07/10/2026

O dono tentou ensaiar e voltou: **"não consegui concluir o ensaio, tá demorando
muito"**. Três coisas saíram daí.

**1. Prender a tela no ensaio não compra nada.** A espera de 150s existia para
que a emissão conseguisse terminar o serviço (planilha, Omie, card, Drive) na
mesma visita. **No ensaio não há serviço para terminar** — então a tela girava
dois minutos e meio em troca de nada, e ele desistiu antes do fim. A espera do
ensaio caiu para 30s, e as duas ficaram ajustáveis por variável de ambiente
(`EMISSAO_NF_ESPERA_S` e `EMISSAO_NF_ESPERA_ENSAIO_S`), porque ainda não se sabe
como a fila da prefeitura se comporta no dia a dia e adivinhar um bom número
agora seria chute.

**2. A tela de "ainda processando" entrega um BOTÃO, não um código.** Antes ela
mostrava a identificação de 45 caracteres para a pessoa copiar e colar na outra
tela. Agora é um link que já leva a identificação, o card e o ambiente dentro.

**3. O erro E0037 diz uma coisa e significa outra.** O manual da prefeitura tem
uma seção própria para ele:

> ⚠️ **Nota de 08/10/2026:** este item dizia "e isso resolve o mistério do
> ensaio". **Não resolvia nada** — o E0037 nunca foi visto acontecer aqui. Ler o
> manual e concluir que o ensaio estava bloqueado foi um salto, e ele parou de
> ser tentado por quatro correções seguidas. Ver a correção na seção de estado.


> O texto do erro diz que o município não existe no cadastro nacional, mas na
> prática ele ocorre quando **o município ainda não configurou a Produção
> Restrita** junto à Plataforma Nacional. O município precisa habilitar o módulo
> e concluir as configurações de convênio. *O contribuinte deverá entrar em
> contato com a prefeitura e solicitar a habilitação.*

**Ou seja: SE este erro aparecer**, o ambiente de teste não está habilitado — e
isso não é defeito nosso nem dos dados da nota. Não há o que corrigir aqui: é um
pedido à prefeitura. **Enquanto ele não aparecer, não há motivo para supor que o
ensaio esteja bloqueado**, e o ensaio é a única forma de provar uma mudança sem
gastar nota.

Por isso o sistema passou a **traduzir** esse erro na tela, dizendo o que ele
realmente significa e de quem é a ação. Texto cru de integração manda a pessoa
procurar o problema no lugar errado — e, neste caso, procurar nos dados da nota,
onde ele não está. A tradução só existe para erros documentados; erro
desconhecido aparece cru, porque explicar errado é pior que não explicar.

**Consequência prática, se o ensaio não for habilitado:** a conferência de uma
emissão real passa a ser a tela "Conferir declaração". Não é o ideal — o ideal é
ensaiar —, mas é seguro: ela pergunta à prefeitura e termina o serviço, sem nunca
emitir nada.

### ⚠️ Uma correção a um commit anterior: o conserto da espera não tinha subido

Fica registrado porque é exatamente o tipo de coisa que corrói a confiança no
histórico: o commit *"a saída do aperto"* afirmou que o defeito do valor padrão
congelado (`espera_total_s`) estava corrigido. **Não estava.** O comando que
aplicava a correção morreu antes de rodar, e eu não conferi o resultado antes de
seguir.

**Como isso passou por uma suíte verde:** o sintoma era a suíte ficando **três
vezes mais lenta** (de 60s para 200s), porque um teste rodava os 150 segundos
inteiros girando. Ninguém liga uma suíte devagar a um defeito de código — e
nenhum teste falhava.

Agora a correção está aplicada de verdade, e há um teste que olha a **assinatura
da função** e acusa se o valor padrão voltar. Ele existe porque o sintoma natural
deste defeito é lento e silencioso: sem um teste olhando direto para a causa,
ele volta e ninguém vê.

### "Aguardando Transmissão", o Bad Gateway, e a declaração que vivia só na tela

Três coisas no mesmo fim de tarde de 07/10/2026, e as três têm a mesma raiz: eu
desenhei a emissão como se a nota voltasse na mesma visita, e ela não volta.

**1. O portal da prefeitura mostrou o estado de verdade.** O dono foi lá e trouxe:

```
DPS ...260000000003281 | Tipo: API | Situação: Aguardando Transmissão
Data: 07/10/2026 | Número Nfs Reservado: 3281 | Chave Nacional: (vazia)
```

Isso diz tudo: a prefeitura **recebeu e aceitou**, **reservou o número 3281** para
aquela declaração, e **ainda não transmitiu** para a plataforma nacional. A nota
só existe como documento fiscal quando a chave nacional aparece.

E traz duas consequências práticas que o sistema não estava dizendo: **não emitir
com outro número** (o 3281 está reservado para essa declaração) e **não
reenviar** — basta consultar depois. O aviso de "ainda processando" passou a
explicar esse estado com essas palavras.

**2. O Bad Gateway, e este é o mais grave: o desenho da emissão derrubava o
serviço inteiro.**

O serviço atende **4 pedidos ao mesmo tempo** (1 worker, 4 threads) e é
compartilhado com o ERP, o painel e todo o resto. Cada emissão esperando a nota
prendia **uma dessas quatro linhas**, por até 150 segundos. Umas poucas
tentativas seguidas ocuparam as quatro — e o monorepo **inteiro** passou a
responder *"Bad Gateway"*.

Ou seja: uma fila do lado da prefeitura virava indisponibilidade do ERP. Isso não
é desconforto de tela, é defeito de arquitetura, e foi meu.

**A espera caiu para 25s** (15s no ensaio). Ela serve só para o caso feliz, em
que a nota sai em segundos e dá para terminar o serviço na mesma visita. Quando
não sai, quem termina é a tela "Conferir declaração". Há teste exigindo que a
espera continue curta, com o motivo escrito nele — porque o número parece
inofensivo e não é.

**3. A declaração vivia só na tela aberta no navegador.**

Entre o aceite e a nota, o único registro da identificação era a página que o
dono estava olhando. Publicar o serviço, fechar a aba ou cair a conexão perdia o
rastro — e foi o que aconteceu: a identificação da 3281 só foi reencontrada
porque ele foi procurar **no portal da prefeitura**.

Agora existe a aba **"Declaracoes"**, e a declaração é gravada nela **antes de
qualquer espera**. Três decisões dentro disso:

- **gravar é a primeira coisa depois do aceite.** Não depois da espera, não "se
  der tempo". É o "antes" que garante que nada se perde;
- **se a gravação falhar, a emissão NÃO para** — a declaração já está com a
  prefeitura, e abortar não desfaz nada. Mas **reclama alto no log**, com a
  identificação, porque silenciar aí seria recriar o problema que a gravação
  existe para resolver;
- **a tela "Conferir declaração" LISTA o que está em aberto**, com um botão em
  cada. Assim ninguém precisa guardar 45 caracteres: é por isso que a lista
  existe, não por enfeite.

**A lição que atravessa as três:** quando o outro lado tem fila, esperar por ele
dentro de um pedido web é pedir para transformar a lentidão dele em
indisponibilidade nossa. O certo é registrar o protocolo, soltar a linha, e ter
uma tela que fecha o ciclo depois.

### A fila mudou de dono, e eu perguntava para o lado errado — 07/10/2026

A tela de conferir finalmente mostrou a resposta crua da prefeitura, e ela era
diferente do que eu supunha:

```
O que ela respondeu agora: <em processamento adn nacional>
```

**"ADN nacional" é o Ambiente de Dados Nacional.** Ou seja: a prefeitura **já
transmitiu** o documento. A fila deixou de ser dela — quem tem de autorizar agora
é a plataforma nacional. O estado "Aguardando Transmissão" que o portal mostrava
antes já havia passado.

**E a partir desse momento a prefeitura deixa de ser a melhor fonte.** A resposta
dela pode continuar nesse mesmo texto mesmo depois de a nota existir no nacional:
ela está dizendo "entreguei", não "não existe".

**O que eu não estava fazendo, e deveria:** o sistema **já sabia** perguntar
direto à plataforma nacional, pelo certificado — é assim que ele reencontra nota
antiga desde setembro (a busca por DPS/chave na SEFIN). Eu simplesmente não
liguei isso na consulta nova. Então a conferência tinha uma fonte só, e era a
fonte que para de saber justamente quando o documento sai da mão dela.

**Agora a consulta pergunta nos dois lugares:** primeiro à prefeitura; se ela
disser que está no nacional, pergunta direto ao nacional. A mesma segunda fonte
entrou também na espera da emissão.

**Três cuidados dentro disso:**

1. **falha na segunda fonte não é erro da consulta.** Se a plataforma nacional não
   responder, a tela mostra o que a prefeitura disse e registra no log que a
   segunda fonte falhou. Melhor resposta incompleta que tela de erro;
2. **no ensaio a plataforma nacional de produção não é consultada** — ensaio vive
   em outro ambiente, e perguntar ali daria resposta errada;
3. **as duas filas são explicadas diferente, porque muda a quem se reclama.**
   Antes de transmitir, é com a prefeitura. Depois, a autorização é do ambiente
   nacional — e a pergunta útil para a prefeitura passa a ser se o **convênio do
   município com o ambiente nacional** está em ordem, que é o que costuma travar.

**A lição:** numa integração em etapas, "quem sabe a resposta" muda de mão ao
longo do caminho. Consultar sempre o mesmo lado dá resposta velha — e, pior, dá
uma resposta velha que *parece* atual.

### A nota 3281 não estava em lugar nenhum — e o que foi conferido

O dono checou **os dois sites** — o da prefeitura e o nacional — e a nota não
estava em nenhum. Aí deixou de ser "esperar a fila" e passou a ser "descobrir o
que travou".

**O que foi conferido do NOSSO lado, e está certo:**

- **o código do serviço.** `070202` existe na lista oficial de serviços nacionais
  (Anexo B do pacote) e é exatamente *"Execução, por empreitada ou subempreitada,
  de obras de construção civil…"*;
- **a classificação do IBS/CBS**, que era a minha principal suspeita por ser a
  parte nova e nunca conferida. Está certa, e agora com fonte: a tabela oficial
  (Anexo VIII) diz que o item **07.02** vai com `INDOP` **020201**, `cClassTrib`
  **200046** ("Operações com bens imóveis") e NBS **1.0101.11.00** — que é o que
  o sistema manda;
- **a estrutura da declaração**, que passa no schema oficial (já havia teste).

Ou seja: o conteúdo da declaração confere com as tabelas oficiais. O que sobra
está fora do nosso alcance, e é preciso prova para levar a quem resolve.

**O que foi construído: o "Diagnóstico completo desta declaração".**

Ele pergunta sobre a mesma declaração em três lugares e mostra as respostas
**cruas**:

1. à prefeitura, o processamento da declaração;
2. à prefeitura, a chave;
3. **à plataforma nacional, direto pelo certificado: "você conhece esta
   declaração?"**

**A terceira é a que decide**, e é por ela que a tela existe: se a plataforma
nacional responde que **não conhece** a declaração, enquanto a prefeitura diz que
"está em processamento adn nacional", as duas versões **não fecham** — e a
transmissão é a prefeitura que faz. A tela diz isso com essas palavras, e o texto
é feito para ser copiado e mandado a ela.

**Dois cuidados dentro disso:** o texto **nunca** mostra o token nem nada do
certificado (ele existe para sair daqui, então isso não é detalhe — há teste), e
a falta do token aparece como uma linha explicando, em vez de estourar.

**O que continua sem resposta, e não é nosso:** por que a autorização não sai.
Os dois candidatos são o convênio do município com o ambiente nacional e alguma
fila do lado deles. O diagnóstico é o que transforma "não funciona" em uma
pergunta concreta com evidência.

### O veredito da 3281, e o número que ficava preso — 07/10/2026

O diagnóstico respondeu, e sem ambiguidade:

```
[prefeitura] HTTP 200 — "em processamento adn nacional"   (nos dois endpoints)
[plataforma nacional] HTTP 404 — E2404
   "Não foi gerada uma NFS-e com o identificador de DPS informado"
```

**As duas versões não fecham.** A prefeitura diz que entregou ao ambiente
nacional; o ambiente nacional diz que **não gerou nota** para aquela declaração.
E a transmissão é a prefeitura que faz. Com isso, "não está funcionando" virou um
pedido concreto, com os códigos de erro deles próprios dentro.

**Nada disso é do nosso lado**, e foi conferido antes de afirmar: o código do
serviço, a classificação do IBS/CBS e a estrutura da declaração batem com as
tabelas e o schema oficiais (seção anterior).

### O defeito sério que esse impasse revelou: o número ficava preso e era reusado

Este é o achado que importa para o futuro, e ele teria causado estrago sozinho.

A numeração das notas sai da planilha: **maior número da coluna F mais um**. E a
planilha só recebe **nota pronta** — uma declaração aceita mas sem nota **não
entra lá**.

Resultado: o 3281 ficava "livre" do nosso lado, enquanto a prefeitura o mantinha
**reservado** para a declaração travada. **A próxima nota, de outra medição,
sairia pedindo o mesmo 3281** — e o manual da prefeitura diz que a mesma
identificação é lida como **reenvio da declaração anterior**, não como nota nova.

Ou seja: dois serviços diferentes colapsariam num documento só. Sem erro na tela,
sem ninguém perceber — e documento fiscal não se desfaz.

**O conserto:** o próximo número passou a considerar também os números presos a
declarações em aberto. Três detalhes com motivo:

1. **para o MESMO card o número é reaproveitado de propósito** — ali é o reenvio
   que o manual prevê, com a mesma identificação. Só número de *outro* card conta
   como ocupado;
2. **"o último emitido" continua sendo o da planilha.** Declaração aberta não é
   nota emitida, e misturar as duas coisas num número só confundiria a leitura;
3. **se as declarações não puderem ser lidas, a emissão numera como antes e
   AVISA** que o número pode colidir. Falhar em silêncio aqui devolveria o
   defeito.

### E a tela parou de dizer "espere" para o que está parado

A fila do nacional leva segundos. Uma declaração aberta há horas não é espera: é
coisa travada. A lista de declarações passou a mostrar **"Parada há Xh — isto já
não é fila. Rode o diagnóstico e fale com a prefeitura"**, com link direto para o
diagnóstico. Duas horas é o corte.

Mandar alguém esperar por algo que não vai acontecer sozinho é pior que não dizer
nada — custa o dia dele.

### "Nota emitida no portal": a saída para quando o canal está fora — 07/10/2026

Com o canal travado e a empresa sem poder faturar, o dono pediu:

> *"Minha sugestão é que você crie um botão na tela de emissão de emissão
> manual. Pensei em poder anexar o pdf da nota emitida ou xml, você me diz o
> melhor, e a partir dali você vai fazer a leitura do que foi emitido e fazer o
> processamento e atualizações."*

Feito, e **a escolha entre PDF e XML não é preferência**: é a diferença entre
dado e leitura.

**O XML manda nos dados.** Dele saem número, chave, valores e datas como **dados
exatos**. Do PDF seria preciso *ler* números de um texto — e um valor mal lido
iria para a planilha e para o Omie **sem ninguém notar**. Em documento fiscal
isso não se faz: o erro silencioso é o pior que existe nesta área.

**O PDF, quando anexado, entra como o documento.** O que o sistema desenha é uma
réplica boa; o do portal é o **original**. Tendo o original, é ele que vai para o
Drive e para o cliente — e é melhor assim.

A tela aceita o XML como **arquivo** (era o pedido) ou colado, aceita os dois
modelos (antigo e nacional, decidindo pelo conteúdo), e tem a mesma trava
anti-duplicação do `concluir`. Ela **não emite nada**, e diz isso em letras
grandes.

Erro comum previsto na própria mensagem: baixar o XML da **declaração** em vez do
da **nota**. A tela explica a diferença em vez de só recusar.

### E a pergunta do RPS: a resposta é boa notícia

Junto do pedido, ele levantou uma dúvida importante:

> *"A nota manual não gera número RPS eu acho. Isso será problema quando formos
> emitir nova via API? Uma coisa que não tenho conseguido por conta desse RPS é
> substituir uma nota manual por nota via API."*

**O RPS era um problema do modelo ANTIGO, e ele morreu com o modelo.** No ABRASF,
substituir exigia apontar o RPS da nota antiga, e nota manual tinha RPS com série
vazia e tipo 0 — que a prefeitura guardava mas o XSD de envio recusava. Foi o que
tornou nota manual insubstituível pela aplicação (ver a seção da substituição,
mais acima).

**No modelo nacional não existe RPS.** Quem identifica a nota é a **chave de
acesso**, e a substituição é um **evento** registrado sobre ela. Nota manual tem
chave como qualquer outra. Então o impedimento que o incomodava há meses
**deixou de existir** — não por conserto nosso, mas porque o formato mudou.

**Duas ressalvas honestas:** a substituição por evento ainda **não está
implementada** aqui (segue como pendência), e emitir manualmente **não cria
dívida nenhuma** para a emissão seguinte pela API — desde que a nota seja
registrada por esta tela. É o registro que mantém a numeração alinhada: a
numeração sai da planilha, e nota que não entra nela faria o sistema pedir um
número que o município já usou.

### ✅ O VEREDITO DA 3281: faltava a identificação da obra (E0370) — 08/10/2026

**O dono trouxe, da TI da prefeitura, o erro que a plataforma nacional tinha
guardado:**

> *Pendências / Erros de Transmissão da DPS Nacional — Código: **E0370** — O
> grupo de informações de obra é obrigatório quando o código de tributação
> nacional pertencer a um dos subitens 07.02.01, 07.02.02, 07.04.01, 07.05.01,
> 07.05.02, 07.06.01, 07.06.02, 07.07.01, 07.08.01, 07.17.01, 07.19.01, 14.14.03
> e 14.14.04 da lista de serviços.*

E, no portal, a declaração passou de "Aguardando Transmissão" para
**"Processado com Erros"**. Ou seja: a nota 3281 **nunca existiu e nunca vai
existir**, e o número está livre.

**Isto fecha o impasse de 07/10/2026.** O que estava escrito aqui — "as duas
versões não fecham, a transmissão é da prefeitura" — descrevia o sintoma
corretamente e **errava o dono do problema**. A fila não estava travada: a
plataforma nacional recusou, e a recusa ficou numa tela de pendências que a
nossa consulta não alcança. Durante um dia a consulta respondeu "em
processamento adn nacional" para uma declaração **já morta**.

#### Por que não apareceu em nada que conferimos

É o ponto que vale guardar, porque vai se repetir.

**A declaração sem o grupo de obra é VÁLIDA no schema oficial.** No XSD o grupo
é `minOccurs="0"` — opcional. A obrigatoriedade não está no arquivo: é **regra de
negócio da plataforma nacional**, condicionada ao código do serviço. Então a
conferência contra o schema, que pegou tudo o mais, **não tinha como pegar esta**.

**E o modelo antigo não tinha esse campo.** No ABRASF o CNO ia solto no texto da
discriminação ("CNO Nº 90.025.25410/76") — e ia, em todas as notas, há meses. A
migração traduziu campo por campo o que existia; um campo que **passou a existir**
não aparece numa tradução.

**A divisão de responsabilidade escondeu o resto:** o município aceitou (HTTP
200, `idDPS` devolvido) e o nacional recusou, depois, do outro lado da fila. Os
dois estavam certos sobre a sua parte, e nenhum dos dois contava a do outro.

#### O conserto

**O grupo de obra passou a ir na declaração**, com o **CNO** da obra — que o dono
confirmou estar na **C. Diários, coluna Z, cabeçalho "CNO"**, que é exatamente a
coluna que o carregador já lia (por nome, não por posição).

**Por que o CNO e não as outras duas.** O layout (`TCInfoObra`) aceita três
identificações e exige **exatamente uma**:

| Alternativa | O que é | Por que não |
|---|---|---|
| `cObra` | CNO ou CEI da obra | **é o que usamos** — a BWS tem para cada obra |
| `cCIB` | Cadastro Imobiliário Brasileiro | a empresa não usa |
| `end` | endereço da obra (CEP, logradouro, nº, bairro) | a C. Diários não tem o endereço da OBRA; o que ela tem é o do cliente, que é outra coisa |

As três estão implementadas e provadas contra o schema — o endereço é o que
destrava uma obra sem CNO, e por isso tem de estar pronto **antes** de ser
preciso.

**O número vai sem pontuação**, como todo documento deste layout: o CNPJ, o CPF
e o CEP já eram enviados só com dígitos pelo próprio construtor. Na planilha o
CNO está escrito `90.025.25410/76`; na declaração vai `900252541076`.

**Obra sem CNO barra a emissão ANTES de enviar**, com o motivo escrito e dizendo
onde resolver. Essa escolha tem conta: barrar custa um aviso na tela; deixar
passar custa um número de nota queimado, uma declaração presa na fila e — como
se viu — um dia para descobrir. Sem o `print` de aviso quando o número tem
tamanho diferente de 12 dígitos, um CNO truncado seria a próxima caçada.

**Só nos treze subitens da lista.** Mandar o grupo onde ele não é previsto é tão
errado quanto omiti-lo onde é. A lista do erro é a regra, e nada além dela — a
BWS emite sempre em 070202, então para ela é sempre.

#### Dois consertos que o impasse revelou, e eles não são do E0370

**1. A recusa descoberta pela conferência não liberava o número.** A emissão já
marcava a declaração como recusada; a tela "Conferir declaração" **não marcava**
— e ela é justamente a tela por onde se descobre a recusa que chegou tarde.
Enquanto a declaração fica "aguardando", ela **segura o número dela** (defeito
consertado em 07/10, ver acima), e a numeração pularia 3281 para sempre. Agora
marca.

**2. A API pode nunca contar a recusa.** Foi o caso: um dia inteiro respondendo
"em processamento" para uma declaração que o portal já dava como recusada. Então
a lista de declarações em aberto ganhou, **só nas declarações paradas**, um
"liberar o número": marca como recusada e devolve o número ao uso. Oferecer isso
numa declaração que ainda está na fila convidaria a liberar o número de uma nota
que talvez exista — por isso só na parada, e com confirmação que diz em letras
claras para usar apenas quando o portal mostra a recusa. Não emite, não cancela
e não apaga nada: mexe só no controle de numeração.

**E o E0370 ganhou tradução.** O texto cru fala em "grupo de informações de obra"
e lista treze subitens; quem lê não tem como saber que o que falta é o CNO da
obra na C. Diários. A tela da recusa agora diz isso, e cita a 3281.

#### O que foi conferido, e o que NÃO foi

**Conferido:** a declaração com o grupo de obra passa no schema oficial nas
quatro tributações que a BWS usa; o grupo sai na posição que o XSD exige (dentro
de `serv`, depois de `cServ`); o CNO vai sem pontuação; os treze subitens da
lista exigem o grupo e um código fora da lista não o leva; obra sem CNO barra
antes de enviar, com mensagem que nomeia a coluna e o erro; as outras duas
identificações (CIB e endereço) montam e validam; grupo vazio é recusado em vez
de sair como `<obra/>`; a declaração que a tela de emissão envia de fato leva o
`<cObra>`. São 24 casos novos; a suíte inteira passa (5.133).

**NÃO conferido, e é o que importa:** se a plataforma nacional aceita o **formato**
do CNO que mandamos. Dígitos sem pontuação é a convenção do layout e o que o
resto do arquivo já faz, mas não há no manual um exemplo de `<cObra>` — os dois
exemplos oficiais não são de serviço de obra. E não houve ensaio: a Produção
Restrita do município **pode** não estar habilitada (o erro E0037 indicaria
isso, mas nunca foi observado — ver a correção na seção de estado), então **a
primeira emissão de verdade segue sendo a primeira prova** enquanto o ensaio não
for tentado. Se vier recusa com o grupo presente, o
mais provável é o formato do número, e o conserto é de uma linha.

### Dois erros a mais no mesmo dia: EL99 e E0959 — 08/10/2026

Depois do conserto do grupo de obra, o dono emitiu de novo. Deu **EL99**; e,
pouco depois, na tela de pendências do portal, apareceu **E0959**. São coisas
diferentes e vale separar, porque uma delas desmente o manual.

#### E0959 — o CST não é um campo de escolha

> *E0959 — cClassTrib não pertence ao grupo CST indicado.*

**O que era:** o grupo da reforma tributária ia com `CST = 000` e
`cClassTrib = 200046`. **O CST são os três primeiros dígitos do `cClassTrib`** —
e `200046` é do grupo **200**, não do 000.

Isso está nos dados do **Anexo VIII oficial**, que veio no pacote da prefeitura:
`000001` ("Situações tributadas integralmente") é do grupo 000, `200046`
("Operações com bens imóveis") é do 200, `400001` (transporte público) é do 400.
Sem exceção em toda a tabela. A regra, os valores da BWS e a procedência de cada
um ficaram escritos em `xsd_nacional/IBSCBS_CLASSIFICACAO.md`, porque a planilha
do Anexo VIII não está no repositório e a próxima pessoa não vai ter o pacote.

**Como eu tinha "conferido" isso antes e errei:** em 07/10 eu conferi o
`cClassTrib` contra o Anexo VIII — e ele está certo, é 200046 mesmo. O CST eu
não conferi contra nada: `000` entrou como "operação tributável", que é o que
`tribISSQN=1` significa no ISS, e os dois campos não têm nada a ver um com o
outro. Foi palpite com cara de verificação.

**O conserto, que é mais do que trocar o valor:** o CST passou a ser **derivado**
do `cClassTrib`, e o construtor da declaração **recusa** um par que não casa. Não
dá mais para digitar os dois e eles divergirem. Trocar `000` por `200` consertaria
esta emissão; derivar impede a próxima.

**E, de novo, o schema não pegava:** os dois campos são válidos sozinhos. Quem
confere a combinação é a plataforma, horas depois. É a mesma lição do E0370, e já
é a segunda vez no mesmo dia.

#### EL99 — e a frase do manual que não vale em Eusébio

> *EL99 — ID da DPS inválida. Chave informada para a DPS não existe no
> repositório municipal.*

Este veio da **prefeitura**, não do nacional, e no caminho da consulta: ela não
encontrou a declaração no repositório dela.

**O que aconteceu antes dele:** o dono usou o "liberar o número" (publicado
horas antes, naquele mesmo dia) para soltar o 3281, e emitiu de novo. A
identificação da declaração é construída **a partir do número da nota** — então
reusar o número reusou a identificação de uma declaração que a prefeitura já
tinha recebido e transmitido.

**A frase do manual que caiu:** *"a mesma declaração pode ser reenviada com a
correção, mantendo a mesma identificação"*. Está escrita no manual, foi a base
de duas decisões registradas aqui (a exceção do mesmo card na numeração e o
próprio "liberar o número"), e **em Eusébio ela não se sustentou**. Número já
enviado fica gasto.

**Os consertos:**

1. **A numeração conta TODAS as declarações já enviadas**, qualquer que seja o
   desfecho — recusada, aguardando ou concluída. A aba só recebe declaração
   **depois** de a prefeitura aceitar, então todo número que está lá já foi
   enviado. **A exceção do mesmo card saiu.**
2. **O "liberar o número" virou "encerrar"**: tira a declaração da lista e diz,
   em letras claras, que o número **não volta**. A versão que liberava durou
   horas e produziu este erro — fica registrado porque foi uma decisão minha,
   tomada por leitura do manual, contra a qual não havia evidência nenhuma.
3. **A tela de recusa parou de prometer o mesmo número.** Ela dizia "pode emitir
   de novo, inclusive com o mesmo número, que é o caminho previsto pela
   prefeitura". Era o manual falando, e estava errado.
4. **EL99 ganhou tradução, e ela é cuidadosa num ponto:** diz que o erro **não**
   significa que nada foi criado. Diferente de uma recusa de conteúdo, aqui o
   envio já tinha sido aceito — então a instrução é conferir no portal antes de
   emitir, e emitir com número novo.

**Buraco na sequência de números é normal**, e vale dizer para não assustar: nota
cancelada faz o mesmo. O número 3281 não existe e não vai existir.

#### O que foi conferido, e o que NÃO foi

**Conferido:** o CST derivado bate com a tabela oficial nos quatro grupos
observados; a declaração de obra sai com CST 200 e passa no schema; par digitado
que não casa derruba a montagem **com o valor certo na mensagem**; `cIndOp` tem
os seis dígitos que o schema exige (no Excel ele aparece com cinco, porque a
planilha come o zero da frente); `tpOper` e `tpEnteGov` não são enviados por
padrão e, quando preenchidos, saem na ordem do XSD; a numeração conta declaração
recusada; nem o mesmo card reusa número; EL99 e E0959 traduzidos. 14 casos novos.

**NÃO conferido:** se a plataforma aceita a declaração agora. Nada aqui faz rede,
e o ensaio nunca foi tentado até o fim (ver a correção na seção de estado). Se
vier recusa nova **com o CST casado**,
os suspeitos já estão identificados e prontos para ligar: `tpOper` (o candidato
é `1`, fornecimento com pagamento posterior) e `tpEnteGov` — este último o
sistema **não pode** deduzir, porque não dá para saber do CNPJ se o órgão é
federal, estadual ou municipal. Essa é pergunta para o dono.

### ✅ A PRIMEIRA NOTA DO PADRÃO NACIONAL SAIU — 08/10/2026

**Nota `2600000003283`**, obra IFSPSAOJOSE, medição 11, emitida em 08/10/2026,
chave `23042851200079526000109260000000328326100010793653`. O pós-emissão rodou
inteiro: planilha, Omie (documento 3255/2600000003283), slot A do card, XML e
DANFSe no Drive, recibo, links na Descrição e WhatsApp.

**O XML oficial dela foi lido campo por campo** (está no Drive, "NOTA FISCAL
2600000003283 … (XML Nacional).xml"), e ele é a prova de que os quatro consertos
do dia passaram pela plataforma:

| Conserto | No XML da nota |
|---|---|
| grupo de obra (E0370) | `<obra><cObra>900232558978</cObra></obra>` |
| CST derivado (E0959) | `<CST>200</CST><cClassTrib>200046</cClassTrib>` |
| imposto não retido omitido (E0699) | `tribFed` **sem `vRetCP`** — esta obra não retém INSS |
| total de tributos | `<totTrib><indTotTrib>0</indTotTrib></totTrib>` |

E o que já estava certo desde a migração continua certo: `tpRetISSQN=2` (ISS
retido pelo tomador), `vDedRed/vDR = 12.576,34` (dedução de material, com
`vBC = 12.576,35` — a base do ISS fechando), `tpRetPisCofins=3`.

**A plataforma calculou o IBS/CBS**, e o resultado confirma que o CST 200 era o
certo: `pRedAliqUF = 50,00`, `pRedAliqCBS = 50,00` — redução de 50%, que é
exatamente o que a classificação "Operações com bens imóveis" prevê. Valores
`vIBS = 11,93` e `vCBS = 107,36`.

#### ⚠️ E o número quase destruiu a numeração — pego a tempo

A nota voltou como **`2600000003283`**: ano (26) + o nosso sequencial (3283) em
11 dígitos. Tanto `nNFSe` como `nDFSe` vêm assim — **não existe, no XML, um
número municipal "3283" separado**. Esse é o número oficial, e é ele que foi para
a planilha, o Omie, os nomes dos arquivos e o cliente.

**O problema:** o próximo número sai do maior da planilha MAIS UM. Lido cru,
`2600000003283 + 1` faria a nota seguinte pedir **2.600.000.003.284** — e a
sequência da BWS nunca mais voltaria. A nota seguinte sairia com número absurdo,
e a de depois também.

**O conserto:** o número gravado é traduzido de volta ao sequencial antes de
qualquer conta (`worker.sequencial_da_nota`). Número de **13 dígitos** é nacional
e tem o ano na frente; qualquer outro é do modelo antigo e vale como está — um
sequencial da BWS tem 4 dígitos e não chega perto de 13. Então a planilha pode ter
os dois formatos convivendo, que é o estado real dela: milhares de linhas `3280` e
as novas `2600000003283`.

**Decisão, e o motivo:** o número **oficial** é o que fica na planilha, no Omie e
no documento do cliente. Guardar `3283` ali seria mais cômodo para a numeração,
mas faria o sistema divergir do que a prefeitura e o cliente veem — e conciliação
é exatamente o que essa coluna serve para fazer. A tradução resolve a numeração
sem mentir sobre o número.

**E o alarme falso saiu da tela.** Ela dizia *"Número devolvido (2600000003283) ≠
esperado (3283). Confira a numeração."* — em TODA nota nacional. Alarme que
sempre aparece deixa de ser lido. Agora a comparação é pelo sequencial, e quando
os dois batem a tela **explica o formato**: o número oficial é ano + sequencial, e
diz qual é o próximo.

**✅ CONFIRMADO pela segunda nota, no mesmo dia.** Isto estava escrito aqui como
dedução de um caso só: que o número longo é montado a partir do NOSSO sequencial.
A nota seguinte saiu como **`2600000003284`** — o sequencial pedido era 3284.
Então a regra é essa, e são dois casos: **o número da nota é ano (2 dígitos) + o
nosso sequencial (11 dígitos)**, e o que a BWS controla continua sendo o
sequencial.

Essa nota também é a prova de que o conserto funcionou em produção: sem ele, ela
teria pedido o número 2.600.000.003.284 em vez de 3284, e a sequência estaria
perdida.

#### O `vLiq` da nota não é o líquido que a BWS recebe

Descoberto no mesmo XML, e vale para a tela "Só a linha da planilha": o
`vLiq` do modelo nacional **não desconta PIS nem COFINS**. Nesta nota ele veio
`24.222,04`, e o que a BWS recebe de fato é **23.303,97** — PIS (163,49) e COFINS
(754,58) foram retidos.

A coluna O da planilha é "valor a ser recebido", ou seja valor menos **todas** as
retenções. A tela publicada de manhã lia o `vLiq` — e teria posto R$ 918,07 a
mais nessa coluna, num campo que o dono usa para conferir recebimento. Agora o
líquido é **calculado** a partir das retenções presentes no XML (imposto não
retido não aparece lá, pela regra do E0699, então presença quer dizer retenção).
O número confere com o do motor fiscal, centavo a centavo.

#### O que foi conferido

A nota real, campo por campo, contra o XML assinado que a prefeitura devolveu —
é a primeira vez nesta migração que a conferência não é contra um dublê. Mais: 10
casos novos, entre eles o que impede o estrago da numeração (planilha com os dois
formatos devolvendo 3284) e o líquido da nota 3283 fechando em 23.303,97. Suíte
inteira: 5.287 passando.

**O que NÃO foi conferido:** as duas notas saíram da MESMA obra e tributação
(PIS, COFINS, IR e CSLL retidos, ISS retido, sem INSS). As outras combinações da
BWS seguem provadas só contra o schema — e foi justamente uma combinação
diferente (esta, sem INSS) que revelou o E0699.

### E0699, e a varredura que devia ter existido desde a migração — 08/10/2026

> *E0699 — O valor do tributo CP deve ser maior que zero e menor que o valor do
> serviço informado na DPS.*

**CP é a contribuição previdenciária — o INSS.** A declaração mandava o campo
dele com **0,00**, numa obra cuja tributação não retém INSS. O campo é
**opcional** no layout, e a plataforma recusa valor zero: zero declara uma
retenção DE valor zero, que é diferente de não haver retenção.

**E esta é a parte feia: a regra já estava escrita neste arquivo.** A decisão
"imposto sem retenção não aparece na nota" está registrada aqui desde
21/09/2026, descrevendo o comportamento do modelo antigo. O grupo de PIS/COFINS
da declaração nova a segue — tem até comentário no código dizendo que mandar
"0,00" é diferente de não mandar. **Os três campos de retenção federal (INSS, IR
e CSLL) não seguiam**, e só eles.

**Pior: havia um teste afirmando o contrário.** O
`test_imposto_nao_retido_vai_zerado_e_nao_omitido`, escrito por mim em 07/10,
dizia que "ausência e zero são lidas igual aqui, mas zero é explícito". A
primeira metade é falsa, e o teste trancava o defeito no lugar. Ele foi
invertido, com o motivo escrito dentro.

#### O conserto, e um segundo defeito que ele revelou

1. **Os três campos federais só vão quando foram de fato retidos**, e quando
   nenhum foi, o grupo `tribFed` inteiro não sai (grupo vazio é válido no schema
   e não diz nada).
2. **A outra metade da regra do E0699 virou trava:** retenção maior ou igual ao
   valor do serviço derruba a montagem, com o motivo e os dois números na
   mensagem. Isso é erro de dado, e falhar antes de enviar é mais barato.
3. **O total aproximado de tributos estava errado também, e ainda não tinha dado
   erro.** O layout dá uma escolha de quatro para `totTrib`, e uma delas existe
   exatamente para quem não informa valor estimado: **`indTotTrib=0`**.
   Mandávamos a outra, `vTotTrib`, com os três valores em **0,00** — declarando
   que o total aproximado dos tributos da nota é zero, o que é falso. Mesmo
   defeito, mesma família, encontrado antes de custar uma emissão.

#### A varredura — o que eu devia ter feito em vez de consertar campo por campo

O mesmo defeito apareceu **três vezes em formas diferentes**: PIS/COFINS (pego
na migração), os três federais (E0699) e o total de tributos. A causa comum é
simples de dizer: **a plataforma trata "zero" e "ausente" como coisas
diferentes, e o schema não ajuda** — campo opcional com zero é arquivo válido.

Então, em vez de confiar em lembrar da regra campo por campo, passou a existir um
teste que **varre a declaração inteira contra o XSD**: todo elemento que o layout
permite omitir e que está indo com valor zero é acusado. A exceção são os
**indicadores** (`indFinal`, `indDest`, `indTotTrib`, `finNFSe`, `regEspTrib`),
onde o zero é um **significado** — "não é consumidor final" — e não um valor.
Roda nas quatro tributações que a BWS usa.

Esse teste é o que teria pego o E0699 em 07/10, e teria pego o total de tributos
junto. A lição, para a próxima vez que um layout novo entrar: **a pergunta não é
"este campo está certo?", é "que classe de erro este campo pertence, e como eu
varro a classe inteira?"**.

#### O que foi conferido, e o que NÃO foi

**Conferido:** imposto não retido é omitido e não vai zerado; nota sem retenção
federal nenhuma não leva o grupo, e continua passando no schema; retenção maior
que o serviço derruba a montagem; o total de tributos sai como "não informado" e
a outra opção continua disponível se um dia quiserem informar; a varredura não
acusa nada nas quatro tributações; E0699 traduzido (começando por dizer que CP é
o INSS, que o texto cru não diz). 9 casos novos, e um teste antigo invertido.
Suíte inteira: 5.277 passando.

**NÃO conferido:** se a plataforma aceita a declaração agora. É a quarta correção
do dia e nenhuma delas pôde ser ensaiada, porque o ambiente de teste do município
nunca foi tentado até o fim — ver a correção na seção de estado, que é
justamente sobre isso. Os suspeitos seguintes continuam os de antes —
`tpOper` e `tpEnteGov`, prontos e desligados.

### "Só a linha da planilha": uma nota certa com a planilha faltando — 07/10/2026

**O que ele trouxe:**

> *"Tenho uma nota emitida antes que não entrou correta na planilha. Foi emitido
> tudo certo. É só planilha. Como faço pra inserir ela na planilha?"*

**Por que nenhuma das telas que existiam servia.** Tanto "Recuperar entrega"
(modo completo) como "Nota emitida no portal" rodam a **conclusão inteira**.
Usar qualquer das duas para resolver só a planilha faria, de quebra, três
estragos: preencheria um **segundo slot** de nota no card (o card tem cinco, A–E,
e o primeiro vazio é o que o sistema usa), mexeria no **Omie** outra vez, e
mandaria o **WhatsApp** ao cliente de novo. Trocar um problema por três.

**O que foi feito:** a tela `/emissao/planilha`, "Só a linha da planilha". Ela
grava a linha A–P da "Notas BWS" e **não faz nada além disso** — não emite, não
toca no Omie, não preenche slot, não sobe arquivo, não avisa ninguém. A trava de
duplicidade que já existia (o número na coluna F) continua valendo, então repetir
não duplica.

**A decisão que de fato importa: os valores vêm do XML, não do card.** E isso não
é preferência — é a única fonte que ainda existe. Ao concluir uma emissão, o
sistema **limpa doze campos de entrada do card** (`CAMPOS_LIMPAR`, em
`pipefy_update.py`), e entre eles estão justamente os que mandam na conta: valor
parcial, tipo de medição, as alíquotas de IR/INSS/ISS e o banco. Recalcular a
nota pelo card dias depois da emissão produziria números **diferentes dos que
foram realmente emitidos** — e eles iriam para a planilha com cara de certos. Do
card ficam só o **código da obra** e o **número da medição**, que sobrevivem à
limpeza e são o que a linha precisa dele.

**A leitura aceita os dois modelos, decidindo pelo conteúdo do arquivo.** No
nacional os totais estão em `infNFSe/valores` (`vISSQN`, `vLiq`) e o valor do
serviço e os federais retidos moram dentro da **declaração embutida na nota**
(`vServ`, `vRetCP`, `vRetIRRF`, `vPis`, `vCofins`). No modelo antigo é tudo
`ValoresNfse`. Quem precisa consertar uma linha não tem como saber em que modelo
a nota saiu, e há notas dos dois no Drive — então a tela não pergunta.

**Uma sutileza que quase passou, e ela mudaria número na planilha.** A coluna P
("Valor Líquido Tributado") desconta os federais **cheios**, retidos ou não — é
como a planilha sempre foi. O XML, porém, só traz o que foi **retido**: PIS,
COFINS e IR simplesmente não aparecem quando não houve retenção. Ler o XML cru
deixaria a coluna P alta nessas notas. A regra ficou: **havendo valor no XML vale
o do XML** (é o que a nota destacou, inclusive com alíquota diferenciada), **não
havendo, aplica-se a alíquota padrão sobre o total** — que é exatamente o número
que o motor fiscal produziria nos dois casos.

**O limite disso, dito claro:** nota emitida com alíquota diferenciada **e sem**
retenção daquele tributo sai com a alíquota padrão na coluna P. O campo de
alíquota do card é um dos doze que a conclusão limpa, então esse dado não existe
mais em lugar nenhum — não é perda nova, é perda antiga que só agora apareceu.
Afeta só a coluna P, que é informativa.

**A página de resultado é própria.** A de emissão diz "NFS-e emitida" e "os
documentos sobem no Drive" — aqui nada disso aconteceu, e reusá-la faria a tela
mentir sobre o que fez.

**Conferido:** 16 casos novos em `tests/test_emissaonf_linha_planilha.py`, e
entre eles os dois que importam — que a linha sai com os valores do **XML** mesmo
quando o card traz um valor absurdo, e que o pós-emissão e o Omie **não são
chamados**. Mais: os dois modelos de XML lidos, o federal sem retenção caindo na
alíquota cheia, a trava de duplicidade, o XML da declaração (sem número de nota)
sendo recusado em vez de gravar linha sem número, e a linha mantendo as 16
colunas — tamanho diferente desalinharia as fórmulas de Q em diante.

**NÃO conferido:** a gravação na planilha de verdade. Nenhum teste faz rede.

### A limpeza do que o modelo antigo deixou

Saíram do `web.py` o preparo do certificado para o envelope SOAP, a busca
nacional em segundo plano (60s/180s/300s depois de emitir) e os imports do
emissor antigo. Nada disso tinha mais caminho até ele.

Ficou de propósito: as telas de busca nacional **manual**, o job por NSU e o
leitor do modelo ABRASF. Há notas emitidas antes de 07/10/2026 que ainda
precisam ser reencontradas, ter PDF regerado e ser recuperadas.

### O que foi conferido, e o que NÃO foi

Dito sem rodeio, porque a decisão de emitir é do dono:

**Conferido:** a declaração passa no schema oficial nas quatro formas de
tributação que a BWS usa; a dedução de material fecha com a base do ISS; o ISS
retido sai como retido; as oito combinações de retenção federal, inclusive a
CSLL sozinha; que nenhuma resposta além de 404/405 faz o envio ser repetido; a identificação
da declaração continua igual à que o job antigo monta (senão as notas antigas se
perderiam); o PDF municipal, a DANFSe e o valor do recibo saem da resposta nova;
a tela de recuperação reconhece os dois formatos. São 46 casos, e a suíte inteira
do repositório (4.983) passa.

**NÃO conferido:** a conversa com a prefeitura de verdade. **Nenhum teste faz
rede.** Não se sabe se o token está configurado no serviço, se o endereço de
produção é o que o portal diz, se a prefeitura aceita a declaração como ela está,
nem qual número ela devolve. A primeira emissão de verdade é a primeira prova — e
é por isso que o ensaio em homologação existe e deve ser o primeiro passo.

---

## Incidentes

### A obra que existia e o sistema dizia não existir — 21/09/2026

**O que aparecia na tela:** `Erro ao carregar o card 1447316614: KeyError: "Obra
'AREFORTAL09' não encontrada na C. Diários."` — e a obra estava lá.

**O que era:** a C. Diários identifica a mesma obra por **dois** códigos, o
primário (coluna "Código Primário") e o secundário (primeira coluna, A). O
carregador só indexava pelo primário. Card que trouxesse o secundário não achava
a obra, e a emissão parava antes de começar.

**Conserto:** o índice passou a ter os dois códigos, e a busca tenta o primário
primeiro; só cai no secundário se não achar. Só levanta erro depois de tentar os
dois — e a mensagem agora diz que tentou.

**A decisão que precisou ser tomada junto:** o que fazer quando o código
secundário de uma linha for igual ao primário de outra. **O primário ganha
sempre.** Um código secundário encobrindo o primário de outra obra emitiria a
nota com a tributação errada — que é exatamente o erro que não se desfaz. A
busca também passou a ignorar espaço e maiúscula, porque o código vem digitado
no card.

**Este incidente trouxe os primeiros testes automatizados da área**
(`tests/test_emissaonf_codigo_obra.py`, seis casos): os dois códigos achando a
obra, o primário não sendo encoberto, a linha que só tem secundário, e o erro
continuando a acontecer para código que não existe mesmo. É pouco perto do que
falta, mas é o começo de uma rede que aqui não existia.

**Não foi verificado com dado real:** a correção roda sobre linhas de planilha
montadas no teste. Ninguém abriu a C. Diários nesta sessão — vale abrir o card
1447316614 na tela e conferir que a obra carrega, com a tributação certa, antes
de emitir.

### O `ValorDeducoes` que nunca foi enviado — o mais grave

**O que acontecia:** o sistema calculava a dedução de material corretamente,
mostrava o valor certo no espelho e no PDF arquivado no Drive — mas **enviava
`ValorDeducoes` como 0,00 no XML**. A prefeitura calcula a base do ISS como
(valor dos serviços − deduções); recebendo zero, ela aplicava a alíquota sobre o
valor **cheio**.

**O tamanho do erro:** numa nota `80/20`, o ISS saiu **R$ 1.409,37** na
prefeitura contra **R$ 1.127,50** no nosso cálculo.

**O que isso significa:** toda nota emitida **com dedução de ISS** (`60/40`,
`80/20` e semelhantes) durante o período do defeito está com **ISS a maior na
prefeitura**. O PDF guardado no Drive mostrava o valor certo — que nunca chegou
lá. Notas `100/0` não foram afetadas, porque nelas a dedução é zero mesmo.

**Correção:** o XML passou a levar `ValorDeducoes = valor total − base do ISS`.

**⚠️ Correção ao relato do dono, conferida no código:** o relato dizia que o
conserto "precisa subir antes de qualquer emissão nova". **Ele já está no ar.**
A linha entrou na `main` em **11/09/2026** (commit `f2d93c8`) e continua lá na
`main` de hoje — e juntar na `main` publica no Render na hora. Conferido no
`montar_emissao.py` da `main`, não só no ramo. Ou seja: **emitir agora está
seguro quanto a isso.** O que continua pendente é o **passivo**: as notas
emitidas antes daquela data.

> Não deu para datar o **início** do defeito: o histórico de Git disponível
> nesta sessão é raso (começa em 04/09/2026) e esta pasta aparece nele uma vez
> só. Para levantar o passivo, o caminho confiável é a planilha — filtrar por
> tributação com dedução de ISS e emitir até 11/09/2026 —, não o Git.

### O Omie recusava a segunda escrita seguida

Na substituição, duas chamadas de alteração seguidas no mesmo título eram
bloqueadas ("já sendo processada", "consumo redundante, aguarde 55s"). O Omie
trava o registro por alguns segundos depois de cada escrita.

**Dois consertos, nesta ordem de importância:** o principal foi juntar remoção do
número antigo e inclusão do novo numa **única chamada**; o secundário foi
esperar e tentar de novo quando a mensagem de trava aparece.

### A discriminação vinha "corrida" nos PDFs

O texto do sistema nacional colapsa as quebras de linha, e a discriminação
chegava sem parágrafo na municipal regenerada e na DANFSe. Foi criado um
tratamento que **reinsere as quebras** nos pontos conhecidos do texto, usado nos
dois geradores.

Junto disso, a DANFSe nacional passou a ter **altura variável** no campo da
descrição do serviço — antes era fixa, e o texto longo escrevia por cima da
seção seguinte.

### O sufixo "R" da medição de reajuste não saía

Quando o "Tipo de Documento" do card é de reajuste, o número da medição deve ir
para a planilha com um "R" no fim (`7R`). Não ia, porque o campo simplesmente
**não estava sendo lido** do card. Passou a ser lido — e, de quebra, a busca de
campos no Pipefy ficou tolerante a acento, maiúscula e espaço, que é de onde
esse tipo de erro costuma vir.

### Nota com período invertido

A emissão passou a ser **barrada** quando o término da medição é anterior ao
início.

### As linhas embaralhadas da "Notas BWS"

**O que se temeu:** que a aplicação estivesse gravando fora de ordem ou por cima
de linha existente.

**O que era:** a aplicação só **acrescenta ao fim**, nunca ordena. A mistura
vinha do **Apps Script da própria planilha** — um gatilho `onChange` rodando
concorrente e reentrante, com dois `sort()` se atropelando.

**Conserto:** um `LockService` para serializar o gatilho. **A integridade das
notas nunca foi afetada** — prefeitura, Drive e Omie estavam certos; o que
embaralhava era a ordem na tela.

> Esse conserto vive **na planilha, não neste repositório**. Quem procurar o
> código dele aqui não vai achar. Se o problema voltar, o caminho combinado é
> tirar a ordenação do `onChange` e pôr num gatilho por tempo.

### O token da prefeitura ficou no histórico do Git

Registrado no `CONTEXTO.md` §9 e repetido aqui porque é precedente desta área:
um token da prefeitura foi commitado dentro do `consultar_status.py` (commit
`fa985ab`). O código passou a lê-lo de `EL_NFSE_TOKEN`, **mas o valor antigo
continua no histórico do repositório** até ser trocado na origem.

**A regra que ficou:** o certificado e a senha **nunca entram no chat**, e
nenhum segredo entra no código. Diagnóstico se faz pela tela `/emissao/diag`,
que diz o que está errado sem mostrar o que é.

---

## Ferramentas que nasceram desses incidentes

Não são funcionalidade: são conserto. Vale saber que existem antes de escrever
script novo.

- **`/emissao/recuperar`** — refaz o pós-emissão de uma nota já emitida, colando
  o XML. Sem "completo", refaz só a entrega (Drive, links, Descrição). Com
  "completo", refaz tudo — e tem trava anti-duplicação. É o que fecha o ciclo
  de uma substituição feita pelo portal.
- **`/emissao/regerar`** — regrava os PDFs de notas já concluídas com o layout
  atual, subindo com o mesmo nome: mesmo link, nada duplicado na planilha nem no
  card.
- **`/emissao/nacional_chave` e `/emissao/nacional_xml`** — fecham a parte
  nacional de uma nota pela chave ou pelo XML, quando ela ainda não apareceu na
  distribuição.
- **Os ~290 PDFs antigos** foram baixados do portal por script (consulta →
  identificador interno → exportação do relatório), salvos como `NF {número}.pdf`.
  Estão à espera do registro na planilha — item 4 das pendências.

---

## Regras que não se discutem

1. **Publicar na `main` espera o "pode" do dono.** Juntar publica no Render na
   hora.
2. **O certificado e a senha nunca entram no chat.** Nem o arquivo, nem o
   base64, nem a senha. Diagnóstico é pela tela.
3. **Nota emitida não se apaga.** Antes de mexer em qualquer coisa do caminho de
   emissão, lembrar que o teste é uma nota fiscal de verdade. Não existe
   ambiente de homologação em uso nesta área.
4. **Mudança no caminho da emissão exige olhar o espelho antes de clicar.** Não
   há teste automatizado nenhum aqui para segurar erro.
5. **Antes de publicar, perguntar se há carga do painel ou sincronização do
   Análise de SPs em andamento** — publicar reinicia o serviço e mata trabalho
   longo. Vale para todas as áreas, e já aconteceu.

---

## O que ficou de fora, e por quê

- **Cancelamento fiscal na prefeitura.** O módulo de substituição diz, em letras
  grandes, que **não cancela** a nota no município. Cancelar continua sendo
  passo manual no portal.
- **Município que não seja Eusébio/CE.** O código do município, o endereço do
  webservice, o código da DPS e o brasão estão fixos no código. Emitir para
  outra prefeitura é outro trabalho, não um parâmetro.
- **Teste automatizado.** Não há nenhum, e escrever um que exercite o caminho de
  emissão esbarra em não existir ambiente de homologação em uso.
- **Login de verdade.** A porta é o token na URL. E, diferente do resto do
  repositório, se o token não estiver configurado a tela **libera** em vez de
  recusar. Fica registrado como fraqueza conhecida, não como decisão defendida.
