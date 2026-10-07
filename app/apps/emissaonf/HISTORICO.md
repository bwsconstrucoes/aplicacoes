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

### ⚠️ Estado em 07/10/2026 — leia isto primeiro

A prefeitura **desligou o formato de nota** que o sistema usava, e a emissão
ficou parada. A migração para o formato novo (DPS, padrão nacional) foi feita no
mesmo dia e está na seção própria mais abaixo.

A migração **foi publicada em 07/10/2026**, com o "pode" do dono no mesmo dia.

**O que falta é a primeira emissão de verdade.** Nenhum teste aqui conversa com
a prefeitura: tudo o que dava para conferir sem emitir foi conferido (a
declaração passa no schema oficial, os documentos saem certos da resposta nova),
mas a primeira nota real é a primeira prova.

**A sequência combinada com ele, nesta ordem:**

1. abrir **`/emissao/diag`** e ver se o token da prefeitura está no serviço — a
   primeira linha responde isso. Sem ele nenhuma nota sai, nem em ensaio;
2. **ensaiar uma nota em homologação** (caixa "Ensaiar primeiro" na tela de
   emissão) e conferir o resultado;
3. só então **emitir de verdade** e conferir o número, os PDFs, a planilha, o
   Omie e o card.

**O que acontece se algum desses passos falhar está escrito abaixo, na seção da
migração.** O que NÃO se sabe, e só a primeira emissão responde: se a prefeitura
aceita a declaração exatamente como ela está, e qual número ela devolve.

### O histórico até aqui

O sistema **emite em produção há meses** e faz o ciclo inteiro sozinho: lê a
medição no Pipefy, calcula as retenções pela tributação da obra, assina com o
certificado A1, envia à prefeitura de Eusébio, e depois grava a planilha, ajusta
o título no Omie, preenche o card, arquiva os PDFs no Drive e avisa no WhatsApp.
A nota nacional fecha sozinha em minutos, pela SEFIN.

Não é um projeto em construção: é uma ferramenta em uso. O que existe de
trabalho pendente é **conserto e faxina**, não funcionalidade nova.

### O que está pendente AGORA

**O que está na frente de tudo (07/10/2026):**

1. **Ensaiar uma nota em homologação** e conferir o resultado. É o primeiro
   passo depois de publicar a migração — e o único jeito de ver a nota antes de
   emitir de verdade.
2. **Emitir a primeira nota de verdade** no formato novo, e conferir: o número
   que a prefeitura devolve, o PDF municipal, a DANFSe, a linha da planilha, o
   título no Omie e o card.
3. **Conferir se o token da prefeitura (`EL_NFSE_TOKEN`) está no serviço.**
   Sem ele nenhuma nota sai. `/emissao/diag` responde isso.
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

### O que foi conferido, e o que NÃO foi

Dito sem rodeio, porque a decisão de emitir é do dono:

**Conferido:** a declaração passa no schema oficial nas quatro formas de
tributação que a BWS usa; a dedução de material fecha com a base do ISS; o ISS
retido sai como retido; as oito combinações de retenção federal; a identificação
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
