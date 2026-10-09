# Faturamento: a base consolidada e a tela

Pedido do dono em **09/10/2026**, por áudio. O que ele disse, no essencial:

> *"Essa atualização de planilha eu quero eliminar. Não quero mais fazer a gestão
> dessas notas emitidas na aba Notas BWS. (…) Essa planilha é um lixo, é uma
> bagunça. (…) Numa nova tela no Análise de SPs, que a gente pode chamar de
> **Faturamento**, eu quero fazer o controle de notas — ver faturamento, fazer o
> download da nota, uma parte gráfica de evolução, poder fazer toda essa gestão.
> (…) Consolidar todos os dados que estão na Notas BWS, Notas BWS Links e
> Controle Nacional numa coisa só."*

---

## 1. O que o emissor usa HOJE de cada planilha

Resposta direta à pergunta dele. São **três** planilhas e **sete** abas.

### Planilha "Controle de Impostos e Emissão de Nota" (`1NOEzey3…PpEbU`)

| Aba | O emissor LÊ | O emissor ESCREVE |
|---|---|---|
| **Notas BWS** | só a **coluna F** (Nº Nota), para saber o próximo número e para a trava anti-duplicação | a linha **A:P** de cada nota nova; e a **coluna L** (Observação) na substituição |
| **Notas BWS Links** | a **coluna B** (número), para achar a linha da nota | a linha **A:H**; e depois **F, G, H** (links municipal, nacional, recibo) |
| **Controle Nacional** | a **coluna A** (número) e a aba inteira no job | a linha **A:O**, o status, os links e a chave; e **P1** = último NSU lido |
| **Declaracoes** | a aba inteira (declarações em aberto e números já enviados) | a linha de cada declaração enviada, e o status dela |

**Nada mais.** Das 65+ colunas da "Notas BWS", o emissor toca **16** (A:P) e lê
**uma** (F). Todo o resto — as três repetições de PIS/COFINS/IR/CSLL/INSS/ISS, os
`CPRB`, os `REGIME ESPECIAL`, as colunas `Coluna1`, os hiperlinks de AV a AY — é
fórmula da planilha ou obra dos scripts do Apps Script. O emissor **nunca gravou
tributo nenhum**, e é por isso que o dono vê os valores "não conforme foi gerado
na nota fiscal": eles são recalculados por fórmula, não registrados.

### Planilha "Bases de Dados Pipefy" (`1C7MWQ…xPsBk`)

| Aba | Para quê |
|---|---|
| **Centro de Custo** (a "C. Diários") | a obra: código primário e secundário, município, **tributação**, **alíquota do ISS**, CNO, cliente, CNPJ, contrato, código Omie, conta de pagamento — e, a partir de agora, **Empresa** e **SCP** |
| **Protocolos** | o **card do Pipefy** e o **código de integração do Omie**, por `OBRA-MEDIÇÃO` |

### Planilha de credenciais (`1D4aVC…B9i-U`)

| Aba | Para quê |
|---|---|
| **Credenciais** | tokens do Pipefy, do Omie, da Z-API, da prefeitura |
| **Destinatarios WhatsApp** | para quem o recibo é enviado |

---

## 2. A base nova: aba `Base Faturamento`

Mesma planilha das notas, aba nova, **64 colunas**, uma linha por nota.
O layout e a procedência de cada campo estão em `base_faturamento.py` — o
cabeçalho é a única fonte da verdade sobre a ordem.

**Por que uma aba, e não banco:** o ERP ainda não é o dono dessa informação, e o
dono foi explícito — *"porque uma base na planilha? Porque pode ser que, como a
gente não está no ERP ainda, aí a gente mantém a planilha"*. Quando virar ERP, a
aba vira tabela: o layout já está nomeado campo por campo para isso.

### ⚠️ O que NÃO entra: nada que venha da C. Diários

Correção do dono em **09/10/2026**: *"informação que vem da C. Diários não precisa
entrar na base, a gente vai cruzar"*. Saíram da base, e a tela os busca pelo
**código da obra**:

`contrato`, `municipio_obra`, `centro_custo`, `tributacao`, `obra_codigo_primario`,
`empresa`, `empresa_cnpj`, `scp`, `scp_cnpj`.

O carregador da C. Diários **continua** lendo `Empresa`, `CNPJ Empresa`, `SCP` e
`CNPJ SCP` (pelo nome do cabeçalho) — é a tela que cruza. Da obra, a base guarda
só a **chave**: `obra_codigo`.

**A fronteira é esta, e vale escrever:** atributo da OBRA pode mudar amanhã e tem
dono (a C. Diários); **fato da NOTA** é congelado no dia da emissão e não tem
outra fonte. Por isso a `aliquota_iss` **fica** — é a que a nota aplicou, não a
que está cadastrada hoje.

*Risco aceito, e é dele:* se uma obra trocar de empresa ou de tributação, a tela
vai mostrar a nova para as notas antigas. Para faturamento é o que ele quer; se
um dia precisar do retrato histórico, o campo volta para a base.

### Os campos que não existiam em lugar nenhum

1. **`medicao_periodo_ini` / `medicao_periodo_fim`** — o período da medição. Está
   no card do Pipefy e era descartado na emissão.
2. **`discriminacao`** — o corpo da nota, o texto que o cliente lê. Era usado e
   jogado fora.
3. **`ibs` / `cbs`** — os tributos da reforma. Quem calcula é a plataforma
   nacional, e o resultado volta no XML da nota (com a redução de 50% da
   construção civil). Pedido dele: *"pode ser que isso aí seja necessário"*.
   No modelo antigo não existem.

Para as **notas antigas** esses campos ficam **vazios** — não existem em fonte
nenhuma, e inventá-los seria pior que deixar em branco. Para as **novas**, o
emissor grava. Era o pedido: *"não vamos ter a completude dos dados, mas para
frente a gente passa a ter"*.

### ⚠️ Os tributos vêm de BB em diante, e SÓ

Correção do dono no mesmo dia: *"não existe aquilo dali, aquilo são repetições, é
outra metodologia que eu utilizava, dali é lixo. Eu comentei que eles são da
coluna BB em diante só."*

A primeira versão lia o bloco **T:Y** como se fossem os tributos da nota. A
planilha tem o mesmo conjunto PIS/COFINS/IR/CSLL/INSS/ISS **três vezes** (T:AB,
AC:AK, AL:AT), mais CPRB e "REGIME ESPECIAL" — metodologia abandonada. Ler dali
encheria a base de **números plausíveis e errados**, que é o pior resultado
possível: ninguém desconfia de um número com cara de certo.

**De P a BA não se lê nada.** O que se lê é **E:O** (os dados da nota e o
recebimento) e **BB:BM** (os tributos, em pares valor/retém).

### E BB:BM é o tributo EQUALIZADO — o lado da NOTA

Esclarecimento dele no mesmo dia, e é o que define para onde esse bloco vai:

> *"A parte de tributos Omie, aquilo dali eu criei exatamente para equalizar. Já
> está tudo equalizado ali. E o que não tiver, talvez tenha alguns que estão em
> branco, mas são poucos, são as mais recentes."*

Então BB:BM **não** é "o que o Omie tem por acaso": é o valor **acordado** entre a
nota e o título, conferido por ele ao longo do tempo. Para as notas antigas é o
**único registro que existe** dos tributos delas — o emissor nunca gravou nenhum.
Por isso entra como o **lado da NOTA** da base.

As colunas `omie_*` ficam para o que a consulta ao Omie devolver **agora**. É
comparando as duas que se vê **se o título saiu do lugar depois de equalizado** —
que é a utilidade real da tela do Omie para o acervo antigo.

O "retido ou não" entra por um motivo só, que é o que ele pediu: **compatibilizar
com o Omie**. O valor fica (é informação) e a marca diz se é descontado; só o que
foi retido entra na soma que vai para o título.

### A nota NOVA grava conforme o EMITIDO

Pedido dele no mesmo dia: *"as novas notas já têm a informação dos tributos
emitidos, então vamos gravar conforme. Se necessário, a posteriori eu equalizo."*

E isso não é o mesmo que gravar o que o motor fiscal calculou. O motor calcula os
cinco federais **sempre** (era assim que a coluna P da planilha antiga era feita),
mas a nota só **declara** o que foi retido — imposto não retido nem aparece no
XML, que é a regra do erro **E0699**. Gravar o valor calculado de um imposto não
retido afirmaria uma retenção que não houve.

**Os três estados de um campo de tributo, e eles querem dizer coisas
diferentes:**

| No campo | Quer dizer |
|---|---|
| **vazio** | não se sabe — nota antiga que ele ainda não equalizou |
| **0,00** com retém **N** | a nota **não** reteve esse tributo |
| valor com retém **S** | a nota reteve |

O **ISS** é a exceção: ele é declarado de qualquer jeito, porque a prefeitura o
calcula e ele sai na nota — o que muda é quem recolhe.

**A trava que sobra, e ela protege as poucas em branco:** as notas mais recentes,
que ele ainda não equalizou, chegam **sem tributo**. Para essas a soma daria zero,
a equalização veria divergência em tudo e **zeraria as retenções no Omie**. Então
título sem tributo registrado **nunca é equalizado**, mesmo com a confirmação
marcada — e a tela diz "sem tributo na nota", que é o aviso de que falta equalizar
aquela.

### Nota declarada × Omie: dois campos, não um

Cada tributo aparece **duas vezes**: `pis` (o que a nota declarou) e `omie_pis`
(o que está no título do Omie). Guardar um só esconderia exatamente o que o dono
confere à mão hoje. Daí saem dois campos calculados:

- **`divergencia_tributos`** — a nota e o Omie batem?
- **`divergencia_recebimento`** — o que entrou em conta bate com o líquido
  previsto? É a pergunta que fecha o ciclo: quando não bate, o tributo no Omie
  precisa de ajuste para a baixa sair correta.

Os dois ficam **vazios** quando não há com o que comparar. "Não divergente" numa
nota que ninguém recebeu seria uma conferência que não aconteceu.

### Como a base é preenchida

- **Nota nova:** o emissor grava no passo `[11]` do `concluir.py`, **além** da
  "Notas BWS". Trabalho duplicado de propósito, para conferir uma contra a outra
  antes de a antiga sair de cena.
- **Notas antigas:** a tela `/emissao/faturamento`, **em lotes**. Cada rodada
  processa um lote e diz quantas faltam. Repetir não duplica (a chave é o número
  da nota); rodar de novo continua de onde parou.
  **Em lotes porque o serviço atende 4 pedidos por vez** — ler e escrever 3.300
  notas de uma vez prenderia uma thread por minutos, e foi assim que o monorepo
  caiu em 07/10/2026.

---

## 3. Onde cada parte do trabalho mora

| Parte | Área | Por quê |
|---|---|---|
| a aba `Base Faturamento`, o layout, a consolidação | **emissaonf** (feito) | é o emissor que produz o dado |
| o emissor gravando na base | **emissaonf** (feito) | mesmo motivo |
| a **tela de Faturamento** (listagem, gráficos, download, filtros) | **Análise de SPs** (feito em 09/10/2026 — `analisesps/faturamento.py`, migração 053, leva 207 do histórico de lá) | é lá que o dono quer a tela, e é lá que vive a navegação |
| as 3 operações no OMIE (consultar, equalizar, atualizar tributos) | **emissaonf** (feito) | decisão dele em 09/10; tela `/emissao/omie` |

A tela é trabalho do **chat do Análise de SPs**, e o que ela precisa saber está
escrito aqui para atravessar: a aba, o nome de cada campo, e o que está vazio e
por quê.

---

## 3-B. As três operações no Omie — como ficaram

Tela **`/emissao/omie`**. Vale a pena guardar as três regras que a governam,
porque errar qualquer uma mexe em dinheiro:

**1. A nota manda, o título obedece.** A nota fiscal não se desfaz; o título do
Omie é registro interno. Então o que pode ir para o Omie é a **soma dos tributos
das notas válidas** daquele título — nunca o contrário.

**2. Um título cobre VÁRIAS notas.** Para mostrar quanto do título cabe a cada
nota, o valor é **rateado pelo valor bruto da nota, fechando ao centavo** — o
residual vai para a de maior valor. É a regra do Apps Script
(`ratearProporcional_`), portada e testada. Sem o fechamento exato, a conferência
acusaria um centavo de diferença em toda nota, e alarme assim deixa de ser lido.

**3. Só o que foi RETIDO entra na soma.** Somar o valor de um imposto não retido
infla a retenção do título e a baixa sai errada — é o problema que ele descreve
quando o líquido não fecha. Nota **cancelada ou substituída fica fora** de tudo.

**Conferir é leitura; equalizar escreve.** A tela confere por padrão e grava o
resultado na base. Para alterar o Omie é preciso **marcar a confirmação**, e a
rodada de leitura antes mostra, tributo por tributo, o que vai mudar: gravar em
sistema financeiro sem dizer o que vai mudar não se faz.

**O que a atualização NÃO toca:** o `numero_documento_fiscal` do título. O número
da nota ali é assunto da emissão (ela acumula `3001/3072`), e mandá-lo na
equalização sobrescreveria esse acúmulo por tabela.

---

## 4. O que o Apps Script faz, e o que vale salvar

O dono mandou o projeto (`Controle_de_Impostos_e_Emissão_de_Nota`) e avisou:
*"no script tem muito, mas muito lixo, acho que 80% é lixo"*. Lido inteiro, a
conta é parecida. **Não foi copiado nada para o repositório** — o que segue é o
inventário.

### Vale salvar (é regra de negócio de verdade)

| O quê | Onde | Por quê |
|---|---|---|
| **Rateio proporcional ao valor da nota, fechando ao centavo** | `OmieRateiroeConsulta.gs` → `ratearProporcional_` | um título do Omie cobre VÁRIAS notas; o residual do arredondamento vai para a(s) nota(s) de maior valor, para a soma bater exatamente com o título. É aritmética testável, e a base nova precisa dela |
| **Consulta do título e do valor recebido** | `consultarContaReceberOmie_`, `consultarValorRecebidoOmie_` | é a origem dos campos `omie_*` e `valor_recebido` |
| **Retry com espera dobrando no 429 do Omie** | `chamarOmieComRetry_` | o Omie limita chamadas; sem isso a consolidação para no meio |
| **Linha com "CANCELADA" fica fora de tudo** | em vários | virou o campo `status` da base |
| **`LockService` no `onChange`** | `Classificar.gs` | foi o conserto do embaralhamento da "Notas BWS" — a nota explica que o emissor só faz `append_row` e a ordenação é toda do script |

### Lixo ou obsoleto

`POR_EXTENSO`, `ConsolidaFolha`, os filtros de menu por conta e por período, os
dois arquivos "Sem título", `corrigirEOrdenarNotasBWS`, `OmieConsulta.gs` (vazio),
`FormularioTributos.html` e o relatório PIS/COFINS. Tudo isso é conveniência de
planilha, e some junto com a planilha.

### ⚠️ E um achado de segurança

O arquivo `OmieRateiroeConsulta.gs` traz a **chave e o segredo do Omie em texto
claro**, e também a URL do webhook do Make. Ou seja: quem abre o Apps Script da
planilha tem a credencial do Omie. Está registrado em `CONTEXTO.md` §9 — **os
nomes, nunca os valores**. A ação é do dono: trocar na origem.

No repositório isso nunca foi assim: o `omie.py` lê `OMIE_KEY` e `OMIE_SECRET` da
aba Credenciais, e os valores do script **não estão** no código nem no histórico
do git (conferido).

---

## 5. O que ficou em aberto, e precisa de decisão dele

1. ~~Quem passa a falar com o Omie?~~ **RESPONDIDO em 09/10/2026**, com estas
   palavras: *"os scripts, eles apenas para consulta, equalização e atualização
   da parte de tributos no Omie. Se as emissões estiverem todas corretas e
   gerando títulos corretos, a operação se limitará ao que eu disse e não mais a
   uma série de outras funções que foram criadas."* As três operações estão
   implementadas em Python (`omie.py` + `omie_conferencia.py` + a tela
   `/emissao/omie`), e a credencial vem da aba Credenciais — não mais de dentro
   do código. **Falta só ele desligar os menus do Apps Script e trocar a chave
   do Omie na origem** (§4).
2. **A coluna de SCP na C. Diários.** O código já a lê **pelo nome do
   cabeçalho** (`SCP`, `CNPJ SCP`, `Empresa`, `CNPJ Empresa`). Ele disse que vai
   criar e que seria "provavelmente a coluna AN". Enquanto o cabeçalho não
   existir, os campos ficam vazios — sem erro.
3. **A "Notas BWS" para de ser atualizada quando? — decisão DELE, e tem trava.**
   Ele reforçou em 09/10/2026, depois de a base nova ser publicada: *"por
   enquanto, a nota do BWS a gente vai continuar usando normal. Só depois que
   estiver consolidado essa nova etapa aí, a gente vai deixar de usar ela."*

   A gravação duplicada é o estado **desejado**, não resíduo de transição — e há
   teste exigindo que ela continue. Desligar é uma linha no `concluir.py`, mas
   **não se faz sem ele pedir**: a base nova precisa estar conferida contra a
   antiga primeiro.
4. **Deduplicar a "Notas BWS Links"** (~20 mil linhas) continua pendente desde
   setembro. A consolidação convive com isso: a PRIMEIRA ocorrência ganha,
   sempre, para a base não mudar de valor entre duas rodadas.
5. **Notas antigas sem card no Pipefy.** A aba Protocolos só cobre o que passou
   por lá; nota mais antiga fica sem `card_id`. Não é erro, é limite da fonte.
