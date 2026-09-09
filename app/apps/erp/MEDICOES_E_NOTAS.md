# Medições, contrato e emissão de nota — o lado do que a BWS RECEBE

> Ditado pelo dono em **09/09/2026**. Como nos outros dois documentos grandes
> (`NOTAS_FISCAIS.md` e `GESTAO_DOCUMENTOS.md`), a especificação vem antes do
> código: o desenho errado aqui custa meses, e a decisão é dele.
>
> ⚠️ Este documento trata do **que a BWS recebe** — medição de obra, nota que
> ela EMITE contra o cliente. Não confundir com `NOTAS_FISCAIS.md`, que trata
> do contrário: nota que o FORNECEDOR emite contra a BWS.

---

## 1. Como funciona hoje, e por que sai do Pipefy

Existe um processo no Pipefy chamado **"Protocolos e Medições"**. Cada card é
uma medição de uma obra. Abrindo o card, estão as informações da medição, e é
de lá que a nota é emitida — o card dispara a API do município, atualiza o
Omie, atualiza uma planilha e atualiza o próprio card.

Palavras do dono sobre por que isso existe: *"a gente tem isso porque dentro do
Omie não dá pra trabalhar isso"*. O Pipefy foi a saída para uma falta do Omie.

**No ERP, isso deixa de ser um processo à parte e vira o lado a RECEBER dos
títulos** — com uma ressalva que ele mesmo fez: *"pode haver outro algo a
receber que não seja medição"*. Então:

- **título a receber** é o gênero;
- **medição** é uma espécie dele, com campos próprios.

Nem toda coisa a receber é medição; toda medição é uma coisa a receber.

---

## 2. A MEDIÇÃO — o que ela precisa carregar

Boa parte já existe no ERP (`Titulo` tem `contrato_id`, `numero_medicao`,
`periodo_inicio`, `periodo_fim`). O que falta:

### 2.1 O tipo da medição — e por que não pode ser uma lista fixa

Foi o ponto mais detalhado da fala dele, e o mais fácil de errar.

O caso simples: existe a **medição 1** (normal) e a **medição 1R** (reajuste)
— o `R` colado no número diz que aquela é a de reajuste daquela.

Mas ele descreveu três desvios que acontecem de verdade:

1. **Órgão que numera em sequência.** Houve a medição 1, a 2, e a de reajuste
   virou a **medição 3** — o reajuste entrou na fila como se fosse normal.
2. **Órgão que numera em paralelo.** A medição de reajuste da 1 é a **1R**, e
   ela fica correlacionada com a 1.
3. **Medições subsidiárias.** *"Já aconteceu uma medição 1 alguma coisa e outra
   medição 1 alguma coisa, por conta de fontes diferentes, e o órgão trata
   dessa forma."*

**Conclusão de desenho:** o tipo é uma **categoria editável**, não uma lista
fechada no código; e o número da medição é **texto livre**, não número. Quem
manda na nomenclatura é o órgão, não o ERP. Tentar impor "1, 2, 3" quebraria no
primeiro contrato fora do padrão — e ele já viu isso acontecer.

Tipos iniciais, editáveis: `NORMAL`, `REAJUSTE`, `ADITIVO`, `SUBSIDIARIA`,
`COMPLEMENTAR`.

### 2.2 A correlação entre medições

A medição de reajuste **aponta para a medição que ela reajusta**. Isso é o que
permite dizer, no quadro do contrato, "a medição 1 rendeu R$ X, mais R$ Y de
reajuste". Sem a correlação, os dois valores ficam soltos e ninguém soma.

A ligação é opcional de propósito: no órgão que numera em sequência, o reajuste
é a medição 3 e mesmo assim aponta para a 1.

### 2.3 O protocolo — e o indicador que ele destrava

Ele pediu, com o motivo junto: *"a gente poder atualizar número de protocolo,
data que foi protocolado. Isso é bom porque a gente pode gerar indicadores com
isso aí pra saber o tempo de recebimento."*

Dois campos, e um indicador que nasce deles: **quantos dias entre protocolar e
receber**, por obra e por órgão. É a métrica que responde "esse cliente demora
quanto?" — e ela hoje não existe em lugar nenhum.

---

## 3. O QUADRO FINANCEIRO DO CONTRATO

Ele mudou de ideia no meio da fala, e a segunda ideia é melhor:

> *"Até pensei que seria melhor a gente ir pela obra, pelo contrato, ou ir pela
> medição, pelo título a receber. Aí talvez seja até interessante pelo
> contrato, né? Que lá vai estar relacionando todas as medições."*

**Entrar pelo contrato é o certo**, e o motivo é estrutural: contrato é o que
tem começo, meio e fim. A medição é um evento dentro dele. Quem pergunta "como
está essa obra?" quer o contrato; quem pergunta "e a medição 3?" chega nela
pelo contrato.

O quadro mostra, por contrato, uma linha por medição:

| Medição | Tipo | Período | Valor | Protocolo | Nota | Recebido |
|---|---|---|---|---|---|---|
| 1 | Normal | 01–31/07 | 100.000 | 4512 · 05/08 | NF 1234 | 92.500 em 20/09 |
| 1R | Reajuste da 1 | 01–31/07 | 3.200 | 4519 · 05/08 | NF 1240 | — |
| 2 | Normal | 01–31/08 | 88.000 | — | — | — |

E os totais que ele chamou de quadro financeiro: **contratado**, **aditivado**,
**medido**, **faturado** (o que virou nota), **recebido**, **a receber**, e
**retido**. Cada um responde uma pergunta diferente, e hoje elas se respondem
somando planilha.

---

## 4. O QUE FALTA NO CADASTRO — e que trava a emissão

### 4.1 Na obra

A obra **já tem** no modelo: CNO, alíquota de ISS, ISS retido, regime de
tributação e conta de recebimento. **O que falta é a tela expor isso.** Sem
esses campos preenchidos a nota não pode ser montada — eles entram tanto no
cálculo quanto no corpo do texto da nota.

### 4.2 A conta bancária da obra — e o filtro que ele quer

Ele pediu duas coisas ligadas:

1. **Toda obra aponta para uma conta da empresa.** Já existe no modelo.
2. **Poder filtrar os títulos pela CONTA**, e não só pela obra: *"às vezes a
   gente quer filtrar o que tem pra pagar nessa conta, o que tem pra pagar na
   outra. Às vezes é mais fácil do que filtrar por obra."*

O segundo é um filtro novo nas telas de Pagamentos e Solicitações. É pequeno e
resolve um incômodo diário.

### 4.3 O cadastro das contas bancárias — falta o Pix

Hoje a conta guarda banco, agência e conta. **Falta a chave Pix** — e o motivo
que ele deu é o uso real: *"eventualmente a gente precisa consultar, e tendo
esse cadastro das contas é o local mais fácil da gente consultar."*

Ou seja: não é para pagar por ali, é para **copiar e mandar** quando alguém
pede os dados da empresa. A tela precisa mostrar os dados de um jeito que dê
para copiar de uma vez — banco, agência, conta, CNPJ e Pix, num bloco só.

Mais de uma chave por conta (CNPJ, e-mail, telefone, aleatória).

---

## 5. A EMISSÃO DA NOTA

### 5.1 O que a pessoa informa

Ele listou, e é curto de propósito:

- **valor total ou parcial** da medição;
- se há **BDI diferenciado** — *"porque a tributação incide de forma diferente
  nessa parcela da nota que obedece o BDI diferenciado, e o corpo da nota que o
  sistema redige também fica diferente"*;
- um **campo livre de observação**. Hoje ele se chama "número do empenho", e o
  próprio dono corrigiu: *"esse campo pode ser algo mais amplo pra não focar
  apenas no número do empenho — se eu quiser colocar uma observação a mais, eu
  colocar pra sair no campo de observações da nota"*.

O resto — CNO, alíquota, regime, tomador, corpo do texto — **o sistema monta
sozinho**, lendo o cadastro da obra e do contrato. É o que o `emissaonf` já faz.

### 5.2 Automático ou manual — por EMPRESA

Decisão dele, e ela é importante para o desenho:

> *"A depender da empresa para a qual a gente está emitindo, pode ser que vai
> ter empresa que a gente emita de forma automática via API, bem como pode ter
> empresa que a gente não vai emitir de forma automática. Então a gente vai só
> fazer o lançamento manual, anexando a nota, e o sistema captura as
> informações e faz o registro."*

Então o cadastro da empresa ganha um **modo de emissão**: `API` ou `MANUAL`. Os
dois caminhos terminam no mesmo lugar — a nota registrada, arquivada e
amarrada à medição, ao contrato e ao título. No modo manual, quem lê o PDF e
extrai número, valor, data e retenções é a mesma IA que já lê nota de entrada.

### 5.3 O que a emissão automática deixa de fazer

Hoje, emitir dispara quatro coisas: a API do município, a atualização do título
no **Omie**, a atualização de uma **planilha** e a atualização do **card no
Pipefy**. Palavras dele: *"isso tudo não vai ser necessário porque tudo vai
ficar dentro do ERP."*

Sobra: emitir pela API, capturar o retorno, **capturar a NFS-e nacional**, e
arquivar tudo (XML, DANFSe, recibo) no Google Drive pela gestão de documentos
que acabou de nascer — amarrado à obra, à medição e ao título a receber.

⚠️ **O Omie e o Pipefy não se desligam no dia da virada.** Enquanto o ERP não
estiver operando de verdade nesse fluxo, desligar o que funciona é trocar o
certo pelo duvidoso — a mesma regra que vale para o FSist.

---

## 6. A TELA DE CONTROLE DE NOTAS EMITIDAS

Ele descreveu coluna por coluna. Fica registrado como ele pediu:

| Coluna | Observação |
|---|---|
| Número da nota | ordenação **decrescente** por padrão |
| Empresa | e um filtro por empresa; ou todas |
| Obra | |
| Número da medição | e o período |
| Valor bruto | |
| Retenções | ISS, IR, INSS, PIS/Cofins/CSLL — o que a contabilidade pede |
| Líquido a receber | bruto menos retido |
| Recebido | valor e **data** |
| Conta do recebimento | |
| Situação | emitida, cancelada, substituída |

E o propósito, nas palavras dele: *"às vezes a contabilidade precisa gerar um
relatório das informações — valor da nota, tributos e tal."* Ou seja, a tela
tem de **exportar**, e o ERP já exporta qualquer tela em Excel e PDF.

### Onde ela mora, e a crítica que ele mesmo pediu

Ele levantou a dúvida: *"talvez isso seja a mesma coisa que o título a receber,
ou não, não sei. Aí você vai fazer essa crítica."*

**Não é a mesma coisa, e a diferença importa:**

- **Título a receber** é o que a BWS tem para receber — inclusive coisas que
  não são medição e não têm nota de serviço.
- **Nota emitida** é o documento fiscal. Uma medição pode virar **duas notas**
  (parcial), e uma nota pode ser **cancelada e substituída** sem que o título
  mude.

São dois eixos diferentes, e forçá-los na mesma tela esconde justamente os
casos que dão trabalho. Ficam **duas telas irmãs**, ligadas nos dois sentidos —
do título se chega à nota, da nota se chega ao título.

---

## 7. MAIS DE UM MUNICÍPIO — a resposta sobre Petrolina

Pergunta dele: *"eu fiz essa API pro município do Eusébio, especificamente. Será
que o município de Petrolina em Pernambuco também tem isso aí?"*

**Tem — e é o mesmo fornecedor.** Pesquisado em 09/09/2026:

| | Eusébio/CE | Petrolina/PE |
|---|---|---|
| Provedor | E&L (`cloud.el.com.br`) | **E&L** (`cloud.el.com.br`) |
| Padrão | ABRASF 2.04 + nacional | ABRASF 2.04 + nacional (em transição) |
| Endereço | `ce-eusebio-pm-nfs-backend.cloud.el.com.br/nfse40/...` | `pe-petrolina-pm-nfs-backend.cloud.el.com.br/nfse/...` |
| Autenticação | certificado A1 | certificado A1 |

O endereço segue o mesmo molde: `{uf}-{municipio}-pm-nfs-backend.cloud.el.com.br`.

**O que isso significa na prática:** o emissor não precisa ser reescrito. Hoje o
endereço e o código IBGE estão **fixos no código** (em
`el_nfse_abrasf.py`, `el_nfse_nacional.py`, `job_nacional.py` e outros). O
trabalho é transformá-los em **configuração por empresa/município** — o que já
seria necessário de qualquer forma, porque a BWS opera com mais de um CNPJ.

**O que ainda depende de providência, não de código:**

- **Inscrição Municipal em Petrolina** e credenciamento na Secretaria de
  Finanças (Unidade de Tributos Mercantis);
- o **token do canal** — o `EL_NFSE_TOKEN` de hoje é do Eusébio; Petrolina terá
  o seu;
- os **códigos de serviço e a alíquota de ISS** de Petrolina, que não são
  necessariamente os do Eusébio.

**E um prazo que muda o planejamento:** a Lei Complementar 214/2025 tornou o
**padrão nacional obrigatório** para todos os municípios, com a convivência dos
dois padrões acabando ao longo de 2026 (o Distrito Federal, por exemplo, já
marcou 01/10/2026 como o fim do ABRASF 2.04). O sistema **já fala o padrão
nacional** — é o `el_nfse_nacional.py`. Portanto:

> **A aposta certa é o canal NACIONAL, não o ABRASF.** Investir agora em
> ABRASF por município é construir sobre algo com data para acabar.

⚠️ **Não verificado por mim:** se a BWS já tem Inscrição Municipal em
Petrolina, e a data exata em que Petrolina encerra o ABRASF. As duas coisas se
confirmam com a prefeitura, não com pesquisa.

---

## 8. ORDEM DE CONSTRUÇÃO

1. **O cadastro que destrava tudo**: expor na tela da obra os campos fiscais que
   já existem no modelo; chave Pix na conta bancária; filtro por conta nas telas
   de título.
2. **A medição completa**: tipo editável, número livre, correlação entre
   medições, protocolo com data.
3. **O quadro financeiro do contrato**, com as medições e os totais.
4. **A tela de controle de notas emitidas**, com exportação.
5. **A emissão a partir da medição** — primeiro o modo MANUAL (anexar a nota e
   deixar a IA ler), que não depende de credenciamento nenhum e já entrega
   valor.
6. **A emissão automática**, com o endereço e o município virando configuração
   por empresa, apontando para o **canal nacional**.
7. **Os indicadores**: dias entre protocolo e recebimento, por obra e por órgão.

Cada passo entrega sozinho, e o passo 5 antes do 6 é de propósito: o caminho
manual funciona no dia seguinte e serve de rede quando a API falhar.

---

## 9. O QUE PRECISA DA PALAVRA DO DONO

- **Os tipos de medição** (§2.1): `NORMAL`, `REAJUSTE`, `ADITIVO`,
  `SUBSIDIARIA`, `COMPLEMENTAR` cobrem o que acontece? Falta algum?
- **As retenções** que a tela de notas deve mostrar (§6): ISS, IR, INSS,
  PIS/Cofins/CSLL — é essa a lista que a contabilidade pede?
- **A BWS já tem Inscrição Municipal em Petrolina?** (§7)
- **Quais empresas emitem por API e quais emitem manual?** (§5.2)
