# Gestão de documentos da empresa — o arquivo vivo da BWS

> Pedido do dono em **09/09/2026**. Como no cruzamento de notas, este documento
> é a especificação escrita ANTES do código, porque a decisão é dele e o
> desenho errado aqui custa meses.
>
> Palavras dele: *"eu queria um ambiente onde eu pudesse simplesmente jogar
> esse documento, ele fosse interpretado, lido, e a partir dali categorizado,
> renomeado e salvo. E que eu pudesse facilmente gerir uma tela onde eu fizesse
> filtros, pesquisasse, buscasse, e a gente pudesse ter acesso a esses
> documentos."*

---

## 1. O problema, do jeito que ele acontece hoje

Os documentos da BWS existem, e até estão organizados — mas **fora do ERP**, num
arranjo que depende de alguém lembrar de alimentar:

- um formulário do Pipefy com os tipos pré-cadastrados;
- a pessoa escolhe o tipo, envia o arquivo;
- a automação grava no Google Drive;
- outras automações leem de lá (montar processo de licitação, por exemplo).

Funciona, e não vai ser jogado fora. O que falta é o resto:

1. **Renomear e categorizar continua sendo trabalho humano.** Alguém abre o
   arquivo, entende o que é, escolhe o tipo, digita o nome.
2. **Não há busca dentro do documento.** Achar "o contrato onde consta tal
   cláusula" é abrir arquivo por arquivo.
3. **Vencimento não avisa ninguém.** Certidão vencida só aparece na hora de
   usar — quer dizer, na hora errada.
4. **Juntar o que sempre é pedido junto é manual.** "A documentação fiscal da
   obra X na competência Y" são oito arquivos que alguém garimpa toda vez.
5. **O ERP já guarda metade disso** (anexo de título, nota fiscal, comprovante)
   e essa metade não conversa com a outra.

## 2. O que muda com IA — e o que NÃO muda

O que muda: **a leitura**. A mesma inteligência que hoje lê comprovante e nota
lê um contrato social ou uma certidão e diz o que é, de quem é, de quando é e
até quando vale.

O que **não** muda, e é decisão consciente: **a IA sugere, a pessoa confirma.**
O dono já disse isso com todas as letras — *"a IA vai sugerir e a gente teria
que confirmar, qual seria o nome, a categoria etc."* Documento arquivado na
categoria errada some tão bem quanto documento perdido, e ninguém descobre até
precisar dele. Confirmar leva cinco segundos; procurar leva meia hora.

---

## 3. A TAXONOMIA — de quem é o documento

Antes do "que tipo é", vem uma pergunta mais estruturante: **a que ele pertence?**
Errar isso é o que faz sistema de documentos virar pasta bagunçada.

São **cinco donos possíveis**, e todo documento tem exatamente um:

| Dono | O que ancora | Exemplos |
|---|---|---|
| **EMPRESA** | um CNPJ da BWS | contrato social, balanço, certidões, cartão CNPJ |
| **OBRA** | uma obra | contrato de obra, OS, ART, aditivo, medição |
| **PESSOA** | um colaborador ou sócio | RG/CPF, ASO, ficha de registro, contrato de trabalho |
| **PARCEIRO** | um fornecedor ou cliente | contrato social do fornecedor, certidões dele |
| **LANÇAMENTO** | um registro do ERP | comprovante de pagamento, nota fiscal, proposta |

O quinto **já existe** e não muda: é o anexo de título, de pedido, de cotação.
A gestão de documentos **não recria** isso — ela passa a **enxergar** junto com
o resto. Documento é documento, venha de onde vier.

E há um sexto eixo que **não é dono, é recorte**: a **COMPETÊNCIA** (o mês).
Folha de agosto, guia de FGTS de agosto, DCTFWeb de agosto. Esses documentos
pertencem a uma obra ou à empresa, *e além disso* a um mês. É esse recorte que
faz o compilado fiscal funcionar.

---

## 4. O CATÁLOGO DE TIPOS

Cada tipo tem: um **código** (que vira o nome do arquivo), um **nome em
português** (que aparece na tela), o **dono**, se **vence**, e se entra em algum
**bloco**. O catálogo é editável na tela — nasce com estes, e a BWS acrescenta.

### 4.1 Empresa — cadastrais

| Código | Nome na tela | Vence? |
|---|---|---|
| `CONTRATO-SOCIAL` | Contrato social e alterações | não |
| `CARTAO-CNPJ` | Cartão CNPJ | não (mas regera-se) |
| `BALANCO` | Balanço patrimonial | anual |
| `FATURAMENTO-12M` | Declaração de faturamento (12 meses) | 12 meses |
| `COMPROVANTE-ENDERECO` | Comprovante de endereço | 90 dias |
| `INSCRICAO-ESTADUAL` | Inscrição estadual | não |
| `INSCRICAO-MUNICIPAL` | Inscrição municipal | não |
| `ALVARA` | Alvará de funcionamento | sim |
| `PROCURACAO` | Procuração | sim |
| `CERTIFICADO-DIGITAL` | Certificado digital (só o registro, nunca o arquivo) | sim |

### 4.2 Empresa — certidões (todas vencem, e é o ponto)

| Código | Nome na tela |
|---|---|
| `CND-FEDERAL` | Certidão negativa federal (RFB/PGFN) |
| `CND-ESTADUAL` | Certidão negativa estadual |
| `CND-MUNICIPAL` | Certidão negativa municipal |
| `CRF-FGTS` | Certificado de regularidade do FGTS |
| `CNDT` | Certidão negativa de débitos trabalhistas |
| `CND-FALENCIA` | Certidão de falência e concordata |
| `CERTIDAO-CREA` | Certidão de registro no CREA |

### 4.3 Empresa — licitação e técnico

| Código | Nome na tela | Vence? |
|---|---|---|
| `ATESTADO-CAPACIDADE` | Atestado de capacidade técnica | não |
| `CAT` | Certidão de acervo técnico (CAT) | não |
| `DECLARACAO` | Declaração para licitação | conforme o edital |
| `PROPOSTA-LICITACAO` | Proposta apresentada | não |
| `EDITAL` | Edital do certame | não |

### 4.4 Obra

| Código | Nome na tela | Vence? |
|---|---|---|
| `CONTRATO-OBRA` | Contrato da obra | vigência |
| `ADITIVO` | Termo aditivo | vigência |
| `OS` | Ordem de serviço | não |
| `ART` | ART / RRT | não |
| `MATRICULA-CEI-CNO` | Matrícula CNO/CEI | não |
| `LICENCA` | Licença ambiental / urbanística | sim |
| `SEGURO` | Apólice de seguro | sim |
| `MEDICAO` | Medição | não |
| `DIARIO-OBRA` | Diário de obra | não |
| `PROJETO` | Projeto e memorial | não |

### 4.5 Fiscal e trabalhista — por competência

É o bloco que o dono descreveu em detalhe, e o que mais dá trabalho hoje.

| Código | Nome na tela | Dono |
|---|---|---|
| `FOLHA` | Folha de pagamento | obra (ou empresa) |
| `RELATORIO-FGTS` | Relatório de FGTS | obra |
| `GUIA-FGTS` | Guia do FGTS | obra |
| `COMPROVANTE-FGTS` | Comprovante de pagamento do FGTS | obra |
| `DCTFWEB-RECIBO` | Recibo da DCTFWeb | empresa |
| `DCTFWEB-CREDITOS` | Resumo de créditos da DCTFWeb | empresa |
| `DARF-INSS` | DARF do INSS | empresa |
| `COMPROVANTE-INSS` | Comprovante do INSS | empresa |
| `DARF-PIS-COFINS` | DARF de PIS/Cofins | empresa |
| `COMPROVANTE-PIS-COFINS` | Comprovante de PIS/Cofins | empresa |
| `GPS` | GPS | empresa |
| `ESOCIAL-RECIBO` | Recibo do eSocial | empresa |
| `RESCISAO` | Termo de rescisão | pessoa |

### 4.6 Pessoa

| Código | Nome na tela |
|---|---|
| `DOC-IDENTIDADE` | RG / CNH / CPF |
| `CTPS` | Carteira de trabalho |
| `FICHA-REGISTRO` | Ficha de registro |
| `CONTRATO-TRABALHO` | Contrato de trabalho |
| `ASO` | Atestado de saúde ocupacional (vence) |
| `EPI-FICHA` | Ficha de entrega de EPI |
| `CERTIFICADO-NR` | Certificado de NR (vence) |
| `CONTRATO-SOCIO` | Documentos de sócio |

### 4.7 Financeiro e parceiros

| Código | Nome na tela |
|---|---|
| `NOTA-FISCAL` | Nota fiscal (já vive no cruzamento) |
| `COMPROVANTE` | Comprovante de pagamento (já vive no título) |
| `BOLETO` | Boleto |
| `CONTRATO-EMPRESTIMO` | Contrato de empréstimo bancário |
| `CONTRATO-FORNECEDOR` | Contrato com fornecedor |
| `PROPOSTA` | Proposta comercial |
| `OUTRO` | Outro (com descrição obrigatória) |

---

## 5. A NOMENCLATURA DOS ARQUIVOS

O dono pediu nome **padronizado e legível**, porque os arquivos saem do sistema:
vão por e-mail, sobem em portal de licitação, entram em pasta compactada.

### As regras

1. **Só A–Z, 0–9, hífen e sublinhado.** Nada de acento, cedilha ou espaço.
   Não é preciosismo: portal de licitação e sistema de prefeitura ainda
   engasgam com acento, e o arquivo volta corrompido ou é recusado.
2. **Sublinhado separa CAMPOS. Hífen separa palavras dentro de um campo.**
   Assim dá para ler o nome de olho e dá para a máquina separar.
3. **A ordem é: TIPO _ DONO _ REFERÊNCIA _ DATA.** Tipo primeiro porque, dentro
   de uma pasta baixada, o que agrupa é o tipo.
4. **Data sempre em AAAA-MM-DD** (ou AAAA-MM para competência). É o único
   formato que ordena sozinho.
5. **Documento que vence leva a validade, prefixada por `val`.** Bater o olho no
   nome e saber até quando vale é metade do problema resolvido.
6. **Campo que não se aplica simplesmente não aparece** — nunca sobra
   sublinhado duplo.

### O formato

```
TIPO_DONO[_REFERENCIA]_DATA.ext
```

### Exemplos reais

```
CND-FEDERAL_BWS_val-2027-03-15.pdf
CRF-FGTS_BWS_val-2026-10-02.pdf
CNDT_BWS_val-2026-12-30.pdf
CONTRATO-SOCIAL_BWS_12a-alteracao_2024-06-10.pdf
BALANCO_BWS_2025_2026-03-28.pdf
FATURAMENTO-12M_BWS_val-2027-01-31.pdf

CONTRATO-OBRA_ESCPLANALTO_2024-11-05.pdf
ADITIVO_ESCPLANALTO_02_2026-04-18.pdf
ART_ESCPLANALTO_ART-1234567_2025-02-11.pdf
MEDICAO_ESCPLANALTO_03_2026-08.pdf

FOLHA_ESCPLANALTO_2026-08.pdf
RELATORIO-FGTS_ESCPLANALTO_2026-08.pdf
GUIA-FGTS_ESCPLANALTO_2026-08.pdf
COMPROVANTE-FGTS_ESCPLANALTO_2026-08.pdf
DCTFWEB-RECIBO_BWS_2026-08.pdf
DARF-INSS_BWS_2026-08.pdf

ASO_JOAO-DA-SILVA_val-2027-01-20.pdf
CTPS_JOAO-DA-SILVA_2019-03-04.pdf
```

**O DONO no nome** é o apelido curto: a empresa pelo nome fantasia sem espaços
(`BWS`), a obra pelo código que já existe no ERP (`ESCPLANALTO`), a pessoa pelo
nome com hífens. Não é o CNPJ nem o número interno — o nome existe para uma
PESSOA ler.

**O nome original nunca se perde.** Ele fica guardado ao lado, e a tela mostra
os dois. Renomear é conveniência, não amnésia: quando o fornecedor pergunta
"você recebeu o arquivo tal", tem de dar para responder.

---

## 6. OS BLOCOS — o que sempre é pedido junto

Foi o pedido mais concreto do dono: *"eu quero a documentação fiscal da
competência tal, da obra tal"*, e um botão que baixa tudo.

Bloco é uma **lista de tipos com um recorte**. Baixar um bloco gera um `.zip`.

| Bloco | Recorte | O que junta |
|---|---|---|
| **FISCAL** | obra + competência | folha, relatório de FGTS, guia e comprovante do FGTS, recibo e resumo da DCTFWeb, DARF e comprovante do INSS, DARF e comprovante de PIS/Cofins |
| **HABILITACAO** | empresa | contrato social e alterações, cartão CNPJ, todas as certidões **válidas**, balanço, faturamento 12 meses, comprovante de endereço, documentos dos sócios |
| **CADASTRO-FORNECEDOR** | empresa | contrato social, cartão CNPJ, faturamento 12 meses, certidões válidas |
| **MEDICAO** | obra + competência | a medição, mais o bloco FISCAL da mesma competência, mais a nota e o recibo quando existirem |
| **OBRA** | obra | contrato, aditivos, OS, ART, matrícula CNO, licenças, seguro |

**O zip vem com um `CONFERENCIA.txt` dentro**, e essa é a parte que importa: ele
lista o que foi encontrado **e o que está faltando**. Bloco que entrega oito de
dez arquivos calado é pior que bloco nenhum — quem monta o processo descobre a
falta na frente do cliente.

Certidão **vencida não entra** no bloco: entra na lista de faltas, dizendo que
venceu e quando. Mandar certidão vencida é pior do que não mandar.

### Onde os botões ficam

- Na tela de gestão de documentos: baixar qualquer bloco, escolhendo o recorte.
- **No título financeiro** (foi ideia dele): num título a receber de medição, um
  botão baixa a documentação fiscal daquela competência e obra. Depois de
  emitida a nota, outro botão baixa a nota e o recibo.
- Na tela da obra: o bloco OBRA.
- Em Suprimentos: o bloco CADASTRO-FORNECEDOR, que é o que fornecedor pede.

---

## 7. BUSCA DENTRO DO DOCUMENTO — vale a pena?

O dono perguntou, e supôs que exigiria muito processamento. **Ele não está
errado no geral, mas está errado neste caso** — e a razão é uma só:

**A leitura já vai acontecer de qualquer jeito.** Para categorizar e nomear o
documento, a IA precisa ler o texto dele. Guardar esse texto no mesmo momento é
quase de graça: são alguns kilobytes por documento, e o Postgres já tem busca em
texto embutida (não precisa de serviço novo, nem de assinatura nova).

O que **seria** caro é o contrário: guardar só o PDF hoje e, daqui a dois anos,
ter de reprocessar dez mil documentos para poder buscar. Aí sim seria conta
grande. **Por isso o texto é extraído na entrada, sempre.**

O limite honesto: a busca acha **palavra**, não entende pergunta. "Contrato com
cláusula de reajuste" vai achar documentos que contenham "reajuste" — não vai
raciocinar sobre o contrato. Isso é o suficiente para o uso real, e é
exatamente o que o Google Drive faz.

---

## 8. VENCIMENTO — o que dá valor de verdade

Certidão que vence sem ninguém ver é a diferença entre ganhar e perder uma
licitação. O sistema passa a saber:

- **quanto falta** para cada documento vencer;
- **quais estão vencidos**, em destaque na tela;
- e **avisar antes** — 30, 15 e 5 dias — pelo mesmo caminho do agente de
  cobrança, que já fala com as pessoas por WhatsApp.

Isso desemboca na **Agenda do ERP**, que já estava no roteiro e agora tem a
quarta coisa dependendo dela. Vencimento de documento não é assunto à parte: é
mais um alerta no mesmo calendário.

---

## 9. O QUE NÃO ENTRA — e por quê

- **O certificado digital em si nunca sobe aqui.** Registra-se que existe e
  quando vence; o arquivo continua cifrado e sem caminho de volta pela tela.
  Quem baixa o certificado age como a empresa.
- **O Pipefy não é desligado agora.** Ele continua funcionando; a gestão nova
  nasce ao lado e recebe os documentos novos. Desligar um caminho que funciona
  antes do outro estar rodando é trocar o certo pelo duvidoso.
- **Nada de link público do Drive.** Vale a mesma regra dos anexos: o arquivo
  mora no Drive, mas quem entrega é o ERP, depois de conferir permissão.
  Documento de sócio e folha de pagamento não podem ficar abertos a quem tem o
  endereço.

---

## 10. PERMISSÃO — quem vê o quê

Documento não é tudo igual. Folha de pagamento e documento de sócio não são
para todo mundo que entra no ERP.

Três faixas, e a mais restrita ganha:

| Faixa | Quem vê | O que é |
|---|---|---|
| **ABERTO** | quem tem `ver_erp` | certidões, cartão CNPJ, contrato social, ART, OS |
| **RESTRITO** | financeiro, diretoria, DP | folha, guias, DARF, contratos, balanço |
| **PESSOAL** | DP e diretoria | documento de colaborador e de sócio |

E o escopo por obra continua valendo: administrativo de obra vê o documento
**da obra dele**, não o das outras. Isso reusa o `aplicar_escopo` que já existe
— regra de escopo nova nunca se escreve à mão.

---

## 11. ORDEM DE CONSTRUÇÃO

1. **O catálogo e o arquivo** — tipos, donos, validade, e o documento entrando
   com nome padronizado. Sem IA ainda: quem envia escolhe o tipo.
2. **A tela de gestão** — filtros por tipo, dono, obra, competência, situação de
   validade; busca; e o download.
3. **A leitura por IA** — sugere tipo, dono, datas e nome; a pessoa confirma.
   Reusa o leitor que já existe para nota e comprovante.
4. **Os blocos e o `.zip` com a conferência** do que falta.
5. **A busca dentro do texto.**
6. **Os avisos de vencimento**, junto com a Agenda.
7. **Os botões nos outros lugares** — título, obra, Suprimentos.

Cada passo entrega valor sozinho, e nenhum precisa ser refeito pelo seguinte.

---

## 12. O QUE PRECISA DA PALAVRA DO DONO

- **O catálogo de tipos** (§4): está completo? Falta algum que a BWS usa toda
  semana? Sobra algum que só atrapalha a lista?
- **A nomenclatura** (§5): o formato serve para os portais onde ele sobe
  arquivo?
- **Os blocos** (§6): a lista do bloco FISCAL está certa? É essa a documentação
  que o cliente pede na medição?
- **As faixas de permissão** (§10): folha de pagamento é mesmo restrita ao
  financeiro e ao DP?
