# O cadastro de pessoas vai para dentro do ERP — levantamento

Escrito em 04/11/2026, a pedido do dono:

> *"Essa parte de cadastro de colaboradores, a gente ainda está usando o Pipefy,
> e puxando uma planilha (…) não estamos utilizando o cadastro do ERP. Mas eu
> vejo que isso está gerando muito conflito. E eu estou vendo que é importante a
> gente colocar logo o cadastro das pessoas dentro do ERP e começar a trabalhar
> nele. (…) me diga, o que é que nós já temos, o que é que a gente precisa para
> terminar de implementar essa parte cadastral."*

Este arquivo é o levantamento, não a implementação. Ele existe para a sessão
seguinte (de qualquer área) não refazer o diagnóstico — e para o dono conferir
se eu entendi o que ele tem hoje.

---

## 1. Por que dá conflito: hoje são TRÊS cadastros da mesma pessoa

| Onde | O que é | Quem escreve | Quando atualiza |
|---|---|---|---|
| **Pipefy** — pipes *Cadastro de Colaboradores* e *Solicitação do DP* | onde o DP trabalha de verdade; um cenário do Make leva os dados para a planilha | o DP, pelo formulário do pipe | na hora |
| **Planilha "Registro de Colaboradores"**, aba *Dados Documentos* (~3.500 linhas × 78 colunas) | o espelho do Pipefy; alimenta as folhas de alimentação, transporte, GM e diaristas | o Make | a cada movimento do pipe |
| **`analisesps.colaborador`** (cópia no banco, ~30 das 78 colunas) | a cópia que a Análise de SPs guarda para não ler a planilha a cada pergunta | o botão **"Atualizar cadastro"**, apertado à mão | **só quando alguém aperta** |
| **`colaboradores`** (cadastro do ERP) | a identidade: é nela que batida, escala, pedido, despesa e título se penduram | a tela de Colaboradores do ERP e um botão do ponto | na hora |

**O ponto LÊ a terceira e PENDURA na quarta.** É isso que produz o conflito que
ele está sentindo, e ele tem três caras:

1. **Pessoa cadastrada hoje no Pipefy não aparece no ponto** até alguém apertar
   "Atualizar cadastro" na Análise de SPs. Até lá, ela bate e a batida vai para
   conferência ("fora do Registro de Colaboradores").
2. **Pessoa que o ponto criou no ERP não existe no Pipefy** — o botão
   "Cadastrar no ERP os que faltam" só anda num sentido.
3. **Dois lugares para a mesma correção.** Trocar a obra, o cargo ou o celular
   no ERP não troca no Pipefy, e vice-versa. Quem está certo depende de qual
   tela a pessoa abriu.

**Nenhum dos quatro tem o cadastro completo.** O do ERP é enxuto de propósito
(*"o suficiente para pagar e para criticar o que se paga"*); a cópia da Análise
de SPs deixa 48 colunas de fora de propósito (custo de memória e dado pessoal
que não serve àquele módulo). O cadastro COMPLETO só existe no Pipefy e na
planilha.

---

## 2. O que o ERP JÁ tem (e não precisa ser refeito)

**Do cadastro da pessoa:**
nome, CPF (único e validado), matrícula, função, obra principal, regime
(CLT/PJ/…), situação, admissão, demissão, valor da diária, auxílio-alimentação,
auxílio-transporte, chave Pix com tipo, banco/agência/conta, telefone,
observações, referência do card do Pipefy. Mais obras adicionais pelo ponto.

**Dos documentos da pessoa** — já funciona o padrão *um cadastro, uma porta*: o
documento entra DENTRO do formulário, a IA lê, preenche os campos e o arquivo
fica guardado no cadastro que ele criou. O catálogo já prevê: RG/CNH/CPF,
carteira de trabalho, ficha de registro, contrato de trabalho, ASO (com aviso
30 dias antes do vencimento), ficha de EPI, certificado de NR (aviso 45 dias),
termo de rescisão. Tudo marcado como **PESSOAL** — só quem tem acesso a dado de
pessoa vê.

**Dos pedidos que o ponto já absorveu** (e que o dono já tirou do pipe de
Solicitação do DP): atestado, declaração de comparecimento, licença, férias,
afastamento, abono, ajuste de batida, compensação e folga do banco de horas —
com caminho de aprovação **configurável pela tela** (encarregado, DP, ou os
dois), sigilo de saúde (só o DP vê CID, médico e o papel do atestado) e trilha
de quem aprovou cada etapa.

**Do desligamento:** quem é desligado perde o acesso ao ponto sozinho (celular
bloqueado, QR cancelado, saída dos aparelhos de grupo), a cada 15 minutos.

**Da estrutura:** trilha de auditoria em tudo, perfis e permissões ajustáveis
pessoa a pessoa, documento com validade e aviso de vencimento, e a Agenda que
cobra o que está vencendo.

---

## 3. O que FALTA — separado em "falta o campo" e "falta o caminho"

### 3a. Campos que o Pipefy coleta e o ERP não guarda

O cadastro do ERP não tem onde pôr:

- **Identificação:** data de nascimento, nome da mãe, sexo, RG (número, órgão
  emissor, UF e data de emissão), PIS/NIS, CTPS (número e série), título de
  eleitor, reservista, nacionalidade, naturalidade.
- **Endereço:** CEP, logradouro, número, complemento, bairro, cidade, UF.
- **Contato:** e-mail pessoal (hoje só telefone).
- **Pessoal:** estado civil, escolaridade.
- **Dependentes:** **não existe tabela nenhuma.** É o que o pipe chama de
  "cadastrar um dependente", e sem isso não há salário-família nem IR.
- **Vale-transporte com as variações:** hoje é um valor só. A planilha tem
  *categoria/modalidade* (Mês, Mensal, Seg-Qui, Seg-Sex, Cartão — e "Cartão"
  significa que não paga em dinheiro). O mesmo vale para a alimentação.
- **Convenção coletiva**, tipo de contrato e "tipo" (as colunas que a Análise de
  SPs já lê da planilha mas o ERP não guarda).
- **Gratificação mensal** (recebe? valor?), parcela única, paga por BeeVale,
  recebe via SP, nome do credor — hoje isso vive só na planilha e a folha lê de
  lá.
- **ID Fortes** (o código da pessoa no sistema de folha) — a Análise de SPs já
  guarda na cópia dela; o ERP não.

⚠️ **48 das 78 colunas da aba nunca foram mapeadas por ninguém.** A Análise de
SPs lê ~30 porque são as que a folha usa. O que existe nas outras 48 só o Pipefy
sabe — **e é por isso que eu preciso da estrutura dos dois pipes antes de criar
campo**, senão a lista acima nasce incompleta e a migração tem de ser refeita.

### 3b. Caminhos (fluxos) que não existem no ERP

O segundo pipe (*Solicitação do DP*) é uma **fila de pedidos com aprovação**. O
ERP tem essa máquina para o ponto, e **não tem** para o administrativo. Falta:

| Pedido do pipe | Existe hoje? | O que falta |
|---|---|---|
| Cadastro de novo funcionário | parcialmente | a tela existe, mas sem os campos de 3a e **sem fila de aprovação** — hoje quem cadastra, cadastra |
| **Readmissão** de colaborador | **não** | achar o desligado pela base, trazer o cadastro de volta e **guardar os dois vínculos** (não sobrescrever o primeiro) |
| **Admissão de prestador → CLT** | **não** | o campo `regime` existe, mas trocá-lo apaga a história; falta registrar que a pessoa foi prestador de tal data a tal data |
| Alteração de cargo | parcialmente | muda-se o campo, mas **sem histórico de cargo** e sem aprovação |
| Atualização de dados (telefone, Pix, endereço) | parcialmente | muda-se, mas sem fila: o pipe hoje faz o DP conferir antes |
| Solicitação de férias | **sim, no ponto** | nada — já absorvido |
| Atestado / compensação / declaração | **sim, no ponto** | nada — já absorvido |
| Solicitação de desligamento | **não** (só o efeito) | o pedido em si, com aprovação, aviso prévio, último dia e o que dispara no ponto |
| Cancelamento de um pedido | **não** | poder cancelar pedido já aberto, com motivo |
| Ajuda de custo | **não** | o pedido; o valor já existe como "observação de auxílio" na planilha |
| Vale-transporte | **não** | o pedido e as variações de modalidade |
| Cadastrar dependente | **não** | a tabela e o pedido |

### 3c. Duas coisas que ele pediu com todas as letras

1. **"O ERP também tem que alertar se o cadastro já existe"** — hoje o ERP só
   **recusa** no salvar (o CPF é único), o que é tarde: a pessoa já digitou
   tudo. Falta avisar **enquanto se digita o CPF ou o nome**, dizendo quem é,
   em que situação está e — se for alguém desligado — oferecer **readmissão** em
   vez de cadastro novo. O pipe já faz isso e é por isso que ele citou.
2. **"Colaborador e prestador de serviço (…) na prática é a mesma coisa, acho
   que não é necessário"** — concordo, e isso simplifica: **um cadastro só**,
   com o regime como campo (e o histórico de vínculo de 3b). Preciso do "pode"
   dele para tratar os dois pipes/fases como um cadastro.

---

## 4. O que eu preciso do dono para começar (ele ofereceu)

1. **O blueprint do cenário do Make** (ele disse que pode colar). É o que mostra
   o que o cenário faz hoje campo por campo — e o que vai ter de ser desligado
   quando o ERP assumir, para não ficarem os dois escrevendo na planilha.
2. **A estrutura dos DOIS pipes**: os campos de cada formulário (nome, tipo,
   obrigatório, lista de opções) e as **fases**. Exportação, print ou lista
   escrita — qualquer formato serve. Sem isso, a lista de campos de 3a nasce
   incompleta.
3. **Quatro decisões que só ele toma:**
   - a planilha "Registro de Colaboradores" **continua existindo** depois que o
     ERP assumir? (ela alimenta a folha de alimentação, transporte, GM e
     diaristas, e a Análise de SPs lê dela — se ela morrer, isso muda de lugar;
     se ela ficar, o ERP passa a ser quem a ALIMENTA, invertendo o sentido);
   - o **Pipefy roda em paralelo** por um tempo (as duas pontas abertas, com
     risco de divergir) ou **corta numa data**?
   - **quem aprova cada tipo** de pedido administrativo (hoje o do ponto é
     configurável pela tela; o administrativo pode seguir o mesmo desenho);
   - **"colaborador" e "prestador de serviço" viram um cadastro só?** (ele já
     disse que na prática é a mesma coisa — falta o "pode").

---

## 5. Ordem proposta (cada fase entrega algo utilizável)

Pensada para o DP poder trabalhar no ERP o quanto antes, sem esperar o conjunto.

- **Fase 1 — o cadastro completo.** Os campos de 3a, com os documentos
  preenchendo o que der (o padrão *um cadastro, uma porta* já existe), o alerta
  de duplicado ao digitar e a tabela de dependentes. **Depende dos pipes.**
- **Fase 2 — trazer o que já existe.** Importar da planilha/cópia o que ela tem
  das pessoas de hoje, para o ERP não nascer vazio, com relatório do que não
  casou. Sem isso, a Fase 1 vira digitação de 3.500 linhas.
- **Fase 3 — a fila de pedidos administrativos.** A máquina de aprovação que o
  ponto já tem, aplicada aos pedidos de 3b, com os tipos que o pipe tem hoje.
- **Fase 4 — vínculo com história.** Readmissão, prestador → CLT, histórico de
  cargo e de obra. É o que impede o cadastro de mentir sobre o passado.
- **Fase 5 — desligar a ponte.** O ponto passa a ler o cadastro do ERP (a chave
  já existe: *Base de pessoas* na Configuração, hoje em "Registro"), e o
  cenário do Make é desligado ou invertido. **Só depois de 1 a 4, e com o dono
  escolhendo a data.**

**O que NÃO muda:** a identidade continua sendo a linha de `colaboradores` do
ERP — é nela que batida, escala, pedido, despesa e título já se penduram. Nada
do que existe hoje precisa ser migrado de lugar.

---

## 6. Risco que precisa ficar escrito

**O dado pessoal sai da planilha e entra no banco do ERP.** Endereço, RG, nome
da mãe, PIS e dependentes de ~3.500 pessoas passam a viver num sistema com tela
na internet. O catálogo de documentos já trata isso como PESSOAL, e os perfis já
recortam quem vê o quê — mas a Fase 1 é o momento de conferir **quem vê o
cadastro completo** e de registrar essa decisão, não depois.
