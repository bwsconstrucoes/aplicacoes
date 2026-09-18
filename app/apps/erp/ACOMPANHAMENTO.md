# ACOMPANHAMENTO — a gestão burocrática da obra (desenho, ainda não construído)

> **Estado: OS TRÊS PEDAÇOS CONSTRUÍDOS em 18/09/2026** (migrações 072 a 074).
> O 3 trouxe o **ofício gerado e numerado**, o **deferimento que registra o
> aditivo na obra** e o quadro de **quanto cada órgão demora**.
>
> Estado anterior: **pedaços 1 e 2** (migrações 072 e 073).
> O 1 trouxe processo, andamento, situação com "parado" calculado, a tela "o
> que está pendente" e o botão Assumir. O 2 trouxe a **lista de passos
> sugeridos por tipo** e o **aviso que chega sozinho**: processo com exigência,
> com previsão estourada ou parado entra na **Agenda**, que é a tela que se
> abre de manhã. O pedaço 3 continua na fila (§11).
>
> **Estado anterior: APROVADO, em construção (pedaço 1).** O desenho abaixo foi
> apresentado ao dono em 17/09/2026 e aprovado por ele em 18/09/2026, com duas
> decisões que mudaram a §12: **nada de Pipefy, e nada de dado antigo de lá**.
> Conforme for construído, o que valer vira `HISTORICO.md` e as perguntas vão
> para `PERGUNTAS.md`.

---

## 1. De onde veio o pedido

17/09/2026, palavras dele:

> *"Uma espécie de gerenciamento da parte documental, burocrática, de
> acompanhamento da obra — **não da execução**. Digamos que eu tenho uma licença
> ambiental e está vencendo: a partir dali eu vou fazer uma gestão para aquela
> licença ser renovada. Me lembra um pouco do que a gente pode fazer dentro do
> Asana. Eu tenho um processo de aditivo, aditivo de prazo. Como é que a gente
> faz atualmente? A gente tem um ambiente no Pipe chamado protocolo e medições
> (…) gerar ofícios, dar entrada, lançar alguma coisa que a gente protocolou,
> solicitação de apostilamento, aditivo de prazo, de vigência (…) acompanhar o
> andamento, alimentar do andamento: 'ó, eu liguei aqui e falei que fulano está
> em tal setor' (…) lançar previsão para publicação. Me lembrando um pouco do
> sistema SEI — **mas eu não quero uma coisa travada**. Tem que ser uma coisa
> rápida de manusear, simples de alimentar e de atualizar (…) imagina que uma
> pessoa saiu de férias: quem for fazer esse acompanhamento precisaria bater o
> olho e ver o que está pendente de ser resolvido."*

Três exigências saem daí, e elas mandam em todo o resto do desenho:

1. **Rápido de alimentar.** Se lançar um andamento custar mais que uma frase,
   ninguém lança — e um acompanhamento desatualizado é pior que nenhum, porque
   quem bate o olho acredita nele.
2. **Não travado.** Nada de etapa obrigatória, aprovação para escrever ou ordem
   imposta. Isso é o que ele está dizendo quando cita o SEI como contraexemplo.
3. **Tem que responder "o que está pendente" para quem chegou agora.** É o teste
   das férias, e é o que decide se o módulo presta.

---

## 2. O que é, e o que NÃO é

**É** o acompanhamento de assuntos que correm fora da BWS e dependem de
terceiros: órgão público, cartório, concessionária, seguradora, prefeitura.
Aditivo de prazo, apostilamento de reajuste, renovação de licença ambiental,
matrícula CNO, alvará, ART, seguro-garantia, entrada de medição.

**NÃO é** a execução física da obra — cronograma físico, diário de obra,
efetivo, produção. O dono descartou isso com todas as letras ("não da
execução"), e misturar as duas coisas faria o módulo virar outra coisa.

**Também não é** o controle de tarefas internas da equipe. Quem lança título,
quem aprova, quem concilia — isso já tem dono e já tem tela.

---

## 3. A peça central: o PROCESSO

Um **processo** é um assunto com começo, meio e fim, sempre preso a uma obra —
ou à empresa, quando é da empresa (CND, certidão, alvará da sede).

Exemplos reais: *"Aditivo de prazo nº 2"*, *"Renovação da licença ambiental"*,
*"Apostilamento do reajuste 2026"*, *"Protocolo da medição 07"*.

O que ele guarda:

| Campo | Para quê |
|---|---|
| Assunto | como a pessoa chama aquilo |
| Tipo | aditivo, apostilamento, licença, certidão, medição, outro |
| Obra (ou empresa) | recorte e escopo |
| Órgão / destinatário | para onde foi |
| Responsável | quem toca |
| Situação | ver §5 |
| Onde está | setor ou pessoa, hoje |
| Número do protocolo + data | quando existe |
| Previsão | para quando prometeram |
| Documentos | os papéis, no mesmo Arquivo de sempre |

**Obrigatório: assunto e obra. Só.** Todo o resto entra depois, conforme a
coisa anda. Exigir órgão e prazo na abertura faz a pessoa abrir o processo
"depois", e depois é nunca.

---

## 4. O andamento é UMA LINHA

Esta é a parte que decide se o módulo vive ou morre, e é literalmente o que ele
descreveu: *"eu liguei aqui e falei que fulano está em tal setor"*.

A pessoa escreve a frase, aperta enter, acabou. **Data e autor entram sozinhos.**
Na mesma linha, se quiser, ela marca três coisas opcionais:

- **onde está** — muda o "onde está" do processo;
- **para quando** — a previsão;
- **situação** — se aquele andamento mudou o estado da coisa.

Nada de formulário, nada de campo obrigatório, nada de salvar em duas etapas.
Anexo só se tiver papel (o comprovante de protocolo, o ofício de resposta).

O histórico do processo é essa lista de frases em ordem, de cima para baixo. É
isso que uma pessoa lê em trinta segundos quando assume o assunto de outra.

---

## 5. A situação, e a que o sistema calcula sozinho

Seis estados, curtos e honestos:

`rascunho` → `protocolado` → `em análise` → `exigência` → `deferido/publicado`
→ `arquivado`

E uma sétima que **ninguém digita, o sistema conclui**: **parado**. Processo sem
andamento há mais dias que o normal do tipo dele. É exatamente o que a pessoa
esquece de marcar, e é o que mais dói descobrir tarde.

> A regra por trás: **o que o sistema sabe, o sistema conclui.** Todo campo de
> situação que depende de alguém lembrar de mexer vai estar errado um dia.

---

## 6. A TELA QUE RESOLVE O CASO DAS FÉRIAS

Uma lista única — "o que está pendente" — ordenada por quem está mais perto de
virar problema, não por data de criação:

1. **vence** (a licença que expira em X dias);
2. **exigência** (voltou pedindo coisa e ninguém respondeu);
3. **previsão estourada** (disseram que publicava dia 10, passou);
4. **parado** (sem andamento há muito tempo);
5. o resto.

Cada linha diz: obra · assunto · onde está · há quantos dias não anda · quem
responde · qual é a próxima ação. Filtro por obra, por responsável e por tipo.

**Botão "Assumir".** Quem entra no lugar de quem saiu de férias passa tudo para
si de uma vez, em vez de abrir um por um. A troca fica registrada.

---

## 7. O que amarra isso ao resto do ERP — e é onde fica melhor que o Pipefy

O Pipefy não conhece a obra. O ERP conhece. Três ligações que só existem aqui:

- **A licença que vence já está no Arquivo e já aparece na Agenda.** Dali sai o
  botão *"abrir processo de renovação"*, com o documento que vence já amarrado.
  Fecha o ciclo que hoje depende de alguém ver o aviso e lembrar de agir.
- **Aditivo de prazo deferido muda a vigência da obra.** O alerta "Vigência
  vencida" do painel some porque o processo terminou, não porque alguém foi lá
  editar o campo. (Com conferência humana antes de gravar — nunca automático e
  silencioso.)
- **O protocolo da medição já existe** (`MEDICOES_E_NOTAS.md` §2.3: número e
  data, que alimentam o indicador de dias entre protocolar e receber). O
  processo não refaz isso: ele **acompanha** o que acontece depois do protocolo,
  e o número continua sendo o mesmo, num lugar só.

---

## 8. Ofícios gerados por modelo

Pedido dele: *"gerar ofícios"*. É o item mais barato de construir e o que mais
economiza tempo.

Modelo de texto por tipo de processo, com os campos da obra já preenchidos —
órgão, número do contrato, objeto, obra, processo. Gera PDF, **numera sozinho
por empresa e por ano**, e já nasce anexado ao processo e arquivado no Drive
na árvore da obra.

Editável antes de gerar, sempre. Modelo não é camisa de força.

---

## 9. O que eu NÃO faria — a lista do "não travado"

Cada item aqui é uma coisa que o SEI e o Pipefy fazem e que mataria isto:

- etapa obrigatória, que impede pular ou voltar;
- aprovação para lançar andamento;
- campo obrigatório além de assunto e obra;
- fase que só muda por botão de outra pessoa;
- exigir fechar o processo anterior para abrir o próximo.

Os passos sugeridos de cada tipo (ofício → protocolo → análise → publicação)
entram como **lista de sugestão marcável**, não como fluxo. Pular, voltar e
fechar fora de ordem tem que ser permitido — e o sistema registra, sem impedir.

---

## 10. Permissão e escopo

Ação própria (`ver_processos` / `tocar_processo`), com **escopo por obra**: o
perfil preso a obra vê os processos da obra dele e nada mais. Fora do escopo
responde 404, como em todo o resto do ERP.

Processo da EMPRESA (certidão, alvará da sede) não é da obra de ninguém: fica
sob a ação do administrativo, não sob o escopo de obra.

---

## 11. Entrega em três pedaços

**Pedaço 1 — o que já substitui o Pipefy sozinho.** Processo, andamento de uma
linha, situação (com o "parado" calculado), documentos, e a tela "o que está
pendente" com o botão Assumir. Ligado à obra e ao Arquivo. Migração de banco:
duas tabelas.

**Pedaço 2 — o que faz lembrar sozinho.** ✔ **PRONTO (18/09/2026, migração
073).** Cada tipo traz a lista do que costuma ter que ser feito, e o processo
travado entra na **Agenda**.

Duas decisões que valem registro:

- **A lista de passos é SUGESTÃO, e a prova disso é uma ausência.** A tabela
  `processo_passos` não tem `obrigatorio`, nem `depende_de`, nem `bloqueia` —
  há um teste estrutural cobrando que essas colunas nunca apareçam. Marcar fora
  de ordem é permitido, e o processo fecha com passo em branco sem reclamar.
  Os passos são LINHAS e não um JSON fechado justamente para caber o passo que
  só aquela prefeitura pede.
- **O aviso entrou na AGENDA, e não numa notificação própria.** A tela do
  Acompanhamento é a de quem já lembrou do assunto; a agenda é a que se abre de
  manhã, já tem escopo por obra, já tem "resolver" e "dispensar", e já é para
  onde o dono olha. Fazer um segundo canal de avisos seria construir de novo o
  que existe — e dividir a atenção dele em dois lugares.

**Pedaço 3 — o que fecha o ciclo.** ✔ **PRONTO (18/09/2026, migração 074).**

- **O ofício.** Modelo por tipo, com a epígrafe montada do que o ERP já sabe
  (contrato, obra, objeto, local, processo). Três decisões: o texto é **sempre
  editável antes de gerar** (modelo que não se ajusta faz a pessoa voltar para
  o Word); o **corpo fica guardado como foi enviado** (regerar do modelo meses
  depois daria outro texto, e o papel do órgão e o sistema divergiriam em
  silêncio); e a **numeração é por empresa e por ano, com restrição única no
  banco** — sem ela, duas pessoas gerando ao mesmo tempo produzem dois
  "OF 012/2026". O rascunho **não numera**: numerar o que vai ser descartado
  deixaria buraco na sequência.
  O que o ERP não sabe fica como `____________`, de propósito: espaço em branco
  é pedido de atenção, valor inventado passa despercebido.
- **O deferimento.** O sistema **propõe**, a pessoa **confirma** — nunca
  automático e calado, porque mexer sozinho no prazo de um contrato é mexer em
  dinheiro e o erro só apareceria numa medição recusada meses depois. O número
  do termo é digitado, não adivinhado: número inventado vira divergência com o
  contrato do órgão. Reusa `criar_aditivo`, que já sabe somar valor vigente e
  mexer na vigência — escrever de novo faria duas verdades sobre o mesmo
  contrato.
- **A demora por órgão.** Só entra processo **encerrado com data de protocolo**:
  o que está aberto não demorou, está demorando, e misturar puxaria a média
  para baixo por causa dos que travaram. A contagem viaja junto e a coluna
  "confiança" diz em português quando é **um caso só** — ler "média 4" de um
  caso como se fosse regra é o erro que isso evita.

---

## 12. As duas decisões do dono, 18/09/2026

Eu havia listado o Pipefy como risco em aberto. Ele fechou os dois pontos:

> *"Não vamos usar nenhum outro recurso. A ideia é a gente fechar **100% toda a
> movimentação da empresa no ERP**. E o que ele não estiver atendendo, a gente
> vai ajustar para poder atender. Não vamos utilizar Pipe, **nem trazer dados
> antigos de lá** — só as coisas novas mesmo."*

**Decisão 1 — nada de sistema paralelo.** Não existe convivência com o Pipefy,
nem período de transição com os dois alimentados. O que falta no ERP vira
ajuste no ERP, não motivo para voltar ao Pipefy. Isto derruba o "pior cenário"
que eu tinha apontado: ele não vai acontecer porque não vai haver dois.

**Decisão 2 — nada de importar o passado.** Processo antigo fica onde está, de
consulta. O acompanhamento começa vazio e só recebe coisa nova. Isso poupa a
importação suja que eu tinha proposto (cartões abertos com histórico resumido)
e, principalmente, evita que o módulo nasça cheio de assunto que ninguém vai
tocar — que é o jeito mais rápido de a tela "o que está pendente" perder
credibilidade.

**Exigência de forma, dita junto:** *"é interessante também que tenha um visual
agradável das informações"*. Não é enfeite: a tela do acompanhamento é feita
para ser olhada de relance por quem assumiu o assunto de outro. Se ela for uma
tabela crua, ninguém bate o olho — vai abrir um por um, e aí o módulo não fez o
trabalho dele.

O que continua valendo como risco, e não é software:

- **Quem alimenta.** O módulo só funciona se quem liga para o órgão lançar a
  frase. Nenhum desenho resolve isso sozinho.
- **Fica de fora**: execução física (cronograma, diário, efetivo) e tarefas
  internas da equipe.

---

## 13. As perguntas que isto tornaria possíveis

Entram em `PERGUNTAS.md` quando for construído. Já previstas:

- "O que está parado há mais de 15 dias?" 🔒 (escopo por obra)
- "Quais licenças vencem nos próximos 60 dias e não têm processo aberto?"
- "Quanto tempo a prefeitura X demora para publicar um aditivo?"
- "Quais processos estão com fulano?" — e o que vira "de quem" quando ele sai.
- "Esta obra tem algum processo aberto?" 🔒

⚠️ **Palavra ambígua: "processo".** No ERP já significa outras duas coisas —
o processo administrativo do órgão (número que vem no contrato) e o processo
judicial. Este módulo precisa de nome próprio na tela (**Acompanhamento**) para
não colidir, e a pergunta que usar "processo" solto tem de dizer qual usou.
