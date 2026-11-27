# HISTÓRICO — ERP · Pessoal e DP

Memória **desta área**. O chat é descartável; isto não.

⚑ **ESTE ARQUIVO NASCEU EM 18/11/2026**, quando o dono dividiu o chat do ERP em
quatro. O pedido dele, com as palavras dele:

> *"Talvez assim separar mais: a parte de pessoal eu trato no chat, a parte de
> suprimento eu trato no outro chat, porque assim uma coisa que eu vejo numa
> área eu jogo lá, e a outra coisa que eu vejo na outra área eu jogo no outro
> chat. Eu acho que tem como evoluir mais rápido dessa forma."*

⚠️ **O PASSADO DESTA ÁREA NÃO ESTÁ AQUI — está em `HISTORICO.md`**, que até
18/11/2026 guardava o ERP inteiro (12 mil linhas). Ele **não foi quebrado**, de
propósito: repartir doze mil linhas de memória por adivinhação perderia
contexto, e perder memória é o pior estrago possível neste repositório. Então:

- **procurar o passado** → `HISTORICO.md`, buscando por "colaborador", "DP", "etapa", "contabilidade", "portal", "agente"
- **escrever o novo** → aqui

**Leitura desta área, além do `HISTORICO.md`:** `CADASTRO_DE_PESSOAS.md`,
`MANUAL_DP.md`, `AGENTE_DP.md`, `COMO_O_AGENTE_TRABALHA.md`,
`PLACAR_DO_DP.md`, `DE_PARA_PIPEFY_DP.md`, `DESLIGAR_A_PLANILHA.md`,
`PERGUNTAS.md`.

---

## Onde o trabalho está

Cadastro de pessoas, as etapas do DP (a fila de trabalho dele),
afastamentos e retorno, solicitações que a obra faz ao DP, pedidos à
contabilidade, o agente de DP, o portal da obra, cargos e convenções.

**É a área onde esteve quase todo o trabalho de outubro e novembro de
2026**, então o `HISTORICO.md` antigo é, na prática, o histórico dela.

---

## ⚑ PENDENTE AGORA — leia isto antes de qualquer coisa

**PUBLICADO em 27/11/2026 (lote 10, 11 e 12)** — ⚠️ confirmar com ele que apertou
"Aplicar atualizações do banco" (129). Antes:
**NO RAMO, SEM PUBLICAR (26/11/2026, lote 10 e 11)** — decisão dele: *"Não vamos
publicar, vamos juntar aí com as novas respostas e fazer tudo de uma vez."*
Caixa de entrada sem marcar lido + pasta Agente, aviso de "processando", fichas
antigas apagadas, o agente autônomo (confere, conclui, envia dependente e
afastamento, avisa quem pediu), o diário com ✔/✘, a marca "🤖 agente", a tela
única e o número único. **Migração 129** — apertar "Aplicar atualizações do
banco" no momento da junção. Ver "Decisões", 26/11 (10) e (11).

**⚠️ EM OBRA NO CHAT DO PONTO (aviso dele em 23/11/2026):** a central única de
assinaturas do colaborador vai para o aplicativo do ponto, usando
`core/termos.py` como base. **Não mexer em `termos.py`** até ele avisar que
publicou; migração nova do ERP: 127 em diante (124–126 tomadas). Pedi a ele que
toda assinatura chame `pessoal_solicitacoes.termo_voltou` (fecha as férias e
despacha a rescisão/desconto) e preserve `termos.concluido` e o lacre.

**PUBLICADO (21/11/2026):** ASO sem travar (admissional reconhecido e com
alerta; demissional pedido junto com a rescisão) e o agente cobrando e lendo o
e-mail no automático — migração **123**. ⚠️ Confirmar com ele que apertou
"Aplicar atualizações do banco" e se a conta de e-mail do DP está configurada
(sem ela a leitura não faz nada).

**PUBLICADO (20/11/2026, noite, 8):** o contrato que vira indeterminado
sozinho ao fim da experiência (sem migração; a primeira rodada diária depois
da publicação alcança o estoque).

**PUBLICADO (20/11/2026, noite, 6 e 7):** a obra do mês automática (migração
**119**, que também cancela as transferências abertas sozinhas) e as regras do
cadastro (autônomo sem convenção, readmissão sem experiência). ⚠️ Confirmar
que ele apertou "Aplicar atualizações do banco" (119).

**PUBLICADO (20/11/2026, noite, 3 a 5):** experiência avisada à obra, obra
atual pelo ponto (migração **118**), afastamento do ponto virando solicitação
e transferência de obra. ⚠️ Confirmar com ele que apertou "Aplicar
atualizações do banco" (118).

**PUBLICADO em 20/11/2026 (noite)** — ele: *"Vamos ficar com esses
procedimentos por enquanto, pode publicar."* ⚠️ Confirmar com ele que apertou
"Aplicar atualizações do banco" (115, 116 e 117). Os procedimentos que faltam
(13º, ASO demissional, prorrogação da experiência, advertência, transferência,
fechamento do ponto, afastamento formal, pensão) ficaram para depois, por
decisão dele. A cobrança e a caixa do agente seguem desligadas.

**(Histórico do que estava para publicar)** a admissão
dentro da Solicitação (migração **115**), os nomes novos das telas e o lote de
20/11 (alteração, dependente, férias e cancelamento pela contabilidade). ⚠️
Uma atualização de banco: apertar "Aplicar atualizações do banco" no mesmo
momento da junção. **E a 116 (programação de férias) e a 117 (aviso assinado,
controle de férias, agente ligado).** Três atualizações de banco no total.

**Publicado (19/11/2026, tarde):** a Admissão de Colaborador (ver "Decisões",
19/11 tarde). Sem migração. ⚠️ **Pendente:** acompanhar a primeira admissão de
verdade — a leitura por IA dos documentos não pôde ser testada aqui (sem
chave de IA no ambiente de teste).

**Publicado em 19/11/2026** (ramo `claude/busy-shannon-w5pp5o`, migração
**114**): o processo do desligamento, a continuidade como prestador, a faixa do
afastamento e a ida e volta da documentação assinada. As migrações 112 e 113,
do ramo `claude/oi-vjvrn8`, já tinham sido publicadas por outro chat antes.
⚠️ Confirmar com ele que apertou "Aplicar atualizações do banco".

**Respostas dele, 19/11/2026, que fecham as perguntas deste trabalho:**

- *"O arquivo precisa ser cobrado pelo Agente e recebido. Imagino que o ideal
  é enviar via solicitação DP, o agente analisa e o DP valida o recebimento."*
  → feito: ver "Decisões", 19/11 (a ida e a volta da assinatura).
- *"Desligamento de prestador acho não precisa. Ele vai receber o pgt baseado
  no ponto nas quinzenas e pronto."* → continua fechando no atendimento.
- *"Os desligamentos antes de hoje já estão em Desligados, não vamos
  exigir."* → ficam como estão; o processo vale do dia 19/11 em diante.

**Falta publicar (18/11/2026).** Está tudo no ramo `claude/oi-vjvrn8`, testado,
e **espera o "pode" dele**:

- migração **112** (as listas de conferência do DP, configuráveis pela tela);
- migração **113** (o primeiro acesso pelo portal: códigos e a marca de
  identidade confirmada);
- a liberação de acesso que também configura perfil e obras (sem migração).

⚠️ **São duas atualizações de banco.** Ao juntar, ele tem de apertar "Aplicar
atualizações do banco" em Configurações **no mesmo momento**.

**Ainda sem resposta dele:**

- ~~o endereço de produção para o PDF de orientação~~ — resolvido em
  23/11/2026: o guia novo usa `aplicacoes.bwsconstrucoes.com.br/erp/portal`, o
  mesmo endereço que as mensagens do ERP já usam (ver `orientacao/`). O PDF e a
  mensagem de 18/11 nunca tinham sido salvos no repositório;
- se o diário de obra deve esconder também obra cuja **fase** está encerrada
  (hoje ele filtra só por `status <> 'ENCERRADA'` e **ignora a fase**, então
  obra marcada "Concluída" continua aparecendo na lista do diário).

**Aprovado por ele e ainda não construído:**

- caixa de entrada de e-mail do Suprimento (a mesma conta que envia, uma por
  empresa);
- campo do resultado do ASO ("apto" / "apto com restrição");
- a ponte ponto → **Solicitações DP** no afastamento. Correção dele, que
  manda: aprovar afastamento no ponto **não** leva o cadastro para Afastado;
  tem de abrir uma solicitação ao DP, porque a informação vai para a
  contabilidade.

O que estava pendente antes da divisão dos chats está em `HISTORICO.md`, na
seção "PENDENTE AGORA".

⚠️ **Perguntar ao dono** se o que está escrito lá já aconteceu — o estado do
mundo muda entre uma sessão e outra, e ele é a única fonte disso.

---

## Decisões desta área

### 27/11/2026 — O assistente deixa de ser um menu de 20 pratos (começando pelo Pessoal)

**O que aconteceu:** ele perguntou *"quantos funcionários temos hoje na obra de
CRECHESUAPE?"* e recebeu de volta duas opções sem relação nenhuma — *"o que tem
a pagar num período?"* e *"quais obras não emitem nota hoje por falta de
cadastro?"*. Reproduzi a frase no `entender`: as duas empataram em **0,40** de
confiança, acima do mínimo de 0,34, cada uma por **duas palavras genéricas**
(*hoje*, *obra*). A palavra que decidia tudo — **"funcionário"** — não existe em
lugar nenhum do catálogo, **e nada penaliza isso**.

**A causa de fundo, que é maior:** o catálogo tinha **20 perguntas**, em cinco
assuntos (financeiro, contratos, suprimentos, obras, documentos). **Nenhuma
sobre gente.** E o `PERGUNTAS.md` já tinha **219 perguntas escritas**. Nove por
cento implementado, e a distância crescendo — cada coisa nova que o ERP ganha
nasce invisível para o assistente, porque alguém precisaria escrever a consulta
dela à mão.

#### A virada, e foi ele quem a enxergou

> *"A pergunta que a gente vai fazer é algo meio que infinito. Não tem como ser
> previsto tudo (…) eu não queria perder tempo tendo que digitar alguma
> pergunta que na verdade vai surgir no momento. Então nessa lógica nunca vai
> funcionar. (…) Eu estava pensando se a gente não poderia construir algo
> dinâmico."*

Ele está certo, e isso derruba o caminho de "me diga as dez perguntas que você
faria" que eu tinha proposto na véspera. **Em vez de uma lista de PERGUNTAS, um
MAPA do que existe** — e a consulta montada na hora contra o mapa.

**O catálogo escrito à mão NÃO SAIU**, e não deve sair: pergunta conhecida é
código testado, exata, instantânea e **sem custo de IA**. Ele é tentado
primeiro. O mapa cobre o resto, que é o infinito.

#### As quatro peças

| Peça | O que é |
|---|---|
| `core/perguntas/mapa.py` | as **visões** do Pessoal — `pessoas`, `solicitacoes`, `ferias`, `presenca_na_obra` — com as colunas explicadas em português de negócio e **o recorte da pessoa já aplicado dentro** |
| `core/perguntas/consulta.py` | **as travas**: um SELECT só, só as visões do mapa, lista de palavras proibidas, prazo de 8 s, teto de 200 linhas |
| `core/perguntas/na_hora.py` | pede a consulta à IA e devolve a resposta **com o caminho à mostra** |
| `scripts/varrer_perguntas.py` | a **varredura dos papéis** — a ideia dele |

⚠️ **POR QUE VISÕES E NÃO AS 144 TABELAS.** Entregar o banco inteiro para a IA
escrever SQL seria mais rápido de fazer e errado por três motivos: a pessoa só
pode ver as obras dela (o assistente viraria a porta dos fundos do controle de
acesso); `situacao`, `etapa`, `vinculo` e `regime` parecem a mesma coisa e não
são (a IA escolheria errado **em silêncio**); e com 144 tabelas não há como
testar o que ela monta. Com quatro visões, há — e há.

⚠️ **O RECORTE É O MESMO DAS TELAS** (`obras_no_pessoal`), aplicado dentro da
visão. Não há regra nova: se o assistente tivesse recorte próprio, mudar a
permissão numa tela o deixaria para trás. **Há teste com banco provando que
quem alcança uma obra não conta a gente da outra** — inclusive quando a
consulta montada tenta filtrar pela obra alheia.

#### A resposta mostra o caminho, e isso não é enfeite

O dono **não tem como conferir o número** — ele não lê SQL e não deve precisar.
Então toda resposta montada na hora vem com **o que eu entendi**, **onde olhei**
e **o que filtrei**, em português, e diz com todas as letras que **foi montada
na hora**, ao contrário das do catálogo. É a mesma regra que a resposta sobre
documentos segue desde 11/09/2026: *sem a citação, é a IA falando*.

**E os três "não sei" passaram a ser três**, porque a saída de cada um é outra:
*não entendi a pergunta* (reescreva), *entendi mas o sistema não guarda isso*
(falta construir) e *entendi mas você não alcança* (falta permissão). Antes
viravam todos a mesma sugestão aleatória.

#### A varredura dos papéis — e o que ela NÃO é

Ideia dele: *"se colocar no papel de um diretor (…) de um coordenador de
suprimento ou de um supervisor de obras (…) uma IA, para poder fazer esses
questionamentos."*

⚠️ **Ela não treina nada, e dizer o contrário seria vender fumaça.** Nenhum
modelo aprende; nada fica mais esperto sozinho. O que ela faz é **gerar a lista
de perguntas no lugar dele** e medir quanto o sistema responde hoje: o que o
catálogo já pega, o que o mapa monta, e **o que não dá — separando "o dado não
existe" de "o mapa não alcança"**, que são dois trabalhos diferentes.

**É ela que casa com o sistema em mudança**, que foi a última frase dele:
cresceu uma área, mudou um campo? Roda de novo e a diferença mostra o que o
assistente perdeu. Recusa banco sem "teste" no nome e recusa rodar sem chave de
IA.

#### O que fica em aberto, dito para não parecer pronto

1. **O MODELO nunca foi exercitado contra este mapa — e só ele.** O resto do
   caminho tem teste: `na_hora.responder` ganhou um `montador` injetável, e a
   suíte percorre tudo trocando **só a ida à OpenAI** por um dublê (31 casos
   com banco). Prova que uma consulta boa chega à resposta, que uma proibida
   vira recado em português em vez de estourar a tela, que "o mapa não
   alcança" é resposta e não falha, e que seis consultas do tipo que um
   modelo competente escreveria — com `GROUP BY`, `ILIKE`, `date_trunc` e
   filtro pelos rótulos em português — rodam contra o mapa.

   **O que continua sem prova:** que o modelo ESCOLHE a visão certa. Isso só
   se mede com chave e internet, pela varredura dos papéis. Para medir:
   a chave da OpenAI entra nas configurações do ambiente de nuvem (em
   *Network secrets*, ou como variável `OPENAI_API_KEY`), e a sessão
   seguinte já a enxerga.
2. **A trava de leitura é conferência de TEXTO, não um usuário de banco
   só-leitura.** Um usuário próprio, sem permissão de escrita, seria mais
   forte. Enquanto não existe, o que segura são as travas de `consulta.py`.
3. **Não há aprendizado ainda.** Pergunta respondida bem não vira pergunta
   conhecida, e pergunta sem resposta não entra numa fila. Era a terceira
   camada do desenho e **não foi feita** — precisa de migração e de tela.
4. **Só Pessoal.** Financeiro, Suprimentos e Obras continuam só com o
   catálogo. O mapa deles é o mesmo trabalho, área por área.

**Arquivos compartilhados mexidos:** `routes.py` (rota nova
`/erp/api/perguntar/pessoal`, ação `ver_pessoal`) e
`templates/erp_perguntar.html`. **Sem migração de banco.**

---

### 27/11/2026 — Tudo da solicitação no mesmo canto: os e-mails, a devolução pela obra e a conversa inteira

Ele: *"Pra eliminar essa tela, eles teriam que estar todos dentro de cada SDP,
todo o histórico"* e *"A devolver pela obra é melhor ficar dentro da SDP
também. E lá ficar o histórico de mensagens enviadas e obviamente as respostas
(…) pra não precisar ir em nenhum outro canto pra tratar de uma mesma SDP."*

O defeito que ele viu: o e-mail à contabilidade só aparecia na solicitação
como PASSO do processo, e só enquanto o processo estava aberto — solicitação
atendida ou antiga não mostrava nada.

- `pessoal_solicitacoes.tudo_que_saiu_e_voltou` (no `ler` da solicitação):
  **E-mails à contabilidade** (todos, inclusive cancelados; clicar abre o
  e-mail com cobrar/concluir), **A devolver pela obra** (com "cobrar a obra" e
  os links do que foi e do que voltou) e **A conversa com a contabilidade**
  (cada e-mail que saiu — pedido, reenvio, cobrança — e cada resposta casada,
  em ordem, com o texto). 4 leituras ao banco, nenhuma ida à internet.
- E-mail/devolução ANTIGO sem vínculo gravado entra pela mesma regra que o
  processo já usava: mesma pessoa, até 60 dias depois da solicitação; a tela
  marca "da mesma pessoa, sem vínculo gravado".
- A conversa (texto dos e-mails) só vai para quem tem `pedir_contabilidade`
  — a obra vê o pedido, não a correspondência.
- Resposta que chegou antes de 26/11 aparece sem o texto (ele não era guardado).
- Horas na tela no horário de Fortaleza (`quandoBR`, erp_base) — antes o
  diário mostrava UTC, 3 h adiantado.

**Tirados no mesmo dia, com o "pode tirar e publicar" dele:** as abas "Todos
os e-mails à contabilidade" e "A devolver pela obra" e o botão "+ Pedido" de
"Contabilidade e agente". Ficaram Diário, rotinas, caixa e sugestões. O que se
perdeu, e ele sabia: o e-mail avulso antigo (feito pelo "+ Pedido", sem
solicitação) não tem mais lista própria — continua no banco e no caixa. As
rotas da API continuam (a criação de pedido ainda é usada pelo processo).

### 26/11/2026 (12) — O agente lê a caixa de hora em hora, nos dias úteis, fora das batidas do ponto

Ele: *"de hora em hora, mas sempre protegendo o horário de batida do ponto
(…) 7 horas, meio-dia, 13 horas, 16 horas ou 17 horas. (…) qualquer rotina que
exija processamento (…) se abstenha desses horários (…) não precisa no final
de semana."*

- **Caixa** (ler → casar → conferir → concluir): seg a sex, 8h30, 9h30,
  10h30, 11h30, 14h30, 15h30 e 18h30 (hora de Fortaleza). 12h30 e 16h30
  ficaram de fora por estarem entre duas batidas. Cada horário vale 50 min
  (se o serviço reiniciava na hora, pega a vez); a marca fica em `parametros`
  (`agente_dp.caixa_em`), então reiniciar não repete.
- **Rodada diária** (obra atual, ponte do ponto, cobrança, vigia): deixou de
  rodar "no primeiro acesso do dia" — que caía na batida das 7h — e roda a
  partir das 8h30, nunca a menos de 20 min de uma batida.
- **Quem dispara:** um relógio na linha de trabalho em segundo plano
  (`tarefas.registrar_relogio`, uma checagem por minuto), não a tela de
  ninguém. Só enfileira; a fila tem uma linha só, então a caixa nunca roda ao
  mesmo tempo que outro trabalho pesado. ⚠️ A linha liga no primeiro acesso ao
  ERP depois de o serviço reiniciar — se ninguém abrir o ERP depois de um
  reinício, o relógio fica parado até alguém abrir.
- **Custo de uma leitura:** 1 conexão ao e-mail + 1 busca + 1 leitura de
  cabeçalhos; por e-mail NOVO, 1 download; por anexo de retorno, 1 envio ao
  Drive + 1 leitura por IA. Sem e-mail novo, são 3 idas ao servidor de e-mail
  e nada mais. 7 por dia.

### 26/11/2026 (11) — O agente trabalha sozinho, com diário revisável; uma tela e um número só

Ele: *"eu quero um agente que trabalhe como um humano (…) as ações que o agente
fez de forma autônoma ficasse todo o histórico (…) opa, essa aqui deu errado."*
Decisões dele (*"concordando com o ABC"*): **envia sozinho** dependente (sim),
afastamento (só se o agente ler e estiver tudo certo), férias (não), alteração
de cargo (não); **conclui sozinho** quando o documento que volta bate — *"mas é
bom dar destaque tudo que for feito pelo agente (…) na própria listagem tem
alguma tag"*; **tela única e número único, sim** — *"você tem que manter o
casamento"* dos e-mails que já saíram com CT.

- **Conferência do retorno** (`conferencia_retorno.py`): o anexo que volta pela
  caixa (ou que o DP anexa no pedido) é lido por IA (nome, CPF, cargo,
  salário, admissão, saída) e comparado com o cadastro. Bate → conclui o
  pedido e fecha a solicitação; diverge → DIVERGENTE, para o DP; não deu para
  ler → fica para o DP. **Desligamento e Diversos nunca concluem sozinhos.** A
  rotina "conferencia" nasce em FAZ pela migração 129 (só se estava
  DESLIGADA).
- **Envio sozinho** (`pessoal_solicitacoes.enviar_sozinho_se_pode`):
  DEPENDENTE e AFASTAMENTO, depois do atendimento do DP, se a análise do
  agente não acusou nada (afastamento exige natureza, início e o atestado).
- **Aviso a quem pediu** (`avisar_quem_pediu`): ao fechar a solicitação, cartão
  verde "✅ Pedido resolvido" na entrada do portal (some no "Entendi") e
  mensagem pelo Telegram/celular (tipo `erp.pedido_resolvido` na mensageria).
  ⚠️ **Ele não respondeu como quer o aviso**; escolhi portal + Telegram.
- **Diário do agente** (`agente_diario.py`, tabela `dp_agente_acoes`, 129):
  cada coisa que ele fez sozinho, com ✔ Certo / ✘ Errado (o errado exige dizer
  o que era para ter feito). **O agente não aprende sozinho com a revisão, de
  propósito** — o "errado" vira conversa no chat e ajuste de regra. Aparece
  dentro da solicitação e na aba "Diário do agente".
- **Marca "🤖 agente"** na lista de Solicitações e cartão "Feito pelo agente"
  (clicável, filtra) — some quando todas as ações daquela solicitação são
  revisadas.
- **Tela única:** dentro da solicitação, o e-mail à contabilidade agora também
  cobra, recebe o documento que chegou por fora (anexar → arquiva na pessoa →
  o agente confere), conclui, aceita divergência e cancela. "Contabilidade e
  agente" virou configuração: Diário (abre primeiro), rotinas, caixa,
  sugestões, e-mail da contabilidade no alto. **A lista "Todos os e-mails à
  contabilidade" e "A devolver pela obra" ficaram** (não pedi para tirar —
  há CT antigo sem solicitação e o "+ Pedido" avulso vive ali).
- **Número único:** e-mail novo de solicitação sai com `[SDP-000003]` no
  assunto (`[SDP-000003/2]` no 2º e-mail da mesma, ex.: retificação). O CT
  continua por dentro como chave. A caixa casa por In-Reply-To, depois
  `[SDP-…]` (procura no assunto gravado), depois `[CT-…]` (os antigos), depois
  CPF. A tela mostra a marca (`contabilidade.marca_do_pedido` / SQL
  `substring(assunto …)`); CT sem solicitação continua mostrando o CT.

**O que ficou de fora / não verificado:** a leitura por IA do retorno não roda
aqui (sem chave); resposta só em texto (sem anexo) não conclui nada — o
cancelamento confirmado por e-mail continua pedindo o clique do DP; o agente
roda uma vez por dia, no primeiro acesso (não há horário fixo).

### 26/11/2026 (10) — Caixa de entrada: só o que o agente casou vai para a pasta "Agente"; o resto fica NÃO LIDO; o pedido mostra a resposta; aviso de "processando"; fichas antigas apagadas

Ele: *"quando a gente coloca ler, ele está lendo toda a caixa de e-mail. Só que
outros e-mails que não tem nada a ver com os assuntos, ele está deixando lido.
(…) Olha o perigo disso. (…) criar uma pasta agente (…) tudo que ele não
identificasse, ele mantinha na caixa de entrada como não lido."* E: *"a gente
clica (…) não vê o que é que a contabilidade respondeu, não vê o que é que ela
anexou."* E: *"Você clica no botão e você fica na dúvida se está havendo algum
processamento ou não."*

**O que o levantamento achou (e foi corrigido):** a leitura baixava com RFC822
(marca lida no servidor) as 40 primeiras não lidas — TODAS, de qualquer
assunto; o "ler sem gravar" também marcava; o texto da resposta não era
guardado; o evento dizia "os dados batem com o cadastro" sem ler o documento;
o "Ler agora" exigia `configurar` (o DP via o botão e levava 403); o recado do
resultado sumia porque a tela era redesenhada em seguida.

- `email_entrada.rodada`: busca por UID, só não lidas dos últimos 30 dias;
  lê o Message-ID de todas com PEEK e descarta as já vistas (senão as que não
  casam, que agora ficam não lidas, tomariam as 40 vagas para sempre); baixa
  com `BODY.PEEK[]`. O que CASOU com pedido/devolução vai para
  `INBOX.Agente` (criada e assinada na 1ª vez; nome pelo LIST — Locaweb usa
  "INBOX."), lido, via `UID MOVE`. Servidor sem MOVE: ficam lidas onde estão
  (não se copia-e-apaga). O resto fica intocado, NÃO lido.
- Migração **129**: `email_entrada_mensagens.corpo` (o texto, até 20 mil
  caracteres) e `movida_para`. Sem a migração, o código segue sem gravar o
  texto (`_tem_coluna_corpo`).
- `contabilidade.o_que_voltou` + `blocoDoQueVoltou` (erp_base): o pedido (na
  tela da Contabilidade e no pedido ao DP) mostra "O que a contabilidade
  respondeu" — o texto de cada resposta e os documentos que vieram (que já iam
  para o Arquivo da pessoa, com a referência "CT-… — retorno").
- `receber` sem leitura diz "o documento voltou — o conteúdo ainda não foi
  conferido pelo sistema" (a conferência de conteúdo não existe ainda).
- `/erp/api/dp/caixa/<acao>` passou a `pedir_contabilidade`.
- `api()` (erp_base): todo clique que chama o servidor desativa o botão, põe
  ⏳ e mostra "Processando…" no alto até a resposta (0,3 s de tolerância).
- Fichas antigas apagadas (ele: *"é lixo"*): `dlg-unica` e o que a servia em
  Colaboradores, `dlg-ponto` em Pedidos ao DP, `dlg-ficha` em Mensagens, e as
  funções dos botões que saíram (alteração, readmissão, troca, desligamento,
  retorno, afastar, mandar à contabilidade, + Solicitação, cancelar
  desligamento, alterar/cancelar férias). Testes que protegiam as fichas
  antigas foram reescritos para proteger o modal-ponte.

**Não há horário 21h/0h no código do agente:** a rodada é uma por dia, puxada
pelo primeiro acesso ao ERP. Ver a resposta a ele sobre isso.

### 26/11/2026 (9) — A Solicitação Diversa: pessoa opcional e, se o DP quiser, à contabilidade em texto

Ele, sobre o reajuste só de salário (que ficou sem porta quando "Registrar
alteração" saiu): *"Eu acho que é melhor colocar em solicitação diversa. É
algo que é muito raro de acontecer. Permitir que a solicitação diversa
opcionalmente seja encaminhada à contabilidade com a demanda, textual mesmo,
sem problema. E o DP edita o cadastro da pessoa manualmente. E essa opção serve
para outras possibilidades também que venham a surgir."*

- `TIPOS["DIVERSOS"]`: `pessoa_opcional` (o portal mostra "Colaborador (se o
  pedido for sobre alguém)", sem asterisco; o alcance continua conferido por
  `portal._pode_pela_pessoa`) e o campo do DP `encaminhar_contabilidade`
  ("Não — o DP resolve aqui" vem marcado; "Sim — mandar o texto à
  contabilidade").
- "Sim": o pedido fica Em andamento e o pedido à contabilidade nasce com o
  modelo novo `DIVERSOS` (`contabilidade.TEMPLATES`): "Solicitamos o que
  segue:" + o texto do pedido; espera de volta a alteração contratual (o DP
  conclui à mão se a resposta for só o e-mail). Sem pessoa ligada, o
  atendimento com "Sim" é recusado com a explicação.
- "Não": fecha na hora, como antes. O cadastro não muda sozinho em nenhum dos
  dois — quem edita é o DP.
Conferido no navegador: o portal com a pessoa opcional (pedido enviado) e a
janela de atender com a pergunta.

### 26/11/2026 (8) — "Todo pleito é pelo portal", agora sem exceção — inclusive o do DP

Ele: *"conforme eu falei, todo pleito ele é feito no portal. Então, qualquer
coisa que eu não tenha citado, para não dar conflito na tentativa de
operacionalizar pelo ERP, então vamos fazer pelo portal. Inclusive o próprio
DP, se precisar fazer de alguma coisa, ele também acessa o portal e faz o
pleito. E aquele pleito vai entrar em solicitações."*

Saíram, com uma frase de "peça pelo Portal da obra" no lugar:
- Cadastro › Histórico: **Registrar alteração, Readmitir, Trocar o vínculo,
  Desligar** (ficam "Conferência concluída" e "Mudar a etapa" — trabalho do DP
  sobre o cadastro, não pedido);
- **↩️ Registrar o retorno…** (na faixa do afastado e na aba Demandas);
- Pedidos ao DP: **+ Solicitação** (virou "Abrir pedido pelo portal") e
  **Pedir o cancelamento deste desligamento…**;
- Férias: **Pedir alteração / Pedir cancelamento**.
Cobertura conferida: o portal tem cada um desses tipos, e o DP pede por
qualquer pessoa (`portal.pode_pedir_por_outros`). As funções por trás ficaram
no código, sem porta. Teste: `test_cadastro_so_pelo_portal_banco.py`.

**Lacuna dita a ele:** reajuste SÓ de salário (sem troca de cargo) não tem
tipo no portal — "Alteração de Cargo" exige o cargo pleiteado. Era o
"Registrar alteração" que fazia isso.

### 26/11/2026 (7) — Todo pleito é pelo portal: saem "+ Colaborador", "Mandar para a contabilidade…" e "Afastar…"

Ele: *"Dentro do sistema, tem o botão Mais Colaborador (…) As regras de
cadastro não batem com as regras do portal (…) ficou faltando um bocado de
informação (…) Vamos eliminar essa possibilidade de cadastro colaborador
dentro do ERP e deixar apenas no portal. E a gente concentra o regramento todo
lá."* E, na aba Demandas: *"botei solicitar férias. Aí ele montou um rascunho
(…) não tem campo para preencher (…) férias de quando? (…) esse botão não está
casando com a metodologia do portal (…) Tira esse botão aqui (…) também não é
para ter esse botão [Afastar] (…) Todo pleito é pelo portal. Se fizer por
aqui atrapalha, porque aqui não está sincronizado com o portal."*

- `erp_colaboradores.html`: saíram o "+ Colaborador" (no lugar, a frase "pessoa
  nova entra pelo Portal da obra"), o "📨 Mandar para a contabilidade…" da aba
  Demandas e o "🏥 Afastar…" do bloco de afastamento — cada um com a frase de
  onde o pedido nasce agora.
- Servidor: `_so_quem_ja_existe` em `/erp/api/colaboradores/completo` e em
  `/erp/api/colaboradores` (POST) — pelo ERP só se EDITA quem existe; pessoa
  nova responde "entra pelo Portal da obra". As importações (planilha,
  Fortes) e o pré-cadastro do portal seguem por outro caminho e não mudaram.
- As funções por trás (`mandarParaAContabilidade`, `afastar`, `formulario(null)`)
  ficaram no código, sem porta.

**NÃO tirei, e perguntei a ele:** no Histórico do cadastro ainda há
"Registrar alteração", "Readmitir", "Trocar vínculo" e "Desligar"; e o
"↩️ Registrar o retorno…" do afastamento. Pela regra "todo pleito é pelo
portal" seriam candidatos — mas tirar funcionalidade é decisão dele
(CLAUDE.md, 17/11: *"eu não pedi pra tirar"*).

### 26/11/2026 (6) — Desligamento a pedido: o portal exigia "vai trabalhar o aviso?" e não mostrava a pergunta

Ele: *"está como obrigatório, mas esse campo não apareceu aqui. Aí a pessoa
está bloqueada, não consegue enviar o pedido, porque está perguntando uma coisa
que o sistema não está permitindo preencher."*

**Causa:** os campos que só existem num VÍNCULO (`cumpre_aviso`, da carteira;
`saida_prestador`, do prestador — este de hoje cedo) moravam só no ramo
`por_vinculo`, e o portal (`portal.tipos`) e a tela do DP
(`tipos_para_tela`) desenhavam só os `campos` do tipo. O campo nunca existia
na tela; o servidor o exigia. Os testes de 24/11 cobriam o servidor, e a tela
"não foi vista no navegador" — estava escrito no registro de 24/11 (5).

**Conserto:** `campos_para_desenhar(regra)` — os do tipo + os de cada ramo, no
lugar do vizinho — usado pelas duas telas. Conferido no navegador: com "À
Pedido do Colaborador" a pergunta aparece e o pedido sai. Teste novo: para
TODO tipo, o que pode ser exigido (por vínculo ou por outra resposta) está
entre os campos desenhados.

### 26/11/2026 (5) — O ASO travava a admissão pelo portal: a validade agora é calculada

Relato da obra, trazido por ele: *"Quando a gente coloca pra concluir o
cadastro no novo app dá essa msg pedindo o vencimento do exame médico… só que
não tem onde colocar no app e no exame também não tem o vencimento."* A
mensagem: *"Atestado de saúde ocupacional vence — informe até quando vale."*

**Causa:** o tipo ASO está marcado como "vence" no catálogo, e
`arquivo.service.arquivar` recusa documento que vence sem validade. O portal
não pergunta validade (nem deveria: o papel não traz). No pedido de admissão,
pedido e papéis gravam na mesma transação — o ASO derrubava o pedido inteiro.
O mesmo valia para a devolução de ASO (demissional/retorno) pelo portal.

**Decisão minha, a confirmar com ele:** `VALIDADE_PADRAO_MESES = {"ASO": 12}`
— em branco, a validade é a data do exame (ou a do envio, quando não há) +
12 meses, gravada com a observação "validade estimada… corrija se o PCMSO
disser outro prazo". Por quê 12: NR-7, o periódico é anual para quem tem risco
ocupacional, que é o caso da obra; o aviso de vencimento continua funcionando.
As telas do ERP (Documentos da pessoa e Arquivo) deixaram de exigir a data
para o ASO e dizem a regra. Os outros tipos que vencem continuam exigindo.

### 26/11/2026 (4) — O MODAL-PONTE DA PESSOA: uma janela, todas as abas, de qualquer tela

**Ele, cobrando pela terceira vez o mesmo assunto (13/11, 17/11, 18/11):**

> *"a conexão entre as telas de cadastro continua ruim (…) todas as telas que
> têm relação com a pessoa, eu poder abrir de um único local, e esse ambiente
> ser partilhado em todas as áreas que se relacionem com o colaborador (…)
> Eu já havia pedido isso, mas ainda não tá bom, tá cheio de furos."*
> E: *"Em resumo essa nova tela seria apenas uma ponte para as demais telas.
> Abre um modal com todas as abas (…) Mas tudo abrindo dentro do mesmo modal."*
> E: *"na parte de operadores (…) não é todo mundo quem pode visualizar. Isso
> tem que estar atrelado ao perfil do usuário."*

**POR QUE OS CONSERTOS ANTERIORES NÃO FECHARAM:** havia TRÊS fichas da mesma
pessoa (a do ponto, em `gestao.html`, mostrada num quadro; a `ficha()` de
Colaboradores, com Resumo/Pagamentos/Documentos/Mensagens/Operador; e o
formulário completo `formulario()`), e CADA TELA tinha a sua própria cópia de
"janela com quadro" + escuta de mensagens. Cada conserto acertava um caminho e
deixava os outros. Um levantamento achou 15 furos: links com `target=_blank`
(Pedidos ao DP, Regras, Cargos), navegação para outra página (Arquivo,
Mensagens sem ponto), "Editar"/"Documentos" da ficha do ponto MORTOS em
Pedidos ao DP e em Mensagens, "Ver ficha" de Títulos sem o número da pessoa,
Operador inexistente na ficha do ponto e dependente de `configurar`, os
botões do ponto no alto de todas as áreas, `?pedido=` ignorado.

**O QUE FOI FEITO — UMA porta só:**
- `abrirPessoa(id, aba, extra)` em `erp_base.html` (vale em TODA tela do ERP,
  do ponto e da mensageria): um `<dialog>` com o nome e o resumo da pessoa no
  alto e as abas **Cadastro · Documentos · Demandas · Ponto · Pagamentos ·
  Mensagens · Operador**. Cada aba é a tela que JÁ EXISTE, embutida (sem
  menu), num quadro que fica carregado enquanto o modal está aberto. Atalho
  em HTML: `data-abrir-ambiente="ID"` (+ `data-ambiente-aba`).
- `GET /erp/api/pessoa/<id>/ambiente` (ação nova `abrir_pessoa`, implicada
  por `ver_pessoal` ou `ver_ponto`; catálogo: seção Colaboradores): o
  cabeçalho e QUAIS abas quem olha pode abrir — a regra de cada aba é a da
  tela dela (Operador = `gerir_usuarios`; Ponto = `ver_ponto`; Mensagens =
  `ver_mensagens`; Cadastro editável = `editar_colaboradores`, senão o
  resumo). Teste: o DP não vê Operador.
- Colaboradores embutida aceita `?mostrar=` e `?ambiente=1`:
  `/erp/colaboradores/cadastro/<id>` (cadastro | arquivos | demandas — o
  formulário sem as abas que viraram abas do modal) e a rota nova
  `/erp/colaboradores/pessoa/<id>` (resumo | documentos | pagamentos |
  mensagens | operador, para quem só vê). Dentro do modal, o que levava a
  outra tela troca de aba (`pedirAoAmbiente`): liberar acesso → Operador;
  "abrir o cadastro completo" → Cadastro; "ver o ponto" → Ponto.
- A ficha do ponto com `?area=ponto` vira a aba Ponto: só as abas do ponto
  (+ "Documentos assinados"), sem nome, sem "Fechar"; e, em qualquer modo, os
  botões do ponto (ajustar, compensação, atestado…) só aparecem na ÁREA do
  ponto — não mais em Mensagens/Cadastro (queixa dele).
- Entradas ligadas ao modal: lista e quadro de Colaboradores e `?editar=` /
  `?documentos=` / `?pessoa=`; Pedidos ao DP (abrir o cadastro, abrir os
  documentos para anexar, o nome da pessoa, "Ver o ponto" do desligamento);
  TODAS as telas do ponto (`abrirPessoaPainel` → modal, na aba Ponto e na
  sub-aba pedida); Mensagens; Arquivo; Regras do DP; Cargos; Títulos ("Ver
  ficha", agora com a pessoa); Férias (as duas listas); Configurações ›
  Operadores ("👤 ver a pessoa" — a lista de operadores passou a trazer o
  `colaborador_id`).
- Operador: a aba deixou de quebrar para quem gere operadores sem
  `configurar` (as obras vêm de `/erp/api/lancamento/dados`).
- Pedidos ao DP aceita `?pedido=ID` (a aba Demandas manda para lá).

**O que ficou como estava, e por quê:** o "abrir" de um pedido dentro de
Demandas ainda abre a tela de Pedidos numa aba nova (é outro objeto, não a
pessoa — agora pelo menos cai no pedido). "Imprimir o espelho" e "ver tudo no
painel das mensagens" também abrem aba nova (impressão e painel geral). O
aplicativo do celular (`ponto/app.html`) não usa o modal — é outra tela. As
fichas antigas (`dlg-unica`, `dlg-ponto`, `dlg-ficha`) ficaram no código como
reserva, sem porta que leve a elas; tirar é proposta, não feito.

Conferido no navegador com banco de teste: as sete abas, a entrada por
Pedidos ao DP, pelo Ponto, por Operadores e pela lista, sem erro de
JavaScript e sem aba nova aberta.
### 26/11/2026 (4) — ⚠️ A fila de acesso ao portal não aparecia em Operadores

**Ele:**

> *"Algo errado na parte de operadores. Aparece a pendência em Pessoal, clico,
> sou remetido pra Operadores, mas não abre a tela pra liberação e confirmação
> do perfil. Tampouco se eu clicar em adm e operadores, não vejo quem tá
> pendente."*

**A causa, e ela é de UMA LINHA:** `carregarFilaDoPortal()` — a chamada que
busca a fila — tinha ido parar **dentro do "Salvar" do formulário de PERFIS**,
em `erp_config.html`. O cartão "Pedidos de acesso ao portal" só nascia se
alguém abrisse um perfil de acesso e salvasse. Em toda abertura normal da tela
a fila simplesmente **não era buscada**.

⚠️ **E não havia erro nenhum.** O cartão nasce com a classe `oculto` e só sai
dela quando a fila chega; sem a chamada, ele ficava invisível, em silêncio. A
tela parecia funcionar. Foi preciso o dono tropeçar para alguém notar — e isso
levou **um dia inteiro** desde a publicação (25/11).

**O segundo sintoma era outro defeito, não o mesmo:** o link "Liberar em
Operadores" era `/erp/configuracoes#usuarios`, **igual para todas as pessoas da
fila**. Mesmo com o cartão aparecendo, quem clicava na pendência de alguém
chegava numa lista e tinha de achar a pessoa de novo. Agora o link leva o
número do pedido (`?pedido=N`) e, do outro lado, a confirmação **abre sozinha**:
confere o e-mail, cria o acesso, manda o convite e abre o cadastro do operador
para escolher perfil e obras — que é o que ele pediu em 24/11.

#### A lição que vale mais que o conserto

**Este é o terceiro defeito desta classe em três dias**, e os três têm a mesma
cara: uma ou mais linhas coladas **dentro de uma função**, quando deveriam
estar na abertura da tela. Começam na coluna zero, o arquivo continua válido, o
navegador não reclama — e o código simplesmente não roda.

Há um irmão ainda de pé, encontrado de passagem e **não consertado** (não foi
pedido): em `erp_config.html`, as linhas `carregarDepara()`,
`carregarOperadores()`, o registro do botão **"+ Novo operador"**, o do
**"Instalar de-para"** e `carregar()` existem **duas vezes** — uma correta, na
abertura, e outra **dentro do clique de "Aplicar atualizações do banco"**.
Efeito: depois de aplicar as migrações, "+ Novo operador" abre **dois**
diálogos, porque ganhou um segundo ouvinte. É pequeno, mas é real.

**O teste que ficou** (`tests/test_tela_da_fila_de_acesso.py`) trava os dois
defeitos: a fila tem de ser carregada na sequência de abertura, não pode estar
dentro do Salvar de perfis, e o link de Pessoal tem de levar o pedido.

⚠️ **O que o teste NÃO faz, e está escrito nele:** ele não entende JavaScript.
Escrevi primeiro um contador de profundidade de chaves e **ele não funcionou**
neste arquivo — o template mistura Jinja, crases com HTML dentro e expressões
regulares, e a conta sai errada. **Provado: com o defeito de volta, o contador
dizia que estava tudo bem.** Troquei pela conferência simples, e provei que
essa pega (2 testes falham com o defeito, 3 passam sem ele). Uma conferência
que mente é pior que nenhuma.

**Arquivos compartilhados mexidos:** `templates/erp_config.html` (a tela de
Configurações, que todas as áreas usam) e `templates/erp_colaboradores.html`.
**Sem migração de banco.**

---

### 26/11/2026 (3) — Autônomo mensalista exige o salário do mês; salário e diária em destaque no atendimento

Ele: *"para o colaborador Autônomo Mensalista, o campo Salário do Mês se torna
obrigatório, pois ele recebe não é por diárias, mas sim por um valor
pré-determinado por mês (…) Exibir esse campo e dar destaque a ele, pois é uma
informação sensível. Dar destaque também ao Valor da Diária, que também deve
aparecer na tela de atendimento do autônomo (RPA)."*
- `obrigatorios_se` CADASTRO: `AUTONOMO_MENSALISTA` → `valor_mes`; a pergunta
  "diarista ou mensalista" some para ele (`ocultos_se`) e `traduzir_do_pipe`
  grava `pagamento = MENSALISTA`. O rótulo `valor_mes` virou "Salário do Mês".
- Tela do DP: faixa amarela com "Salário do mês" e/ou "Valor da diária" no
  alto do pedido e dentro da janela de Atender (`destaqueDoValor`).
- Pedido já aberto de autônomo mensalista sem o valor: não é barrado
  retroativamente; o DP corrige pelo "Corrigir os dados".

### 26/11/2026 (2) — Os diaristas da Análise de SPs passam a ver quem foi admitido pelo ERP

Fecha a lacuna de 25/11/2026 (6). Feito a pedido dele neste chat; o registro
completo está em `analisesps/HISTORICO.md` (26/11/2026). Aqui no ERP só mudou
`pessoas.para_folha`, que ganhou o filtro `cpfs`.

### 26/11/2026 — Subtítulo nos pedidos; o desligamento de prestador diz como saiu

Ele: *"quando se trata de admissão, a gente colocar o tipo de vínculo e o tipo
de contrato como subtítulo, só para você bater o olho (…) desligamento,
colocar o tipo de desligamento (…) eu clico e não aparece, só diz assim, data
de saída e observações."* Na alteração de cargo: *"é um pleito pequeno, tá ok."*
- `pessoal_solicitacoes.resumo`: admissão/readmissão/admissão de prestador →
  vínculo · contrato; desligamento → a forma (e, no pedido de demissão, "sai de
  imediato"/"trabalha o aviso"; "continua como prestador" quando for). Aparece
  embaixo do título do pedido e embaixo do tipo na LISTA de solicitações (sem
  consulta a mais: `d.dados` vem na mesma leitura).
- **O porquê de "só data de saída e observações":** o pedido era de
  PRESTADOR, e o ramo PRESTADOR do desligamento não perguntava a forma (as da
  CLT não servem). Ganhou `saida_prestador` (obrigatório): "A pedido do
  prestador", "Pela empresa", "Fim do serviço ou do contrato". Pedido antigo de
  prestador fica sem — o subtítulo só não aparece.

### 25/11/2026 (8) — As testemunhas da recusa assinam na tela, com CPF; a trava da recusa passa a valer nas duas portas

Ele: *"faz as testemunhas assinarem na tela com CPF."* Ao fazer, achei que a
central de assinaturas do APLICATIVO DO PONTO (`ponto/app_documentos.py`)
também chama `termos.registrar_recusa` — a trava de (7), posta só na rota do
portal, era contornável por lá. As duas regras foram para
`core/termos.registrar_recusa`, que serve às duas portas:
- `TIPOS_SEM_RECUSA` (pedido de demissão, autorização de desconto) — o portal
  só lê a lista para não oferecer a opção.
- `_testemunhas`: duas, cada uma com nome completo, CPF válido e assinatura com
  o dedo; CPFs diferentes entre si e diferentes do de quem recusou. O PDF da
  recusa traz nome, CPF e a assinatura de cada uma; o banco guarda nome e CPF
  (a imagem vai só no PDF).
- Telas: o portal (`erp_portal.html`) e o aplicativo do ponto (`ponto/templates/
  ponto/app.html`, aba "Recusou" dos documentos da equipe) ganharam, por
  testemunha, CPF e um quadro de assinatura.
⚠️ `core/termos.py` e a tela do aplicativo do ponto são arquivos que o chat do
PONTO também mexe — registrado no `ponto/HISTORICO.md`. As telas não foram
vistas num celular; o servidor tem teste (inclusive o PDF com as duas
assinaturas).

### 25/11/2026 (7) — A recusa de assinar não vale no pedido de demissão nem na autorização de desconto; guia do PIN

Ele perguntou se a recusa de assinatura já existia. Existia (portal: motivo +
nome de duas testemunhas; o DP valida), mas valia para QUALQUER documento — e a
recusa validada conta como "concluído" (`termos.concluido`). No pedido de
demissão isso deixava a rescisão ir à contabilidade sem a carta (contra a
decisão do mesmo dia), e na autorização de desconto deixava o desconto seguir
sem autorização (CLT art. 462). Agora `portal.TERMOS_SEM_RECUSA` trava os dois:
a rota recusa, e a tela troca a opção por "avise o DP". A trava NÃO está em
`core/termos.py` porque a central de assinaturas do ponto usa o mesmo arquivo.
Não conferi se já existe em produção alguma recusa validada desses dois tipos.

Limite que ficou dito a ele: as testemunhas da recusa são só NOMES digitados —
não assinam, não têm CPF. Proposta (não feita): assinatura das testemunhas na
tela ou o caminho do papel com as assinaturas delas.

Guia do PIN em `orientacao/` (página 1 para o colaborador, página 2 para o DP e
o administrativo; PDF, imagem e texto de WhatsApp), mais uma mensagem curta
sobre a importância do PIN (`mensagem_importancia_do_pin.txt`).

### 25/11/2026 (6) — A admissão NÃO é mais lançada na planilha nem no Pipefy

Confirmado pelo dono: *"não lança mais"*. Consequência: o cálculo dos
diaristas da Análise de SPs, que lê só a cópia da planilha
(`analisesps.colaborador`), não enxerga quem foi admitido só pelo ERP — os dias
entre o início e a admissão ficam sem pagamento pelo sistema. O ponto não tem
o problema (lê `public.colaboradores`). A troca de fonte é da área da Análise
de SPs (`pessoas.para_folha` já entrega as duas datas); ficou como tarefa
sugerida ao dono, não feita aqui.

### 25/11/2026 (5) — Alteração de cargo com convenção e piso; a recusa vira alerta na entrada do portal

**Alteração de cargo.** Ele: *"a obra faz o pleito de alteração, ela vai
indicar o cargo e no DP, na hora de ele encaminhar para a contabilidade, ele tem
que selecionar a convenção e automaticamente aparecer valor (…) Cargo,
convenção e valor."* Os campos do DP eram só "cargo deferido" e "salário" — sem
convenção, a janela não tinha de onde puxar o piso (a mesma mecânica da
admissão, que já funcionava). Entrou `convencao_id` nos campos do DP; a janela
abre com o cargo pleiteado e a convenção ATUAL da pessoa (`pessoa_convencao_id`
no `ler`), e o piso entra sozinho. O e-mail à contabilidade ganhou a linha
"Convenção" (`contabil_processo._dados_do_pedido` → `convencao_nova`).

**Recusa.** Ele: *"uma recusa é algo mais crítico (…) o natural é ser atendido.
(…) o ideal é que a pessoa, na hora que ela logasse no portal, ela já se
deparasse com aquela mensagem, aquele alerta de recusa."* Antes: a recusa só
virava etiqueta vermelha no fim de "o que você pediu", com o motivo — sem
alerta, sem aviso. Agora: caixa vermelha no ALTO do portal, com o pedido, a
pessoa e o motivo, até quem pediu tocar em "Entendi". O "visto" é um evento na
trilha (`QUEM_PEDIU_VIU_A_RECUSA`, sem migração); o DP vê, no pedido, "quem
pediu viu a recusa em…" ou "ainda não abriu o portal desde a recusa". Só quem
abriu o pedido vê o alerta e marca o visto (`portal.ciente_da_recusa`; de outro,
404). Aviso no celular na recusa NÃO foi feito — ficou como oferta a ele.

### 25/11/2026 (4) — Data de início ≠ data de admissão: o DP confirma a admissão ao atender

Ele: *"não necessariamente a data de início ela é igual à data de admissão, por
questões de fechamento de folha (…) a gente gosta de admitir no primeiro dia ou
no 16º dia, para poder a pessoa entrar dentro da quinzena. E aqueles dias entre
a data de início e a admissão a pessoa paga na diária."* E: *"o que vale para a
contabilidade (…) é a data de admissão. A data de início não importa."* (A
mensagem dele chegou cortada em "a data de admissão pode vir já pré-preenchida
com a…" — entendi "com a data de início", e foi o que fiz.)

**O que estava errado:** o pedido só tem a data de início; o pré-cadastro e a
readmissão copiavam essa data para a admissão, e o DP não tinha onde trocá-la
no atendimento. O e-mail de registro da carteira saía com a data de início.

**O que mudou:**
- "Data de admissão (a da carteira — vale para a contabilidade)" entrou nos
  campos do DP ao atender a Admissão de Colaborador, a readmissão e a Admissão
  de Prestador (prestador → CTPS). Vem preenchida com o início; a dica mostra
  o início e o próximo dia 1º ou 16 ("os dias entre um e outro são diária").
  Não aparece para quem entra como prestador.
- O cadastro guarda as duas: `data_inicio` (da obra) e `admissao` (do DP).
  `pessoas.readmitir` deixou de forçar as duas iguais. `pessoas.salvar`, ao
  trocar a admissão, leva junto o vínculo aberto que tinha a data antiga
  (senão férias e rescisão contariam do dia errado).
- O e-mail à contabilidade já usava `admissao` — agora ela é a certa.

**A conferência que ele pediu, no cálculo dos diaristas (Análise de SPs):**
está certo. A regra é dia a dia (`analisesps/folha_vinculo.classificar_dia`,
tradução da fórmula da coluna AH da Mobponto): do início até a véspera da
admissão é DIÁRIA; da admissão em diante é CTPS e sai da lista dos diaristas.
Sem pagamento dobrado, sem dia perdido. **MAS ela lê a planilha "Registro de
Colaboradores"** (cópia em `analisesps.colaborador`, alimentada pelo Pipefy),
**não o ERP.** A admissão feita só pelo ERP não chega lá sozinha — a
pergunta ao dono sobre isso está na resposta deste dia. Quando o ERP virar a
fonte, `pessoas.para_folha` já entrega as duas datas (`data_inicio`,
`data_admissao`). Não mexi na Análise de SPs.

Pontos de atenção do cálculo, registrados para quando o ERP virar a fonte:
dia de ponto ANTES do início cai como CTPS e é descartado sem aviso; a diária
de CTPS só sai se o "Tipo" for exatamente "CTPS"; sem admissão e CLT, os dias
ficam "sem decidir".
### 25/11/2026 (4) — O manual do Portal da Obra: online e em PDF

**Ele:**

> *"Preciso criar um manual do portal de solicitações com o que temos até
> então. No padrão do manual do ponto. Clique pra visualizar online e ainda
> possibilidade de baixar pdf. Subdividir ele pelas áreas e explicar pra que
> serve e como ele funciona."*

**O que existe agora**, no mesmo desenho do manual do ponto (que está de pé
desde 15/10/2026 e já provou que funciona):

| | Onde |
|---|---|
| A página | `/erp/portal/manual/` (e `/erp/portal/manual`, que redireciona) |
| O PDF | `/erp/portal/manual.pdf` — 23 páginas A4 |
| Os arquivos | `app/apps/erp/manual_portal/` (index.html + `telas/` + o PDF) |
| Os links | **📖 Manual** na barra azul do portal (sem rolar) e no pé da página |

#### ⚑ ELE MUDOU DUAS COISAS NO MESMO DIA, e as duas são a mesma frase

> *"Eu não entendi bem exatamente onde é que está o link para acesso ao
> manual. Mas eu acho que o ideal é na tela depois do login, né? Você loga,
> aí a tela de entrada e nela a coisa, porque não fica exposto esse manual.
> Só para quem tem acesso."*

**1. O link subiu para o alto.** Ele estava só no PÉ da página, e o dono não
achou. Link que a pessoa precisa procurar é link que não existe. Agora está na
barra azul, visível na primeira tela depois de entrar — e continua no pé, para
quem rolou a página inteira e ficou com dúvida. São dois caminhos para a mesma
porta, na mesma tela: não é o "cardápio de portas" que a regra proíbe.

**2. O manual DEIXOU DE SER PÚBLICO.** Nasceu público, no desenho do manual do
ponto. As quatro rotas passaram a exigir login (`ver_erp`, a ação mais baixa
que existe: qualquer um que entre no sistema lê). O link saiu da tela pública
de **criar o acesso**, que é de quem ainda não entrou.

⚠️ **A TRAVA DE VERDADE NÃO É O DECORADOR — É A PASTA.** O manual morava em
`static/manual_portal/`, e o `static/` do blueprint é servido pelo Flask em
`/erp/static/...` **sem passar por login nenhum** (é onde vivem o CSS e os
ícones). Enquanto esteve lá, `/erp/static/manual_portal/index.html` respondia a
qualquer um, e o login das rotas seria encenação. **Conferido no navegador:
respondia mesmo, 200.** Por isso a pasta mudou para `app/apps/erp/manual_portal/`,
fora do `static/`, e há teste travando o retorno dela.

⚠️ **O QUE ISSO CUSTA, e está dito porque é real:** quem está criando o
primeiro acesso **não consegue mais ler o manual** — e é a parte 1 que explica
justamente aquela tela. Quem travar na porta depende de alguém mandar o PDF.
Foi escolha dele, com o motivo dele: o manual conta como a empresa trabalha por
dentro, e isso não precisa ficar aberto na internet.

**Por que o manual do PONTO continua público e isso não é incoerência:** lá o
manual ensina o primeiro acesso de quem nunca teve senha — não há login a
exigir. Aqui o acesso já existe antes de a pessoa precisar do manual.

#### Dois defeitos que a homologação pegou ao fechar o manual

Nenhum dos dois tem a ver com o pedido dele — apareceram porque, ao exigir
login, o manual passou a ser **tela do ERP** e entrou nos testes que toda tela
do ERP enfrenta. Valem registro porque custariam caro soltos:

1. **A página não tinha `<body>`.** Ela nasceu copiando o esqueleto do manual
   do ponto, que também não tem — o navegador fecha sozinho e ninguém nota. A
   homologação exige, e está certa. Agora o manual tem `</head><body>…</body>`.
2. **O teste de homologação quebrava no PDF.** Ele lê toda resposta como texto
   UTF-8 e procura `<body`; o PDF é binário e estourava no primeiro byte. Não
   era defeito do manual: o teste supunha que toda rota de TELA devolve
   página. Agora ele olha o `Content-Type` e, no que não é HTML, confere o que
   faz sentido (abriu 200, não jogou ninguém para o login). **Mexe num
   arquivo que serve o ERP inteiro** — vale para qualquer rota futura que
   entregue arquivo.

**Subdividido pelas áreas, como ele pediu**, e as fechadas entram também: 1
entrar · 2 a tela inicial · **3 Colaboradores (os 14 pedidos, grupo a grupo)**
· **4 Suprimentos** · **5 Pagamentos** · **6 Diário de obra** · 7 pendências
(assinar e devolver) · 8 avisos · 9 acompanhar · 10 quem vê o quê · 11 pode e
não pode · 12 mensagens da tela · 13 dúvidas.

⚑ **Por que a área FECHADA ganhou parte própria no manual:** é a mesma razão
pela qual ela aparece no portal (`core/portal.py`). Manual que pula Suprimentos
faria a obra achar que nunca vai existir — e continuar pedindo material por
WhatsApp, que é o que se quer acabar. A parte dela diz uma frase: *continue
pedindo como você pede hoje*.

**As 17 telas são de um banco de teste**, com nomes, CPFs e obra **inventados**
— nenhuma pessoa de verdade entra no repositório.

#### O que foi feito para o manual NÃO envelhecer em silêncio

É o risco de verdade deste trabalho, e o manual do ponto já viveu isso
(*"ao mudar uma tela ou uma regra que o colaborador vê, o manual fica velho"*).
Três coisas, porque lembrar não é um plano:

1. **`tests/test_manual_do_portal.py`** quebra quando uma **área** nova ou um
   **tipo de pedido** novo não aparecem no manual, quando a conta de pedidos
   escrita nele não bate com a tela, quando uma imagem citada não existe, e
   quando uma área fechada é descrita como aberta. Já pegou uma tela sobrando
   na pasta.
2. **`app/apps/erp/scripts/semear_manual_portal.py`** monta de novo o portal
   cheio (obra, equipe, pedidos em quatro situações, pendências, avisos e um
   aviso prévio para assinar) num banco de teste. **Recusa qualquer banco sem
   "teste" no nome.** Sem ele, refotografar custa meia hora de dado montado à
   mão — e é esse custo que faz manual morrer.
3. **`app/apps/erp/scripts/imprimir_manual_portal.js`** reimprime o PDF da
   própria página. O PDF é cópia: mudar a página e não reimprimir deixa os
   dois diferentes, e **nada acusa** — está anotado em `PERGUNTAS.md` como
   limite conhecido.

#### O que o teste NÃO alcança, e fica dito

Ele confere **nomes e contas**, não redação. Tela que muda de jeito sem mudar
de nome passa batida — por exemplo, se o botão "Enviar o pedido" virar outra
coisa, ou se a ordem dos blocos do portal mudar de novo (mudou em 23 e em
24/11). Quem mexer no portal continua tendo de olhar a parte correspondente.

⚑ **O manual já nasceu tendo de alcançar a `main` — e isso prova o risco
acima.** Entre escrever e publicar, outros chats publicaram três mudanças que
a obra VÊ, e as três entraram no texto antes de o manual subir: **quem libera
o acesso ao portal é o ADMINISTRADOR** (era "o DP libera", e o teste de nomes
não pegaria isso), a **carta do pedido de demissão** passou a ser sempre
exigida, e a leitura de documentos passou a aceitar **PDF, foto grande, foto
de iPhone e a certidão de nascimento do dependente**. As 17 telas foram
refotografadas contra a `main` e comparadas pixel a pixel com as antigas:
três tinham mudado.

**Arquivos compartilhados mexidos** (importa para quem roda outro chat do
ERP): `routes.py` (quatro rotas novas, junto da `pagina_portal`),
`tests/test_permissoes_rotas.py` e `PERGUNTAS.md`. Mexidos também
`templates/erp_portal.html` (o link no pé) e
`templates/erp_primeiro_acesso_portal.html` (o link antes de entrar).
**Sem migração de banco.**

---

### 25/11/2026 (3) — Três decisões dele: carta sempre, ponto na hora, foto de iPhone

Ele: *"Pedido de demissão tem que ter carta de pedido sempre, seja pelo painel,
seja anexado. A admissão pedida pelo portal, sim, o cara imediatamente já pode
bater ponto. Foto de iPhone no portal, sim, tem que aceitar também."*

- **Carta sempre** (fecha a "decisão minha, a confirmar" de 24/11 (5)). As
  duas portas continuam: a carta anexada pela obra, ou o texto que a pessoa
  assina pelo portal (ou no papel, fotografado — plano B do termo). O que
  mudou é que **não há terceira**: no pedido de demissão, o passo "Pedido de
  demissão assinado" não aceita mais "feito por fora do sistema" nem "não se
  aplica" (`desligamento_processo.marcar` recusa; a tela esconde o link pelo
  `sem_marca_manual`), e a trava da rescisão diz "a carta ainda não voltou".
  O texto do portal deixou de dizer "(opcional)": diz que uma das duas é
  obrigatória. Marca manual feita ANTES disto continua valendo (não mexi em
  dado gravado).
- **Ponto na hora:** já era assim — o pré-cadastro da admissão pedida pela obra
  bate ponto antes de o DP atender. Nada mudou no código; fica registrado que
  é a decisão dele, não um descuido. Quem não for admitido sai pelo
  "Cancelado", que conta como desligado e corta o ponto.
- **Foto de iPhone (HEIC):** dependência nova `pillow-heif`, com o ok dele.
  `core/comum/fotos.py` converte para JPG na chegada dos quatro envios do
  portal; o leitor por IA também abre HEIC. Ver `CONTEXTO.md` §9, 25/11/2026.
  Não testado com um iPhone de verdade: o teste gera um HEIC na hora.

### 25/11/2026 (2) — Liberar o acesso ao portal é do administrador; o DP só vê a fila

Ele: *"o portal não é só para o DP (…) quem é para fazer a liberação tem que
ter acesso à parte de operadores (…) dizer o tipo de pessoa, o tipo de obra
(…) isso não é o DP quem vai fazer"*; e, ajustando a proposta: *"o DP pode ser
avisado. É bom que ele visualize (…) ele só não vai ter o botão (…) para o
administrador, tanto no aviso no celular como na tela de início do ERP."*
- `liberar_acesso_portal` → só ADMIN (e quem gere operadores, por
  implicação). Ação nova `ver_pedidos_de_acesso` (ADMIN e DP) para VER a fila.
  A seção "Acesso ao portal" ganhou LER (ver) além de EDITAR (liberar).
  **Migração 128**: o perfil pronto "Departamento pessoal" desce de EDITAR
  para LER nessa seção.
- **Configurações › Operadores** tem agora, no topo, o bloco "Pedidos de
  acesso ao portal (N)" com Liberar (confirma o e-mail e abre o cadastro do
  operador já com a pessoa, para perfil e obras) e Recusar (com motivo).
- **Colaboradores** (DP): a fila continua aparecendo, só leitura, com
  "quem libera é o administrador" (para o ADMIN, um link "Liberar em
  Operadores").
- **Tela de início**: cartão "N pedidos de acesso ao portal esperando
  liberação", só para quem libera, levando a Operadores.
- **Aviso no celular**: `pedidos_de_portal.avisar_administradores`, no pedido
  NOVO (repetido e correção de e-mail não avisam), a cada ADMIN ativo com
  telefone ou CPF, pela mensageria com o tipo novo `erp.acesso_portal`
  (`mensageria/core.py` — registrado lá).

### 25/11/2026 — Dependente com certidão de nascimento: reconhecida, guardada como do dependente e lida para o quadro dele

Ele, cadastrando uma criança: *"ele só tem certidão de nascimento e o sistema
tá exigindo RG"*; e *"o sistema tá identificando como se fosse documento do
titular (…) e ele não tá lendo o documento e preenchendo também não."*
Três defeitos juntos:
- A leitura em lote (`documentos_de_pessoa.ler_varios`) não conhecia certidão:
  `TIPOS_RECONHECIDOS` e a instrução ganharam DOC-DEPENDENTE (certidão de
  nascimento ou casamento), e a instrução diz que na certidão o nome é o de
  quem nasceu, não o dos pais.
- O "diga o que é este arquivo" do portal escondia "Documento do dependente";
  agora aparece no pedido de dependente. Nesse pedido, um RG também conta
  como documento do dependente — e é GUARDADO como DOC-DEPENDENTE, não como a
  identidade do titular.
- A leitura preenchia só os campos do formulário comum; o dependente vive em
  quadros repetidos. Agora o que a leitura achou vai para o primeiro quadro
  sem nome (nome, nascimento e, pela certidão, parentesco Filho/Cônjuge), só
  em campo vazio, e o recado pede para conferir. Não toca nos dados do
  colaborador.
- **Vários dependentes de uma vez** — ele: *"pode ser que eu esteja
  cadastrando mais de um dependente."* No pedido de dependente cada documento
  é lido SOZINHO (a leitura em lote supõe uma pessoa só) e preenche o quadro
  de um dependente. Custo: uma leitura de IA por documento, e não uma por lote.
- **O funcionário pela certidão** — ele: *"o ideal era que ele conseguisse
  buscar o nome do funcionário na hora que eu colocasse o documento. Aí, se
  não, a pessoa fazia a pesquisa e selecionava."* A certidão traz mãe e pai;
  nome IGUAL a um só colaborador do alcance de quem pede → escolhido; parecido
  ou vários → a lista aparece; nada → a busca de sempre. Nunca troca quem já
  foi escolhido. Um recado só no fim diz o que foi preenchido.
- **E a leitura nem acontecia** (mesmo dia, ele: *"a IA não tá conseguindo
  fazer a leitura"*, com "não consegui ler"): as duas leituras de
  `documentos_de_pessoa` mandavam o arquivo CRU como imagem. PDF cru a IA
  recusa (certidão chega muito em PDF), e foto de celular inteira estoura o
  tamanho. Agora usam o preparo do `documentos/leitor.py`: PDF vira PNG (até 2
  páginas por arquivo) e foto é reduzida a 1800 px. Isso vale também para a
  admissão. NÃO coberto: foto HEIC de iPhone (o Pillow não abre sem uma
  biblioteca a mais — dependência nova, perguntar) e o teto de IA da pessoa
  (se ele estourar, a leitura também volta "não consegui ler").
- Conferido no navegador com respostas simuladas (dois documentos, dois
  quadros preenchidos com parentesco Filho, a mãe achada). Não verificado com
  uma certidão de verdade (a leitura usa a IA).

### 24/11/2026 (5) — Pedido de demissão: "sai de imediato ou trabalha o aviso" é da obra; a carta anexada vale como assinada

Ele: *"o mais comum é que o colaborador peça para sair de imediato (…) o campo
data do aviso prévio está obrigatório (…) vamos ajustar para poder a gente
trabalhar de uma forma ou de outra"*; *"pode ser que a pessoa na obra já tenha
feito a cartinha e a gente vai anexar ao pedido"*; e sobre o texto gerado
para assinar: *"acho que esse fluxo não se encaixa bem aqui, já que é o
colaborador que está pedindo."*
- No pedido de demissão (e só nele, `visiveis_se`), a obra responde **"O
  colaborador vai trabalhar o aviso?"** — "Não — sai de imediato (pede
  dispensa do aviso)" vem primeiro. Obrigatório nessa forma. Saiu dos campos
  do DP (o DP corrige se precisar). A data passou a se chamar "Data do aviso
  prévio (no pedido de demissão, a data do pedido)" — ela continua
  obrigatória: é o dia em que a pessoa pediu, base da rescisão.
- **A carta** (portal, opcional, só no pedido de demissão): anexada, entra
  como documento "Pedido de demissão assinado" do pedido. No atendimento,
  `desligamento_processo.gerar_termo` acha a carta e a registra como o
  documento assinado (no papel, já validado pelo DP que atendeu) — a pessoa
  NÃO assina de novo e o processo segue (`termo_voltou`). Sem carta, o
  sistema gera o texto e a obra colhe a assinatura no portal, como antes.
- **Decisão minha, a confirmar com ele:** mantive o texto gerado para quando
  não há carta. A lei pede o pedido escrito e assinado pela pessoa; o texto
  gerado é o "papel pronto" que ela lê e assina no celular. A alternativa
  seria exigir a carta sempre.
- A tela do formulário no portal NÃO foi vista no navegador (o mecanismo de
  mostrar/esconder é o mesmo dos outros pedidos; o servidor tem teste).

### 24/11/2026 (5) — Aba "Operador" na ficha da pessoa, com o MESMO formulário

**Ele:**

> *"Quando eu entro no cadastro de um colaborador em pessoal, eu queria ter
> acesso à parte de operadores, para editar o cadastro dele, ou seja, ter a
> aba lá, operadores (…) da mesma forma que tem lá cadastro, ponto,
> mensagens, aí ter operador, aí clicar e abrir o cadastro operador. **Mas sem
> fechar o modal inicial**, permitindo voltar para ver as outras configurações
> da pessoa."*

**O que mudou:** a ficha da pessoa ganhou a aba **Operador**, ao lado de
Resumo, Ponto, Pagamentos, Documentos e Mensagens. Ela abre o cadastro de
operador inteiro — perfil, perfis acrescentados, obras, cargo, fundo fixo,
senha, situação — **dentro da ficha**. Salvar **não fecha nada**: o aviso
aparece ali e dá para voltar para as outras abas.

**Só para quem gere operadores.** É a mesma alçada que a tela de
Configurações exige; não pode ser menor só porque o caminho é outro.

#### ⚑ A DECISÃO QUE IMPORTA: é o MESMO formulário, não uma cópia

O formulário morava dentro do `erp_config.html` (470 linhas). Copiá-lo daria
**duas telas de cadastro de operador**, e no primeiro ajuste uma ficaria para
trás — a regra dele de 17/11 ("um cadastro, uma porta") e a mesma lição da
tela de fases que nasceu e morreu em 24/11.

Então ele saiu para **`erp_operador_form.html`**, e as duas telas fazem
`{% include %}` do mesmo arquivo. Ganhou dois parâmetros:

- `alvo` — onde desenhar (sem ele, a janela de Configurações, como sempre);
- `aoSalvar` — o que fazer depois (antes chamava `carregarOperadores()`
  direto, função que só existe numa das telas: seria "is not defined" na
  outra).

**Há teste travando isso** (`test_o_formulario_do_operador_mora_num_lugar_so`):
ele acusa se alguém declarar `formOperador` fora do arquivo compartilhado, ou
se uma das duas telas deixar de incluí-lo.

#### A aba NÃO vira uma segunda porta para criar acesso

Pessoa sem operador ligado vê um recado explicando e um botão para o cadastro
completo — **liberar acesso continua sendo só na aba Endereço**, onde está o
e-mail que vira o login. Duas portas para criar acesso é exatamente o que ele
cobrou em 17/11.

#### Dois consertos que vieram de arrasto

1. **A ficha não sabia se a pessoa tem acesso.** O `usuario_id` não vinha no
   que a ficha carrega, então a aba achava que ninguém tinha operador.
   Achado dirigindo a tela, não por teste.
2. **As vigias de tela não enxergavam `{% include %}`.** Elas liam o arquivo
   cru, então o formulário incluído era invisível para elas e toda vigia
   acusava "chama função que não existe". Agora resolvem o include **no
   lugar** — importa que seja no lugar, e não no fim: o include fica DENTRO
   de um `<script>`, e jogado no fim cairia fora de qualquer bloco.

**Arquivos compartilhados mexidos** (importa para quem roda outro chat do
ERP): `erp_config.html`, `tests/test_telas_javascript.py` e
`core/pessoal.py`. Nasceu `templates/erp_operador_form.html`.

⚑ **A junção com a `main` teve de ser feita à mão, e isto vai acontecer de
novo:** enquanto este formulário mudava de lugar, dois commits da `main` (a
entrada (3) e a (4), logo abaixo) mexiam DENTRO dele — "cargo base", "a obra
do ponto + as marcadas", "confere equipamentos" e a lista de obras que volta a
aparecer. O Git não tinha como casar as duas coisas: um lado trocou 504 linhas
por um `include`, o outro editou aquelas 504 linhas. As quatro mudanças deles
foram levadas para dentro de `erp_operador_form.html` uma a uma, com os
comentários que explicam cada decisão. **Quem for mexer no cadastro do
operador agora mexe num arquivo só** — era justamente esse o ganho.

---

### 24/11/2026 (4) — Toda lista de caixinhas longa ganha busca; "responde" vira "confere equipamentos"; "a obra do ponto + as marcadas"

Ele: *"isso aí já é básico e já é para estar assim, nas regras gerais do
sistema (…) As obras não é pesquisável."* A busca automática da base só
cobria `<select>`. `erp_base.html` ganhou `aplicarBuscaEmCaixinhas`: toda
`.filtro-opcoes` com mais de 12 itens ganha o campo de filtro, sozinha, em
qualquer tela do ERP (quem já tem busca própria logo antes ou dentro é
respeitado; exceção com `data-sem-busca`). O que está marcado nunca some ao
filtrar. Isso alcança também os filtros laterais que não tinham busca
(credor, obra, conta nos títulos e pagamentos, etc.).

"Responde", ao lado de cada obra no operador — ele: *"é um termo que ninguém
nem entende"* — virou **"confere equipamentos"**, com a explicação: marcar a
obra é enxergar; conferir é receber todo mês a conferência dos equipamentos
locados daquela obra (e marca a obra junto). No cadastro da obra o bloco
continua "Quem responde por esta obra" (área de Obras, não mexi).

E o alcance: *"É a obra do ponto mais as obras marcadas abaixo."* — é o que
o sistema já faz; a opção dizia "Só as obras marcadas abaixo" e enganava.
Agora: "A obra onde bate ponto + as marcadas abaixo".

### 24/11/2026 (3) — Operadores: a lista de obras some de quem está em "só os lançamentos"; o "cargo (herança)" fica

Ele: *"no perfil da pessoa, não aparece a lista das obras para selecionar (…)
eu não consigo adicionar mais obras."* A tela só mostrava as obras com "tudo
das obras marcadas" — o administrativo de obra nasce em "só os lançamentos".
Isso fazia sentido quando a obra só valia para o financeiro; desde 24/11 ela
vale para o Pessoal, o portal e o diário. Agora a lista (e empresa/projeto)
aparece sempre que a pessoa não vê todas as obras, com o aviso de que no
financeiro continua valendo "só os lançamentos". O servidor já gravava as
obras em qualquer caso.

Ele também pediu para excluir o "Cargo (herança)", *"se não tiver outra
coisa"*. Tem: é ele que diz escritório × obra (`PERFIS_SO_PORTAL`) e decide o
acesso de quem não tem perfil escolhido. Ficou, com o nome "Cargo base" e a
explicação disso na tela.

### 24/11/2026 (2) — Portal: os botões no alto, as pendências agrupadas; a caixinha de tipos por assunto

Ele: *"Sempre manter os botões para solicitação na parte superior. E logo
abaixo (…) os avisos, o que já foi solicitado, o que tem pendente (…) para o
colaborador assinar (…) similar ao aviso da equipe. Para estar agrupado e a
gente poder expandir."* E: *"solicitação de férias, alteração de férias,
cancelamento de férias. Não tem como agrupar melhor (…)? Colocar o cancelar
solicitação de desligamento abaixo de desligamento."*
- Ordem do portal: **Abrir uma solicitação** → **Pendências da obra**
  (novo bloco; "Para o colaborador assinar" e "Documentos a devolver"
  recolhidos, com a quantidade em destaque) → **Avisos da equipe** → **O que
  você já pediu**. Antes, assinar e devolver ficavam abertos ACIMA dos botões.
  (O chat de Suprimentos tinha posto os dois "no alto" em 23/11; a decisão do
  dono agora é outra.)
- A caixinha "O que você precisa" vem com títulos (`portal.GRUPOS_DO_PESSOAL`):
  Admissão · Desligamento (o cancelamento logo abaixo) · Férias (as três) ·
  Afastamento · Dados e benefícios · Outros. A ordem é a dos grupos.
- Conferido com a tela montada no navegador (dados de exemplo).

### 24/11/2026 — ⚠️ VAZAMENTO: o administrativo de obra via o Pessoal da empresa inteira

Ele, depois de liberar administrativos pelo portal: *"estão vendo (…) as
pendências, as solicitações do DP. Uma determinada pessoa passou a informação
de 2 desligamentos (…) que ele não tem nada a ver."*

**A causa:** as telas do Pessoal usavam `obras_do_usuario`, que devolve "sem
filtro" para quem enxerga por AUTORIA ("só o que eu lancei"). Colaborador e
solicitação não têm autor que sirva de recorte, então o administrativo de
obra (padrão PROPRIOS) via a fila de solicitações, a lista e a FICHA de
qualquer colaborador, férias e pedidos à contabilidade da empresa inteira — e
o portal também. O aviso "readmissões não foram feitas" ia sem recorte
nenhum para qualquer um. É a mesma armadilha achada nas Locações em
11/09/2026 (`obras_de_registro_sem_autor`).

**O conserto:** `permissoes.obras_no_pessoal` — quem enxerga todas as obras
OU atende o DP (`atender_dp`) vê tudo; os demais, as obras deles (marcadas,
projeto, empresa, a do ponto). NÃO dava para usar só
`obras_de_registro_sem_autor`: o DP também está em autoria, e o "sem filtro"
era o que o deixava ver tudo — teria sido cortado. Conferido perfil a perfil:
só o ADMINISTRATIVO_OBRA muda. Aplicado em: fila e detalhe de solicitações,
readmissões pendentes, lista/busca/ficha de colaboradores, opções de obras do
cadastro, fila do cadastro, férias e períodos, pedidos à contabilidade, e o
portal inteiro (`portal.obras_que_alcanca`). Teste com banco reproduz o caso
(falha sem o conserto).

**Ainda com a mesma armadilha, em OUTRAS áreas (não mexi):** Arquivo
(`api_arquivo_donos`, `core/arquivo/service.py`), Diário de obra (quadro,
procurar, pendências — o portal usa), Suprimentos (`core/suprimentos/
solicitacao.py`, `compras.py`), lista de obras, agenda, notas e a lista de
despesas com colaborador (`core/pessoal.py::listar`). Cada uma precisa da
decisão da área: lá pode haver autor que sirva de recorte.

**E a decisão maior, no mesmo dia** — ele: *"tanto a parte do financeiro,
pessoal, ponto, suprimento, tudo, quem acessa é o pessoal do escritório (…) o
pessoal da obra (…) têm acesso ao portal (…) Isso em tudo."* E o diário: *"é
só para eles lançarem (…) olhar somente a obra deles e pode visualizar no
calendário."*
- `permissoes.PERFIS_SO_PORTAL = (ADMINISTRATIVO_OBRA,)` e `so_portal()`.
  No `_guarda_permissao`, TELA do ERP (GET fora de `/erp/api/` e de
  `/erp/portal`) para esse perfil leva ao portal, exceto
  `TELAS_DE_QUEM_E_DA_OBRA` (o diário, a folha do diário e o anexo). As APIs
  seguem pela ação de cada uma — é o que o portal chama.
- `pode_ver_obra` para esse perfil usa as obras dele (nunca o "sem filtro" da
  autoria): o diário, o calendário e a logo só da obra dele.
- O portal esconde "ERP completo"; a tela do diário, aberta pelo portal, não
  mostra os módulos do escritório.
- ⚠️ NÃO feito por perfil no banco: o cadastro de perfis junta, na mesma
  seção, "pedir ao DP pelo portal" e "abrir Solicitações no ERP". Separar
  mexeria em todas as áreas; a porta por perfil resolve sem isso.
- ⚠️ O que o administrativo PERDE: Contas a pagar / Lançar / Despesas com
  colaborador no ERP (o roteiro de homologação dizia que abriam). Se alguém
  lança pagamento por lá hoje, deixa de conseguir — o portal ainda não tem a
  área de pagamentos. Avisado ao dono.
- As LISTAS das outras áreas (suprimentos, títulos) mantêm a regra delas
  (a autoria recorta de verdade lá) — tentei trocar no `obras_do_usuario` e o
  teste de Suprimentos "quem só vê os próprios" mostrou que mudaria regra
  alheia. Supervisor e gestor de obra NÃO entraram na lista — ele, 24/11:
  *"depois a gente decide, ainda não tem nenhum cadastrado."* Publicado em
  24/11/2026.

### 23/11/2026 (9) — O guia do portal virou o guia de ACESSO, numa página

Ele: *"não precisa avisar o que é que vai ser solicitado lá pela parte do DP
(…) É só avisando como é que a pessoa faz o cadastramento (…) tem a
solicitação de liberação de acesso. Aí o pedido vai chegar, aí a
administração vai liberar para a pessoa então receber o link."*
`orientacao/guia_do_portal.html` (fonte) → `Acesso ao Portal da Obra.pdf` e
`guia_portal_1.png`, mais `mensagem_whatsapp.txt`. Três etapas com quem faz
cada uma (VOCÊ pede → ESCRITÓRIO libera → VOCÊ cria a senha e entra), o que
fazer se der problema e os cuidados com senha e código. A página 2 (o que dá
para pedir ao DP) saiu. Textos conferidos contra as telas e o e-mail do
convite (código 15 min; link 7 dias, uma vez).

### 23/11/2026 (8) — Liberar o portal: o CPF vem do cadastro, a edição grava o CPF, e o recado diz o próximo passo

Ele, liberando da fila do portal uma pessoa com pré-cadastro: *"O CPF já era
para ele buscar e preencher automaticamente, não aconteceu"*; *"coloquei (…)
salvei, e o CPF não ficou preenchido"*; *"depois que eu liberei, eu não vi
informação nenhuma (…) Eu fiquei boiando."*
- Dois defeitos: `_liberar_acesso` copiava nome, e-mail e telefone do
  cadastro, mas não o CPF; e a edição do operador (`/erp/api/usuarios/<id>`)
  IGNORAVA o CPF — a tela mostrava o campo aberto, só o cadastro novo gravava.
  Os dois corrigidos (vazio não apaga; CPF inválido recusa).
- Ao salvar um operador, o recado diz em que pé está o convite: "próximo passo
  é dela: abrir o e-mail, criar a senha (vale até…)", "já criou a senha", ou
  "o convite venceu".
- **O alcance acompanha o ponto** (decisão dele no mesmo dia: *"o ideal era
  isso ser feito de forma dinâmica (…) A obra que aquela pessoa está batendo
  ponto, ele já teria acesso"*). `permissoes._obras_designadas` soma às obras
  marcadas a obra atual (`colaborador_obra_atual`, recalculada todo dia) do
  cadastro LIGADO ao operador — na mesma consulta de projeto e empresa, sem
  ida a mais ao banco. Mudou de obra no ponto, o alcance muda; as marcadas à
  mão ficam. Vale para todo operador que enxerga por obra e tem cadastro de
  colaborador ligado. A tela de operadores ainda NÃO mostra essa obra que vem
  do ponto (só as marcadas).

### 23/11/2026 (7) — Checklists enxutos; cópia em "Enviados" e "Reenviar o mesmo e-mail"

**Checklists**, revisados com ele item a item: ficam só com o que o sistema
não acompanha. Desligamento de carteira: só "Conferir as faltas no espelho do
ponto" (eram os 11 do Pipefy — o resto é passo do processo). Ele: *"Não
existe sistema para ativar e desativar (…) quando você coloca rescisão [no
SomaPay], automaticamente ele desativa (…) O que precisa é lançar o
pagamento"* — que já é o passo "Rescisão (TRCT) lançada e paga". E: *"basta
informar a quantidade de faltas na rescisão"* — sem espelho anexo nem malote.
Todo item de "sistema de pagamento" saiu de todos os tipos; o que o
atendimento já grava (cadastro, dependente, alteração) também. Readmissão
ficou sem checklist. Só vale para pedidos abertos daqui em diante; se ele
tiver ajustado um tipo na tela "⚙️ Passos de cada tipo", o dele vale.

**E-mail.** Ele: *"os e-mails que eu envio via sistema, eles não ficam salvos
no e-mail (…) não estou localizando (…) se de repente eu precisar reenviar,
eu não tenho essa opção."*
- O SMTP só entrega; quem guarda em "Enviados" é o programa de e-mail. Agora
  o ERP grava a cópia na pasta de enviados por IMAP (ver `HISTORICO.md`,
  23/11, registro entre áreas). Condição: a caixa de e-mail configurada no
  ERP (a que o agente lê) tem de ser a MESMA conta que envia. Se não for, o
  envio sai igual e a mensagem diz que a cópia não ficou e por quê.
- A caixa do DP é da **Locaweb**, a MESMA conta para enviar e receber (ele
  confirmou). Ela tem duas pastas de enviados: `INBOX.enviadas` (a do
  webmail, onde ele procura) e `INBOX.Sent` (criada pelo Thunderbird). A
  escolha da pasta prefere o nome em português; depois a marcada como
  enviados; depois nomes conhecidos.
- **"Reenviar o mesmo e-mail"** no pedido já enviado (na solicitação e na
  tela de Contabilidade): mesmo texto, anexos e destinatários; o prazo de
  volta não muda (reenviar não é cobrar); fica "REENVIADO" na trilha.
- ⚠️ O que eu NÃO verifiquei: qual provedor e qual conta ele usa. Os dois
  e-mails que ele não achou podem ter saído pela conta da EMPRESA (a volta
  automática quando a conta do DP não está configurada, 18/11) — aí estão
  nos enviados de outra caixa. Ele perguntou *"que caixa da empresa é essa?
  (…) Fica alguma coisa em sistema?"*: desde então a trilha do pedido diz
  "enviado DE <conta> PARA <contabilidade>", e a janela do e-mail mostra a
  trilha ("O que aconteceu com este e-mail"). Os envios anteriores não têm o
  "de" — a trilha deles só diz para quem foi.

### 23/11/2026 (6) — O e-mail à contabilidade se lê, se corrige, ganha anexo e sai da própria solicitação

Ele: *"Eu clico [montar o pedido], aí eu vou para outra tela ainda de
contabilidade para poder enviar (…) é para eu ler o e-mail, ver se eu
concordo, para poder enviar (…) permitir até anexar alguma coisa"*; *"todas
as outras solicitações (…) precisam trabalhar da mesma forma (…) A gente quer
velocidade e agilidade."* E sobre os nomes: *"como o que foi mandado, se nem
foi mandado ainda (…) E aí a situação rascunho. Não fica estranho?"*

- **Montar o pedido abre o e-mail na hora**, numa janela por cima da
  solicitação: para quem vai, assunto e texto EDITÁVEIS, o que vai junto
  (os documentos assinados pela pessoa entram sozinhos), "Anexar arquivo…",
  "Enviar à contabilidade" e "Guardar o texto e enviar depois". Vale para
  todo tipo que gera pedido (desligamento, admissão, férias, alteração,
  afastamento…) — um botão só, `verPedidoContabil`. O link "abrir em
  Contabilidade" saiu; o passo agora diz "Ler e enviar o e-mail PC-…".
- Servidor: `contabilidade.editar_rascunho`, `anexar_ao_rascunho` (o arquivo
  fica também no cadastro da pessoa, tipo "Outro documento do colaborador"),
  `tirar_anexo`; `enviar` manda os anexos extras junto; ações `editar`,
  `anexar`, `tirar-anexo` na rota do pedido, que passou a conferir o escopo
  também nas ações. Os extras ficam em `dp_pedidos.dados.anexos_extras` —
  sem migração. Depois de enviado, o texto não muda mais.
- **Nomes:** "RASCUNHO" virou "Ainda não enviado" em todas as telas; o bloco
  "O que foi mandado" diz "O e-mail que vai sair (ainda não foi enviado)"
  enquanto não sai.
- O agente no modo SUGERE continua montando o pedido sozinho; quem envia é o
  DP, agora da própria solicitação — não precisa ir à tela do agente.

### 23/11/2026 (5) — Atender a admissão: sem matrícula, cargo e convenção da lista, piso sozinho; rescisão de quem veio sem vínculo

Ele, atendendo uma admissão de verdade: *"a matrícula é para ser o número do
CPF e não é nem para aparecer aqui (…) a convenção precisa ser uma lista
selecionável (…) o cargo já é para ele identificar dentre os cargos já
cadastrados e automaticamente, quando eu colocar a convenção, ele já vai
buscar o salário."*

- A matrícula saiu da janela de Atender (só da TELA: o servidor continua
  gravando o CPF quando ela está vazia, e na readmissão a antiga fica).
- Cargo e convenção viraram listas do cadastro (`/erp/api/cargos`); o cargo
  vem marcado com o cargo pleiteado pela obra. Escolhidos os dois, o salário
  recebe o piso daquele cargo naquela convenção, com a explicação embaixo; se
  o cruzamento não existe ou está sem piso, a explicação diz isso e manda
  para Administração › Cargos e convenções. O DP pode mudar o salário.
- Defeito, no mesmo dia: "Montar o pedido à contabilidade" da rescisão
  travava com "falta (…) forma do desligamento" para quem foi importado sem
  histórico de vínculo — a forma só era procurada lá. Agora a forma escrita
  no pedido também vale (`contabilidade.o_que_falta_para_pedir(pedido=)`).

### 23/11/2026 (4) — Convenções: quem está em cada uma, excluir e a planilha modelo

Ele, antes de importar a tabela completa: *"eu queria um exemplo da planilha
de como eu devo importar os dados (…) visualizar quem são os colaboradores
para cada convenção (…) se todas que foi cadastrada tem alguém ou se não.
Inclusive, até excluir alguma que não precise estar aqui."*

- **Quadro "Convenções"** no topo de Administração › Cargos e convenções:
  UF, mês da data-base (marca "falta" quando vazio), cargos (e quantos sem
  piso), pessoas ativas (clicável: nome, cargo, obra, vínculo — o nome abre o
  cadastro) e desligados. Uma consulta agrupada.
- **Excluir…** com três travas (`cargos.excluir_convencao`): com gente ativa,
  RECUSA (troque a convenção das pessoas primeiro); só com desligados ou
  histórico de vínculo/alteração, DESATIVA (some das listas, o histórico de
  quem saiu continua dizendo a convenção); nunca usada, APAGA com os cargos e
  pisos dela. Fica registrado na auditoria.
- **"Baixar a planilha"**: o modelo JÁ VEM com o que está cadastrado — ele
  completa e importa de volta. Colunas: Cargo, CBO, Convenção, UF, Mês da
  data-base, Categoria, Salário, Valor diária, Status; aba "Como preencher".
  A importação passou a ler UF e mês da data-base (o mês alimenta o alerta
  dos 30 dias antes da data-base, Lei 7.238). A planilha do Pipefy continua
  aceita.
- Importar **não apaga** nada que não esteja na planilha — tirar convenção é
  só pelo Excluir.

### 23/11/2026 (3) — Plano B: o papel assinado que chega ao escritório

Ele: *"e se de repente aconteceu uma circunstância que a pessoa assinou num
papel e encaminhou para cá para a gente anexar? (…) Isso vale para outras
circunstâncias de sistema também (…) a obra (…) não tem internet (…) a gente
tem que ter uma alternativa, plano B."*

**O que já havia:** pelo portal, a OBRA tinha três saídas para o documento
que a pessoa assina — assinar na tela (PIN), mandar a foto do papel, ou
registrar a recusa com duas testemunhas. O que faltava era o lado do
ESCRITÓRIO: o papel que chegou por malote, mão ou e-mail não tinha por onde
entrar — só pelo celular da obra.

**O que entrou** (`pessoal_solicitacoes.papel_no_escritorio`, ações
`termo_papel` e `devolucao_papel` da rota do DP):
- **"Chegou no papel? Anexar…"** em todo documento para assinar ainda aberto
  — aviso prévio, pedido de demissão, aviso de férias, autorização de
  desconto. Entra como "assinado no papel" e JÁ VALIDADO (quem anexa é o DP,
  olhando o papel); o processo segue igual: a rescisão à contabilidade é
  montada, as férias fecham.
- **"Chegou no escritório? Anexar…"** no que a obra devia devolver pelo
  portal — documentação rescisória assinada e ASO demissional. A pendência
  fecha (o agente para de cobrar a obra) e o arquivo vai para a validação.
- O arquivo fica no cadastro da pessoa marcado "recebido no escritório",
  separado do que veio pelo portal.

**Decisão de detalhe:** o papel substitui o documento do sistema enquanto ele
está ABERTO. Se já foi assinado na tela e o papel chegar depois, o papel vai
para a ficha como documento a mais — não troca o assinado, que tem lacre.

**Outros planos B que já existiam** (conferidos agora): o DP abre a
solicitação pela tela dele quando a obra não consegue; o passo que foi feito
por fora tem "feito por fora do sistema…"; o ponto guarda batida sem
internet; a pessoa sem PIN assina no papel pela obra.

### 23/11/2026 (2) — O atendimento do desligamento chega preenchido

Ele, ao atender um pedido de demissão de verdade: *"As faltas a descontar, eu
gostaria que já fosse coletada do ponto a título de sugestão. Do dia 26 do mês
anterior em diante."* E: *"tem campo redução de jornada e cumpre aviso, mas é
campo de texto livre, não deveria ter opções?"* E sobre as datas: *"se você
souber, você já calcula a partir da data do aviso."*

- **Defeito, não escolha:** "redução de jornada" e "cumpre aviso" têm opções
  desde 20/11, mas as opções só eram mandadas para os campos do DP do TIPO,
  e esses dois só existem no ramo CTPS do desligamento. Agora vão as dos
  ramos também (`_campos_do_dp_todos`).
- **A janela de Atender chega preenchida** (`dp_analise.sugestoes_do_desligamento`,
  rota `GET .../sugestoes`, só quando o DP aperta Atender): último dia e saída
  pela forma — trabalhado (30 dias; saída = projeção de 30 + 3/ano, até 90),
  indenizado (último = aviso; saída = projeção), pedido de demissão (30 dias,
  nunca proporcional; dispensado = sai no dia), fim de experiência (o fim do
  contrato), pedido durante a experiência (sem aviso). Cada campo traz a conta
  por escrito embaixo. O DP muda o que quiser.
- **Faltas pelo espelho do ponto**, a mesma conta do fechamento
  (`fechamento.falta_do_dia`): do dia seguinte ao corte (o parâmetro do ponto,
  25 hoje) até o último dia — ou até ontem, se ele ainda não chegou, e a
  explicação avisa para conferir de novo. O número é só de DIAS INTEIROS sem
  justificativa; as horas a menos aparecem na explicação, fora do número.
  Custo: um espelho de uma pessoa (~6 consultas), só no clique.
- Redução de jornada no pedido de demissão: a explicação diz para deixar em
  branco (art. 488 é de quem foi dispensado). Isto é a leitura majoritária;
  não travei nada.
- **"Ver o ponto de MM/AAAA"** dentro da janela de Atender (um botão por mês
  do período das faltas). Primeiro saiu como a folha de impressão numa aba
  nova; ele corrigiu no mesmo dia: *"era verificar a tela de sistema e não
  baixar o arquivo (…) abrir outro modal com o ponto da pessoa, da mesma forma
  que a gente vê o ponto lá dentro de ponto."* Agora é a FICHA ÚNICA do ponto
  (`/erp/ponto/pessoa/<id>?aba=mes&competencia=`), a mesma que Colaboradores
  mostra, numa janela por cima da de atender — dá para abonar e lançar
  atestado ali. A rota da folha de impressão saiu. Para isso, a ficha do ponto
  passou a aceitar `?aba=` e `?competencia=` (uma linha em `gestao.html`,
  arquivo do PONTO — registrado lá).

### 23/11/2026 — O guia do Portal da Obra para liberar o acesso

Ele: *"preciso liberar as solicitações do DP, o acesso do portal (…) para os
administrativos de obra (…) um mini manual com orientações para o cadastro no
portal, bem como instruções de como utilizar a parte de solicitações (…) o
pessoal já estava antes acostumado com o Pipefy."*

- `app/apps/erp/orientacao/`: `guia_do_portal.html` (a fonte),
  `Guia do Portal da Obra.pdf` (A4, 2 páginas), `guia_portal_1.png` e `_2.png`
  (as mesmas páginas como imagem, para o WhatsApp) e `mensagem_whatsapp.txt`.
  `gerar_guia.js` refaz PDF e imagens a partir do HTML (Playwright, Node).
- Conteúdo conferido contra as telas: os nomes dos botões ("Criar o meu
  acesso", "Mandar o código", "Confirmar"), as quatro áreas do portal
  (Pagamentos ainda fechada), os 15 pedidos e os rótulos de situação.
- ⚠️ **Mudou a tela, muda o guia** — editar o HTML e rodar o gerador.

### 23/11/2026 — (feito pelo chat do PONTO) Os documentos do DP passam a ser assinados pelo aplicativo do ponto

⚠️ **Mexeu em arquivo desta área sem ser o chat dela, a pedido do dono** (*"faça por aqui mesmo (…) e avisa ao chat
do DP"*; avisado no mesmo dia). Detalhe em `app/apps/ponto/HISTORICO.md`, 23/11/2026.

- **`core/termos.py`** (`dp_termos`) virou a base da CENTRAL DE ASSINATURAS: a pessoa assina o próprio documento,
  logada no "Meu ponto" (`assinar(..., pela_pessoa=True)`; a regra "quem conduz não assina o próprio" continua para
  quem conduz); `abrir(..., origem, origem_ids)`; `pendentes(..., colaborador_id=, pessoas=)`; `da_pessoa`,
  `painel_dp`, `da_origem`. Nada do que já existia mudou de comportamento: o atendimento, o portal da obra e a
  validação continuam iguais.
- **Migração 126** (`126_termos_origem.sql`): `origem`, `origem_ids`, `assinado_pela_pessoa` e o tipo
  TERMO-COMPENSACAO no Arquivo. **A próxima livre é a 127** (124 e 125 são de Suprimentos).
- **Documento novo:** o termo de acordo de compensação nasce sozinho da compensação aprovada no ponto.
- **Onde o DP gere agora:** ERP › Ponto › Assinaturas (pendentes, a validar, assinados) — inclusive os documentos que
  nascem aqui, no atendimento (aviso prévio etc.).

### 21/11/2026 (2) — O agente cobra e lê o e-mail sozinho; o ASO alerta

Ele, depois de entender as duas rotinas: *"sim, vamos deixar as rotinas
automáticas. E quanto ao ASO, vamos alertar, só não vai ser obrigatório por
enquanto."*

- **Migração 123** (era 120; renumerada porque o chat de Suprimentos usou 120–122 no mesmo dia): cobrança e leitura do e-mail em **FAZ**. A cobrança manda
  e-mail à contabilidade quando o pedido vence (até o teto; depois escala para
  o DP) e WhatsApp à obra pela devolução vencida; não cobra no fim de semana.
  A leitura liga a resposta ao pedido, arquiva o anexo e marca respondido — não
  conclui. Rodam uma vez por dia. ⚠️ A leitura depende da conta de e-mail do
  DP configurada no ERP — não verificado daqui.
- **ASO admissional:** a análise do pedido passou de lembrete a ATENÇÃO (⚠️)
  quando não veio; a lista de pendências avisa quando a admissão começa em até
  2 dias sem ASO. Continua sem travar.
- O mapa (artefato "Mapa do Pessoal") trocou "vigia" por "lista de
  pendências", que é o que ele entende.

### 21/11/2026 — O ASO entra sem travar; folha e 13º para depois

Ele, sobre os buracos do mapa (artefato "Mapa do Pessoal"): *"fechamento de
folha (…) ainda vai integrar a parte do ponto com os pagamentos, então não vou
mexer ainda (…) 13º salário é uma vez por ano, também não precisa. Exame
admissional e demissional: a gente pode deixar preparar para receber ele junto
com os demais documentos (…) por enquanto sem travar. O demissional já pode
ficar como cobrança do retorno junto com a documentação assinada rescisória,
mas por enquanto não é obrigatório."*

- **ASO admissional:** a leitura por IA reconhece o ASO entre os documentos da
  admissão; o portal mostra "ASO admissional — opcional por enquanto"; a
  análise do pedido lembra quando não veio. Nada trava.
- **ASO demissional:** ao mandar a documentação rescisória para a obra
  assinar, abre-se junto uma pendência "Exame demissional (ASO)", devolvida
  pelo portal e cobrada como as outras. Passo próprio no desligamento, marcado
  opcional: não entra no "falta" nem segura o encerramento.
- **Fechamento da folha e 13º:** ficam para depois, por decisão dele.

### 20/11/2026 (noite, 7) — As regras dele na tela de Regras do cadastro

Comentários dele sobre a medição:
- *"Autônomo não tem convenção. Apenas se define o valor da diária."* → a
  convenção SAIU do obrigatório do prestador (tinha entrado por suposição
  minha em 18/11, sustentada só pelos 207 de 207 preenchidos — sobra do
  Pipefy); a convenção só aparece para CTPS; o autônomo RPA passa a exigir a
  diária. A medição registra a proposta antiga como recusada, com o motivo.
- *"CNPJ PJ e Razão Social PJ são obrigatórios para o Tipo de Contrato PJ"* →
  já valia no formulário; a medição parou de listá-los como "campos que
  ninguém preenche" (são poucos PJ, abaixo do mínimo da medição).
- *"se o colaborador sair e retornar num prazo de um ano, na readmissão o
  contrato tem que ser indeterminado (…) não entra em período de experiência"*
  → `pessoas.readmitir`: carteira → carteira em até 365 dias grava CLT por
  prazo indeterminado e "Sem contrato de experiência", com evento; a análise
  do pedido avisa; a medição não conta essas pessoas como discordantes.
  ⚠️ **A lei (CLT art. 452) fala em 6 meses**, não 1 ano; a regra da empresa é
  mais protetora e está dentro da lei. Dito a ele.

**Feito a pedido dele (*"quero que altere sozinho o contrato"*):** a rodada
diária troca "CLT (tempo determinado)" por "CLT (tempo indeterminado)" — no
cadastro e no vínculo aberto, com evento — de quem já passou do fim da
experiência (`pessoal_experiencia.virar_indeterminado`). Na primeira rodada
alcança o estoque (havia 353 "determinado"). Quem está "determinado" sem
contrato de experiência legível fica como está. O Análise de SPs trata os dois
CLT igual: não muda pagamento. Não vai à contabilidade (a experiência que
acaba vira indeterminado sozinha também no eSocial).

### 20/11/2026 (noite, 6) — A transferência vira AUTOMÁTICA: a obra do mês

Ele, desfazendo a Transferência de Obra como pedido (publicada horas antes):
*"Isso é um trabalho a mais para o DP (…) não quero que o DP tenha trabalho
numa coisa que o sistema pode detectar automaticamente (…) a gente vai
considerar um mês trabalhado (…) na obra que ela mais trabalhou. Porque isso,
em relação a cálculo de FGTS, INSS, é o que deve ser levado em consideração
(…) trabalhou 15 dias numa obra, 5 dias na outra (…) a obra que ela trabalhou
os 15 dias. E todo mês isso a gente faz essa alteração. Não vamos botar nada
para o DP."*

- `core/obra_do_mes.py` + migração **119**: conta DIAS trabalhados por obra no
  mês (dia com batida válida), vence a de mais dias (empate: a do dia mais
  recente). No começo do mês a rodada diária calcula o mês que acabou e troca a
  **obra de cadastro sozinha**; cada mês fica guardado com os dias de cada obra
  e a obra de antes — a base do relatório de lotação para a contabilidade
  (ainda por fazer). Uma vez por mês.
- **Escolha minha, a confirmar:** o mês é o do CALENDÁRIO (competência das
  guias). Se for o período do fechamento (26 a 25), troca-se num lugar só.
- A Transferência de Obra saiu da caixinha (tipo oculto) e parou de nascer do
  ponto; a migração 119 **cancela as que o sistema abriu sozinho** e ninguém
  atendeu.
- A obra ATUAL (últimos 15 dias, migração 118) continua: é onde a pessoa está
  agora — para onde vão os avisos do portal e o documento para assinar.

### 20/11/2026 (noite, 5) — Transferência de obra vai à contabilidade; a folha recebe a obra atual

Ele: *"O fechamento do ponto quem faz somos nós e enviamos à contabilidade
para receber de volta a folha. O que falta é criarmos os relatórios de
fechamento de folha ainda. Obra atual em análisesps alimenta obra atual do
colaborador (…) isso altera custo de obra, mas ainda vamos migrar toda a parte
de pagamento de colaboradores de analisesps pra dentro do ERP. A transferência
será informada também à contabilidade, pois impacta diretamente na geração
das guias de FGTS e tributos. Demanda outro relatório a ser criado também."*

- **Transferência de Obra** (tipo novo): nasce sozinha quando o ponto mostra a
  pessoa uma semana noutra obra (`obra_atual.abrir_transferencias`, uma por
  episódio), ou a obra/DP abre. Atender troca a obra de CADASTRO; de carteira,
  vai à contabilidade com obra de origem e de destino (código, CNO, município)
  e fecha com o retorno. Apoio temporário: o DP recusa. O alerta de "obra
  diferente do cadastro" virou esta solicitação.
- **A folha** (`pessoas.para_folha`, o contrato que o Análise de SPs vai ler):
  `obra_codigo` passa a ser a obra ATUAL (do ponto); `obra_cadastro`, a de
  cadastro.

**PENDENTE, dito por ele, ainda sem construir:**
- os **relatórios de fechamento da folha** (nós fechamos o ponto, mandamos à
  contabilidade, ela devolve a folha) — o ponto já gera planilha e arquivos
  `.ps`; falta o relatório que ele quer e o pedido com retorno;
- o **relatório de transferências** para a contabilidade (guias de FGTS e
  tributos por obra);
- a **migração do pagamento de colaboradores** do Análise de SPs para o ERP.

### 20/11/2026 (noite, 4) — O afastamento do ponto vira solicitação ao DP, e vai à contabilidade

Ele, sobre o alerta "afastamento aprovado no ponto que não chegou ao
cadastro": *"Não é para ser (…) um lançamento via ponto que gera um
afastamento. Eu acho que ele automaticamente gera uma solicitação do DP para
ser dado o devido tratamento junto à contabilidade (…) acidente de trabalho
também (…) Mesma coisa para maternidade (…) tudo que demandar ação do DP e
contabilidade no mesmo modelo."* E: *"afastamento e férias (…) pode deixar
que a contabilidade tenha o controle (…) eles utilizam o Fortes."*

- Tipos novos: **Comunicação de Afastamento** e **Retorno de Afastamento**
  (só carteira). Atender registra na ficha (se ainda não estiver) e vai à
  contabilidade, fechando com o retorno dela. A natureza (INSS, acidente,
  maternidade) o DP confirma ao atender — o ponto não a registra; a soma de
  atestados acima de 15 dias (que o ponto já abre sozinho) chega marcada INSS.
- **Ponte do ponto** (`core/ponte_do_ponto.py`): afastamento lançado no ponto
  desde 20/11/2026 vira a solicitação sozinho (ao abrir a fila do DP e na
  rodada diária); negado ou cancelado no ponto, a solicitação que não andou
  cai. Só LÊ o ponto.
- **Pela ficha também:** afastar ou registrar a volta direto no cadastro abre
  a solicitação, para nada ir à contabilidade por fora.
- **Acidente:** o e-mail pede a CAT (1 dia útil) e o agente cobra todo dia até
  o pedido sair. O alerta antigo "afastamento só no ponto" saiu (virou a ponte).
- **Não feito, de propósito (ele):** zerar férias por afastamento longo e
  contar benefício — é a contabilidade, no Fortes.

### 20/11/2026 (noite, 3) — Fim da experiência avisado à obra; obra atual pelo ponto

Ele: *"Prorrogação da experiência. Entendo que DP e o administrativo de obra
devam ser alertados (…) Poucos dias antes. Transferência de obra (…) teremos a
obra de cadastro e a obra atual. Para a obra atual, vamos buscar a informação
do ponto. Uma rotina que atualize essa informação."*

- **Experiência terminando** (`pessoal_experiencia.terminando`): 10 dias antes
  do fim da 1ª parte (que se prorroga sozinha) e da prorrogação (que vira
  indeterminado). O DP já recebia pelo agente; a obra passa a ver no portal
  ("Contratos de experiência terminando"), com o que acontece se ninguém fizer
  nada e como encerrar no prazo.
- **Obra atual** (migração 118, `core/obra_atual.py`): onde a pessoa mais
  bateu ponto nos últimos 15 dias; recalculada todo dia antes da rodada do
  agente (uma consulta só). A obra de cadastro continua sendo do DP. A ficha
  mostra as duas; cada mudança fica registrada (`colaborador_obra_mudancas`);
  uma semana numa obra diferente da do cadastro vira alerta de transferência
  no agente. Os avisos do portal (férias, experiência) e o documento para
  assinar vão para a obra ATUAL.
- ⚠️ **Não mexido, decisão dele:** a folha do Análise de SPs tem os campos
  "obra" e "obra do cadastro" (`pessoas.para_folha`), e hoje os dois recebem
  a obra do cadastro. Pôr a obra atual no primeiro muda o rateio do custo.
  Também em aberto: a transferência de quem é de carteira vai à contabilidade?

### 20/11/2026 (noite, 2) — Nome e endereço não vão à contabilidade; aviso de férias assinado na tela

Ele: *"Troca de nome e endereço não mandar pra contabilidade. Aviso de férias
em tela."*

- **Atualização cadastral** voltou a se resolver só no cadastro (desfeito o
  envio à contabilidade do lote anterior; o modelo "Atualização cadastral"
  saiu).
- **Aviso de férias na tela** (entendido como: a pessoa assina o aviso no
  celular, como o aviso prévio — se era outra coisa, ajustar): quando a
  contabilidade devolve as férias (ou a alteração), o sistema gera o **Aviso de
  férias** com as datas confirmadas e manda para a obra colher a assinatura
  (PIN da pessoa; ou foto do papel — inclusive do aviso que a contabilidade
  mandou). O pedido de férias **só fecha com o aviso assinado** (CLT art. 135:
  ciência por escrito, 30 dias antes).
- Conserto achado no caminho: com uma correção esperando a ciência da
  contabilidade, o retorno dela deixava de confirmar as férias. Agora confirma
  sempre; só o FECHAR espera a ciência.

### 20/11/2026 (noite) — A contabilidade como ponte; o aviso assinado; corrigir e cancelar com ciência; o agente ligado

O que ele explicou, e que muda o desenho de tudo (vale para os próximos chats):

> *"O que a contabilidade faz? Ela faz toda a gestão junto ao eSocial e cálculo
> da folha (…) hora extra, aplicação das faltas, cálculo das DSR (…) admissão,
> demissões (…) baseado nas informações que nós alimentamos. Na gestão do
> carteira assinada, nosso sistema funciona como uma ponte, mas tudo é fechado
> lá (…) E a gente requer sempre o retorno de tudo. Quem não é CLT, aí tudo se
> resolve internamente (…) ajuda de custo, de alimentação, de transporte também
> é conosco."*

> *"O DP serve para intermediar o que as obras demandam e a contabilidade (…)
> pode ser que venha um pedido incoerente (…) a ideia de futuro é que o DP
> apenas olhe e dê o ok (…) o agente junto com a contabilidade resolva tudo."*

**Regra que sai disso:** pedido de quem é CLT que muda o que a contabilidade
informa ao eSocial ou calcula na folha **vai a ela e espera retorno**; o resto
(prestador, ajuda de custo, telefone, Pix) se resolve aqui.

- **Desconto em folha** de CLT passou a ir à contabilidade
  (`contabil_processo`). (A atualização cadastral também foi, e ele desfez no
  mesmo dia — ver a entrada seguinte.)
- **Desconto em folha** exige a **autorização assinada** pela pessoa (CLT art.
  462) antes de ir à contabilidade.
- **Aviso prévio assinado** (`core/termos.py`, migração 117): o DP analisa e
  atende → o aviso (ou o pedido de demissão) é gerado com as datas aprovadas →
  vai para o portal da obra → a pessoa **assina com o dedo e o PIN dela** (o do
  ponto), como a folha de ponto; sem PIN, foto do papel; recusa, com duas
  testemunhas → o DP valida o papel/a recusa → **só então a rescisão vai à
  contabilidade, com o aviso anexo no e-mail**. Encerramento de experiência não
  tem aviso. Ele: *"o DP só confere e libera para a obra."*
- **Análise do agente em todo pedido** (`core/dp_analise.py`, regra escrita,
  sem IA): experiência (encerrar no prazo × romper antes, art. 479), aviso
  proporcional (Lei 12.506), último dia e data de saída com a projeção (Súmula
  380, OJ 82), a **data-base da convenção** (indenização adicional — Lei
  7.238/84; é o "paga em dobro" dele), afastamento e estabilidade; férias
  (período aquisitivo, início antes do descanso, fracionamento, 30 dias de
  aviso, prazo de concessão); redução de salário, piso, vigência retroativa;
  admissão no prazo do eSocial e menor de idade; peso da parcela do desconto.
- **Corrigir e cancelar depois de atendido** (`contabil_processo`): o pedido à
  contabilidade em rascunho é refeito; já enviado, nasce um pedido de
  **Correção** ou de **Cancelamento** a ela, e a solicitação **só fecha com a
  ciência**. A obra pode **pedir** para cancelar ou corrigir pelo portal; o DP
  faz. Desligamento continua desfeito pelo cancelamento de desligamento.
- **Controle de férias** (migração 117): até quando as férias de cada pessoa
  estão quitadas. As de antes do sistema o DP informa uma vez (tela Férias ›
  Períodos); as confirmadas no sistema quitam os períodos sozinhas. O agente
  avisa 90 dias antes do vencimento (férias vencidas = em dobro, art. 137).
- **O agente** (ele: *"Vamos botar para funcionar"*): análise, despacho,
  vigia e relatório ligados em SUGERE; **rodada diária sozinha** (fila de
  trabalhos, `trabalhos.agente_dp_dia`); vigia novo: fim da experiência em 10
  dias, férias vencendo/vencidas, documento parado na obra, pedido de mudança
  da obra parado, correção sem ciência, **afastamento aprovado no ponto que não
  chegou ao cadastro** (a ponte que ele aprovou, como alerta). A
  **conferência** saiu de "promessa": não liga até existir.
- **Defeito do lote da tarde corrigido:** o e-mail de alteração e de
  cancelamento de férias saía sem dizer o que se pedia (só os dados da pessoa).

⚠️ **Não verificado e a confirmar:**
- A assinatura com PIN não pôde ser testada de ponta a ponta aqui (o banco de
  teste local não tem o ponto); a conferência do PIN é a mesma função do ponto.
- **A validade jurídica** da assinatura no celular (Lei 14.063/2020) é a mesma
  base da folha de ponto — falta o advogado confirmar, como lá.
- A cobrança e a leitura da caixa do agente **continuam desligadas** até ele
  decidir (mandam e-mail/WhatsApp).
- O texto do aviso usa a razão social do cadastro da empresa; pessoa sem
  empresa sai "A empresa".

**Procedimentos que faltam** (levantados a pedido dele, ainda não construídos —
ver a resposta do dia): prorrogação/efetivação da experiência com termo
assinado; advertência e suspensão; transferência de obra; ASO demissional no
processo de desligamento; fechamento mensal do ponto para a folha (área do
ponto); afastamento formal com a contabilidade (a ponte do ponto); 13º salário
(1ª parcela até 30/11); pensão alimentícia por ofício.

### 20/11/2026 (tarde) — Programação de férias; alterar e cancelar férias; o cancelamento de desligamento sempre avisa a contabilidade

Ele: *"o sistema precisa ter a programação dessas férias (…) a gente pede à
contabilidade, confirma o que foi registrado. O sistema registra. Lembrando
que tem que pensar na possibilidade de alteração e cancelamento."* E do
cancelamento de desligamento: *"Imagina desligar uma pessoa que vai continuar
trabalhando. É uma falha grave (…) todo mundo envolvido tem que estar
avisado."*

- **Migração 116 + `core/ferias.py`:** a programação de férias. Atender o
  pedido de férias cria a linha "aguardando a contabilidade"; o retorno da
  contabilidade a confirma. Tela nova **Pessoal › Férias**.
- **Alteração e Cancelamento de Férias** viraram tipos de pedido (portal e
  tela Férias). Passam pela contabilidade; **a data só muda com o retorno**
  — até lá a linha mostra a mudança pedida ao lado da data que vale.
  Férias marcadas antes do sistema (sem linha) não travam a alteração: a
  linha nasce no retorno.
- **Avisos de férias chegando:** no portal, "Férias da equipe" para quem
  alcança a obra (15 dias antes e durante); no agente, férias chegando (7
  dias), sem confirmação a 30 dias, mudança sem retorno, pedido de férias
  ainda não atendido.
- **Cancelamento de desligamento:** de carteira, **sempre** vai à
  contabilidade (antes, só se a rescisão já tinha sido enviada). Pode ser
  pedido em cima do desligamento — botão no portal ("O que você já pediu") e
  na tela de Solicitações. Quem pediu o desligamento lê no portal que ele foi
  cancelado. **A virada do dia segura** quem tem cancelamento pedido e não
  atendido, e o DP pode atender depois da data se o pedido foi aberto antes
  dela. O agente cobra todo dia até a contabilidade confirmar.

⚠️ **O que ficou de fora, e é decisão dele:**
- **O ponto corta o acesso pela data de saída**, sozinho (é do app do ponto,
  outra área). A trava da virada protege o ERP; o ponto, não. O agente avisa
  "atender hoje".
- **Férias confirmadas não viram lançamento de férias no ponto.** Hoje o DP
  lança à mão no ponto; o agente lembra 7 dias antes. Ligar os dois é mexer
  no ponto.
- Aviso por WhatsApp/Telegram das férias chegando: hoje o aviso é na tela
  (portal e agente).

### 20/11/2026 — Alteração de cargo, dependente, férias e cancelamento passam pela contabilidade; admissão de prestador também

Ele: *"A alteração de cargo (…) tem todo o procedimento de encaminhamento
para contabilidade, acompanhamento, retorno, o agente tem que acompanhar. A
mesma coisa vale para o dependente (…) salário de família (…) E a mesma coisa
serve para as férias. Se as férias não estão em solicitações, tem que entrar
também, porque o administrativo de obra pode pedir as férias. E o DP tem que
fazer a mesma coisa, solicitar a contabilidade, aguardar retorno e etc. Para
então finalizar."*

- `core/contabil_processo.py` (novo): o mesmo desenho da admissão. Atender
  **não fecha**: o pedido fica **Em andamento** com três passos — DP atendeu →
  pedido à contabilidade montado (botão "Montar o pedido à contabilidade", que
  já preenche com o que veio da obra) → contabilidade devolveu. **Fecha
  sozinho** quando o pedido à contabilidade é concluído
  (`contabilidade.concluir` chama `fechar_pelo_pedido`).
- Vale para: **Alteração de cargo/salário**, **Dependente** (só quando marca
  salário-família ou dependente de IR — dependente só para o plano não vai à
  contabilidade), **Férias** e **Cancelar desligamento** (este só quando já
  havia pedido de rescisão enviado; rascunho é só cancelado).
- **Férias virou tipo de solicitação** (`FERIAS`), aberto pelo portal da obra
  e pelo DP: início, quantos dias, vender 1/3, adiantar o 13º, observação. Só
  para carteira.
- **Alteração de cargo não alterava o cargo** (defeito encontrado na revisão):
  os nomes dos campos do pedido não batiam com os da ficha. Corrigido; cargo
  deferido vale sobre o pleiteado.
- **Cancelar desligamento passou a fazer algo:** limpa demissão, aviso e
  último dia do cadastro e do vínculo, cancela o desligamento em aberto e
  recusa se a pessoa já saiu (aí é readmissão).
- **Admissão de prestador (prestador → CTPS):** o cadastro volta para Inicial
  e o pedido fica em andamento até o registro da carteira voltar da
  contabilidade, igual à admissão comum. Antes fechava no atendimento sem
  registro nenhum.
- **O agente vigia, com os prazos da lei:** registro sem pedido à
  contabilidade a 2 dias do início (eSocial exige antes do 1º dia); férias sem
  aviso a 30 dias do início; pedido em andamento parado; rescisão sem retorno
  depois de 10 dias da saída (prazo de pagamento).

**Não feito, e é decisão dele:** o pagamento das férias como passo (o "e etc."
dele) — hoje o processo termina no retorno da contabilidade.

### 19/11/2026 (noite, 4) — A admissão caminha dentro da Solicitação; "Assinar" sai do caminho do PJ

Ele: *"as solicitações, elas devem ser tratadas em solicitações. O passo a
passo (…) ela vai avançando dentro da parte de solicitações, até a conclusão
(…) Quando uma admissão for concluída (…) é o momento dela sumir."* E as
respostas que fecharam o desenho: fecha **sozinha** quando o cadastro chega a
Concluído; os pedidos já atendidos e não concluídos **voltam à fila**; o passo
do PJ é **anexar o contrato de prestação**, *"não tem nada a ver com assinar"*.

- `core/admissao_processo.py`: atender a admissão só fecha o pedido se o
  cadastro já puder ser concluído (prestador comum conclui na hora). Senão,
  **Em andamento**, com os passos: CTPS → pedido de Assinatura de CTPS →
  registro de volta; PJ → contrato anexado; e "Cadastro concluído".
- **Fecha sozinho:** `pessoas.mover_etapa` chama `fechar_concluidas` quando a
  etapa vira Concluído — por qualquer caminho (retorno da contabilidade, DP
  concluindo na ficha).
- **O quadro acompanha:** abrir o pedido de Assinatura de CTPS leva o cartão
  de Inicial para Contabilidade.
- **PJ não vai mais para "Assinar":** vai para Concluído, e a trava de
  concluir exige o contrato anexado (sem ele, fica na Inicial com o recado). A
  etapa "Assinar" continua existindo para quem já está nela.
- **Migração 115:** pedidos de admissão "Atendida" cujo cadastro está em
  Inicial, Assinar ou Contabilidade voltam a "Em andamento", com um evento
  dizendo por quê. Ele perguntou de onde saem: é exatamente essa consulta.
- **Nomes:** a tela virou "Solicitações"; a etapa "Solicitações" virou
  "Contabilidade" e foi para antes de "Concluído".

**Próximo passo combinado:** quando as pendências antigas do Pipefy acabarem,
"Assinar" e "Contabilidade" saem do quadro (só com o "pode" dele).

### 19/11/2026 (noite, 3) — Dinheiro no padrão do Brasil; a pessoa do pedido individualizada; "quem pediu"

- **Valor com vírgula e milhar** enquanto se digita no portal (123456 →
  1.234,56) e "R$ 1.234,56" na tela do atendimento.
- **"O que você já pediu" (portal) e a fila do DP mostram nome, CPF, obra e
  cargo.** ⚠️ O nome vinha só do cadastro: a admissão sem cadastro ligado
  ficava em branco, mesmo com o nome no pedido.
- **"Pediu" virou "Quem pediu"** e ⚠️ vinha VAZIO para o DP em pedido aberto
  pelo portal: o autor era guardado como colaborador e a coluna lia só o
  usuário. Agora lê os dois, e o portal grava também o usuário. Pedidos
  antigos de quem não tem ficha de colaborador continuam sem autor (não havia
  o dado para recuperar).

### 19/11/2026 (noite, 2) — Readmissão enxuta; a tela do atendimento legível e editável

Testando a readmissão, ele foi item por item:
- **Checklist da readmissão:** de sete itens para um ("Conferir os dados e os
  documentos"). Saída anterior, admissão, obra, cargo, convenção, matrícula,
  ponto e pagamento — tudo já resolvido pelo sistema ou inaplicável.
  ⚠️ Pedido JÁ ABERTO guarda o checklist antigo (é copiado na abertura); vale
  para os próximos.
- **Readmissão de prestador vai para Concluído** ao atender (o ponto reativa
  sozinho). De carteira vai para Inicial (falta o registro da CTPS). ⚠️ Antes
  ia para "EM_CONFERENCIA", etapa fora de circulação — a pessoa sumia da fila.
  A saída anterior sai do cadastro (fica no histórico de vínculos).
- **Matrícula na readmissão:** só vira o CPF se estiver em branco; a que já
  existia fica.
- **O pedido em linguagem de gente** (`dados_legiveis`): nome do cargo, da
  empresa e da obra; rótulo da opção; dd/mm/aaaa; CPF, CNPJ, CEP e telefone
  pontuados.
- **"Corrigir os dados"** na tela do atendimento: os mesmos campos da
  abertura, preenchidos; no pré-cadastro a correção vai para o cadastro junto;
  rastro de/para no histórico do pedido.
- **Documento abre numa janela** por cima do pedido, com Baixar e "abrir em
  outra aba".

### 19/11/2026 (noite) — A admissão nasce com PRÉ-CADASTRO; documentos à vista; caixa alta

Ele testou a admissão publicada e cobrou: *"eu pensei que a pessoa já
apareceria na fase inicial (…) Se eu atender (…) gera o cadastro inicial? Não
tem sentido. (…) conferir se a documentação veio completa, mas cadê a
documentação? (…) não existe um cadastro ou pré-cadastro dessa pessoa já."*

- **A pessoa nova nasce no cadastro NA HORA DO PEDIDO**, etapa **Inicial**, com
  os dados e os papéis. Atender passa a ser completar o que é do DP
  (matrícula, convenção, cargo, salário); recusado ou cancelado, o
  pré-cadastro vai para **Cancelado**. A readmissão continua como era: volta à
  ativa só quando o DP atende.
- **A tela do pedido mostra os documentos enviados** (com link para abrir) e o
  link para o cadastro.
- ⚠️ **Defeito achado:** o atendimento passava o pedido ao cadastro com quatro
  nomes diferentes — vínculo, cargo, CTPS e PIS se perdiam (o prestador nascia
  CTPS e sem cargo). `para_o_cadastro` traduz.
- **Caixa alta e sem acento** em nome, mãe, pai, órgão, endereço e
  naturalidade — lido ou digitado. Ele: *"tudo caixa alta, que na leitura da
  IA fosse removido a acentuação."*
- **Busca de quem volta mostra o CPF inteiro.** Ele: *"o que individualiza a
  pessoa é o CPF."* (As outras buscas do portal continuam com o fim do CPF.)
- **Telefone com máscara** (85) 98888-7777 no portal; o banco guarda só os
  números.
- **Prestador esconde CTPS e PIS**; o que a leitura achar continua sendo
  guardado.
- **A leitura grava o que achar além do formulário** — pai, sexo, estado civil,
  naturalidade, série/UF/emissão da CTPS, PIS (`EXTRAS_DA_LEITURA`). Órgão
  emissor fora da lista do cadastro não é gravado (fica para escolher).
- **Um PIS inválido não trava a admissão**: fica no pedido e não entra no
  cadastro.

⚠️ **O pedido que ele já abriu antes desta versão** não tem pré-cadastro: ao
atender, o cadastro nasce do jeito antigo (agora com o vínculo e o cargo
certos), e os papéis passam para a pessoa.

### 19/11/2026 (noite) — "Não recebe" esconde o valor; o PIS deixa de ser obrigatório

- *"Quando coloca ajuda com alimentação, ajuda com transporte, não recebe,
  oculta o campo valor."* Feito na admissão e no pedido de ajuda de custo
  (`ocultos_se`); o desconto do VT some junto.
- *"O PIS (…) pode fazer a leitura, se encontrar, mas não tornar item
  obrigatório, porque o cadastro social é feito com o CPF."* Saiu de TODOS os
  lugares que o exigiam: a admissão (CTPS), a admissão de prestador, o "falta
  para admitir" da ficha, e o pedido de Assinatura de CTPS à contabilidade. A
  medição de regras não volta a propor (`RECUSADAS`). O campo continua, e a
  leitura dos documentos o preenche quando acha.

### 19/11/2026 (tarde) — "Cadastro de Colaborador" vira ADMISSÃO DE COLABORADOR

**O pedido, com as palavras dele:** *"eu quero trocar por admissão de
colaborador. Porque dessa forma, eu tanto atendo quem não tem cadastro conosco,
quem já teve cadastro conosco. A gente precisa atender no cadastro as
exigências do E-Social e os dados mínimos para o SomaPay."*

**Os campos obrigatórios, a lista é dele:**

| Para quê | Campos |
|---|---|
| eSocial | nome, nome da mãe, data de nascimento, CPF |
| SomaPay | RG, data de emissão, órgão emissor, estado de emissão, data de início, telefone, tipo de contrato, **matrícula = CPF** |
| Interno | endereço (CEP, rua, número, bairro, cidade, estado; complemento opcional) |

Moram em `pessoal_solicitacoes.CAMPOS_DA_PESSOA`, com o nome das COLUNAS do
cadastro (o atendimento copia sem tradução). Valem também na **Admissão de
Prestador** (virar CTPS é entrar no eSocial e no SomaPay) — lá, o que a ficha
já tem não se pergunta.

**Matrícula = CPF:** só para quem entra daqui em diante (admissão e
readmissão). Decisão dele: *"Não mudar os antigos."* O DP pode digitar outra
no atendimento; a dele vence.

**Os documentos: uma área só, vários arquivos, uma leitura.** Ele: *"esse
campo deveria ser um campo onde você pode jogar o documento dentro (…) a
pessoa coloca o RG e o RG é uma imagem da frente e uma imagem do verso."*
Arrasta (ou tira foto, ou escolhe vários); o lote vai numa **única chamada de
IA** (`documentos_de_pessoa.ler_varios`), que diz o que cada arquivo é e
preenche os campos vazios. Um arquivo pode cobrir dois papéis (a CNH é
identificação E CPF). O que a leitura não reconhecer, a pessoa diz numa
caixinha. Todos os campos ficam à vista para conferir; o que não foi lido
fica para digitar, e o pedido não sai sem.

**Quem já teve cadastro:** ele: *"se tratar-se de um funcionário que já tenha
cadastro, o sistema precisa cruzar também e informar."*
- Pelo CPF digitado (como já era) **ou** pela busca nova "Já trabalhou
  conosco?", que procura só desligados, em qualquer obra.
- Aviso na tela: o cadastro já existia, segue como readmissão, o histórico
  fica, e o que vier agora atualiza o cadastro.
- **Documento opcional** na readmissão (decisão dele); o que o cadastro antigo
  já tem some da tela ("já está no cadastro"), e só o que falta é pedido —
  *"a obra digita na hora? Sim!"*.
- No atendimento, os dados novos atualizam o cadastro antigo (só o que veio;
  vazio não apaga), com o de→para na trilha.

⚠️ **Dado pessoal não desce para o celular.** A busca de pessoa devolve o RG,
a mãe e o endereço só como a marca "tem" (`SO_A_MARCA`, `MARCA_TEM`): quem
procura um nome não lê o documento de outra pessoa. O valor fica no servidor,
que o junta ao pedido.

#### ⚠️ O DEFEITO MAIS GRAVE ACHADO NO CAMINHO: os papéis nunca eram guardados

O portal **lia** cada documento anexado e mandava ao servidor só a **lista de
tipos** ("anexei o RG"). O arquivo ficava no celular. A coluna para isso
(`documentos.dp_solicitacao_id`, migração 100, de 10/11/2026) existia e
ninguém escrevia nela. Ou seja: **desde o começo, o DP recebia o pedido de
cadastro sem papel nenhum** — e a regra "não sai sem os documentos" conferia
uma lista de nomes, não arquivos.

Agora o pedido e os arquivos vão **na mesma requisição** e são gravados na
mesma transação: pessoa que já existe recebe o papel direto no cadastro;
pessoa nova recebe quando o DP atende (`transferir_papeis`).

**De passagem:** abrir pedido no portal com um login SEM ficha de colaborador
(gente de escritório, o próprio administrador) dava erro de sistema — o evento
de auditoria ficava sem dono. Consertado.

**Custo, contado:** a leitura é **1 chamada de IA por lote** de até 8 arquivos
(antes: 1 por arquivo). O envio é 1 requisição com tudo. Guardar cada arquivo
é 1 gravação no banco por arquivo.

**Exercitado no navegador** (banco local, sem chave de IA — então a leitura
caiu no "não consegui ler" e o fluxo de digitar à mão foi o que se testou): a
área de arrastar, a lista de arquivos com o tipo a escolher, a busca do
desligado, o "já está no cadastro", e o envio com arquivo chegando ao cadastro.
⚠️ **A leitura por IA em si não foi testada aqui** — precisa ser vista em
produção com documento de verdade.

### 19/11/2026 — O desligamento de carteira vira processo; quem continua não é mais desligado

**O pedido, com as palavras dele:** *"É feita a solicitação, o DP analisa, o
DP encaminha o pedido para contabilidade (…) A contabilidade retorna a
documentação rescisória, aí o DP vai encaminhar para quem solicitou, essa
pessoa (…) vai apresentar para o colaborador a documentação, colher assinaturas
e devolver para o DP. E o DP (…) gera o lançamento financeiro (…) da rescisão,
como também (…) a multa rescisória. Vai ser feito esses dois lançamentos pagos,
e só então a solicitação se encerra."*

**O que acontecia:** "Atender" gravava as datas e **fechava** o pedido na
hora. O resto vivia num checklist que ninguém era obrigado a marcar.

**O que acontece agora** (`core/desligamento_processo.py`, migração 114):
atender o desligamento de CTPS abre o processo — situação **"Em andamento"** —
e o pedido só se encerra com os passos cumpridos. O servidor recusa encerrar
antes. Os passos, e de onde vem a prova de cada um:

| Passo | Como se cumpre |
|---|---|
| O DP registrou a saída | ao atender |
| Confirmar se continua como prestador (só quando o pedido disse "sim") | o DP confirma, depois do último dia |
| Rescisão pedida à contabilidade | **lido**: o pedido ligado (ou o da pessoa aberto depois) foi enviado |
| Documentação rescisória de volta | **lido**: o pedido voltou (respondido/concluído) |
| DP mandou a documentação para quem pediu | marcado pelo DP |
| Voltou assinada | marcado pelo DP |
| Rescisão (TRCT) paga | o DP informa a SP; cumpre-se quando o lançamento estiver **pago** |
| Multa do FGTS paga | idem; **não se aplica** sozinho no pedido de demissão e no fim de experiência |

**Decisões tomadas por mim, e que ele pode trocar:**

1. **O sistema lê a prova antes de pedir marcação.** Passo lido pelo sistema
   só se marca à mão com o motivo escrito ("mandei pelo e-mail pessoal") —
   para nada travar quando algo for feito por fora, sem a marcação virar
   formalidade.
2. **O checklist continua**, e é outra coisa: o checklist é a lista de
   tarefas configurável do DP (migração 112); os passos são o portão do
   encerramento. A única mudança no checklist: saiu "abrir a readmissão como
   prestador".
3. **Uma SP não paga dois passos.** Rescisão e multa são dois lançamentos.
4. **Prestador continua fechando no atendimento** — não tem contabilidade,
   rescisão nem multa. Pergunta aberta (acima).
5. **Em andamento não se cancela pelo próprio pedido**: a saída já foi gravada;
   desfazer é o pedido de "cancelamento de desligamento".

**O "sim, continua" — e isto SUBSTITUI a decisão de 15/11/2026** ("continua
desligando e readmitindo — dois pedidos"). Ele: *"Quando eu coloco sim (…) O
ponto dele continua ativo, ele continua ativo. Mas o DP precisa validar (…) o
contrato de trabalho da pessoa precisa mudar (…) A data de saída dele vai ser
apagada (…) As outras informações têm que estar registradas, mas no
histórico."*

- **Ao atender:** a data de saída **não vai para o cadastro** — é ela que corta
  o ponto, o acesso e a folha. Fica no pedido. Último dia e aviso vão (não
  cortam nada).
- **Confirmar "sim"** (só a partir do dia seguinte ao último dia): o vínculo de
  carteira fecha **no histórico** com saída, aviso, forma e salário; abre o de
  prestador; o cadastro fica sem data de saída, sem salário de carteira, com o
  tipo de contrato e a diária/valor do mês informados. A trilha "Alterações no
  cadastro" mostra cada campo.
- **Confirmar "não"** (a qualquer momento — *"no meio do caminho decidiu-se
  que ele não continua mais"*): vira desligamento comum, com a data do pedido.
- **Sem confirmação, a pessoa segue ativa como CTPS.** É o risco que sobra, e
  está à vista em três lugares: no alto de Solicitações, no vigia do agente e
  no relatório.

**O primeiro dia como prestador é o dia seguinte ao ÚLTIMO DIA trabalhado, não
à data de saída** — no aviso indenizado a saída é projetada um mês à frente, e
quem continua trabalha no dia seguinte ao último dia. A tela mostra e o DP pode
trocar.

**Dois defeitos achados de passagem, e consertados:**

1. **Concluir o pedido de rescisão na contabilidade movia o cadastro para
   "Desligado" mesmo com a saída no futuro** — o mesmo defeito do aviso prévio
   de 17/11, por outra porta: a rescisão que volta antes do último dia tirava
   do ponto quem ainda trabalha. Agora só move se a data já passou.
2. **Achado DIRIGINDO A TELA:** depois de confirmar "continua", o pedido de
   rescisão à contabilidade era recusado ("falta o último dia") e sairia com a
   admissão do PRESTADOR. A rescisão agora lê a carteira que fechou
   (`contabilidade.carteira_que_saiu`).

**O agente:** o vigia passou a avisar a continuidade não confirmada, o
desligamento parado ou atrasado e o pronto para encerrar.

**Exercitado no navegador** com banco local: fila, processo, confirmação da
continuidade, montar o pedido à contabilidade, faixa do afastamento e retorno.

### 19/11/2026 — A documentação assinada vai e volta pelo sistema

**O pedido, com as palavras dele:** *"O arquivo precisa ser cobrado pelo
Agente e recebido. Imagino que o ideal é enviar via solicitação DP, o agente
analisa e o DP valida o recebimento da documentação."*

**Não precisou de peça nova para a ida e a volta: ela já existia e ninguém
ligava ao desligamento.** É a DEVOLUÇÃO do portal (migração 091): o DP manda
um documento para a obra, com prazo; a obra baixa no portal, devolve assinado
pelo portal, e o arquivo cai no cadastro da pessoa. E a rotina de cobrança do
agente JÁ cobrava devolução atrasada por WhatsApp.

O que entrou:

1. **"Mandar para a obra assinar"** dentro do desligamento: abre a devolução
   ligada ao pedido de rescisão, levando o arquivo que a contabilidade
   devolveu. Sem o arquivo, recusa — salvo se o DP disser que o papel vai por
   outro caminho.
2. **O agente confere o que volta — sem IA, sem custo de rede:** o arquivo
   chegou, não é minúsculo, o CPF da pessoa está no texto, a palavra
   "rescisão" aparece, e se o arquivo tem exatamente o tamanho do que foi
   enviado (sinal de que voltou sem assinatura). ⚠️ **E diz o limite: ele não
   enxerga assinatura.** Escaneado sem texto aparece como "não deu para ler —
   confira à mão".
3. **O DP valida ou recusa.** Recusar devolve a pendência à obra com o motivo
   e prazo novo. O passo só se cumpre com a validação.
4. Tipo de documento novo: **"Documentação rescisória assinada"**
   (`RESCISAO-ASSINADA`, sigilo pessoal), para o termo assinado não se
   confundir com o que a contabilidade mandou.
5. O vigia avisa a documentação assinada que chegou e espera validação.

### 19/11/2026 — A faixa do afastamento na janela da pessoa

Ele procurou como voltar um afastado para "Concluído" e não achou: *"não existe
a aba demandas"*. Existia, dentro do cadastro completo, dois cliques abaixo de
uma janela que não dizia que a pessoa estava afastada. Agora a janela da pessoa
(a do ponto e a ficha do ERP) mostra no alto "Afastado desde…", o que falta e
o botão **Registrar o retorno**, que abre direto o diálogo.

E o **afastamento antigo** (migração 111) tinha a data de início **estimada** —
para quem veio pela importação, a admissão. Com ela, o sistema cobrava ASO de
quem ficou uma semana fora. No retorno, a data passa a ser corrigível — **só no
afastamento antigo**; o novo teve a data informada na hora. Fica rastro
(`AFASTAMENTO_INICIO_CORRIGIDO`).

**Mexeu em arquivo compartilhado:** `routes.py` (só a rota de ações da
solicitação do DP ganhou três ações). Nada em `erp_base.html`, `permissoes.py`
ou `conftest.py`.

### 18/11/2026 — Liberar o acesso leva direto para configurar o acesso

**O pedido, com as palavras dele:** *"no ato da liberação já abrir a tela de
configurações do cadastro da pessoa. No caso é o administrador que vai ter
poderes para isso. Aí lá será designado o perfil dela, obras de acesso e etc.
E pronto."*

**O que acontece agora:** quem administra operadores confirma a liberação e
**cai direto em Configurações › Usuários, com o cadastro da pessoa aberto** —
perfil de acesso, perfis acrescentados, obras, cargo, fundo fixo e situação.
O resultado da liberação (inclusive o link do convite, quando o e-mail não
sai) aparece **dentro** dessa janela. Vale para as duas portas: a fila de
pedidos do portal e a ficha da pessoa.

Quem resolve a fila mas **não** administra operadores (o DP) não sai da tela
de Pessoal: libera, vê o aviso ali mesmo, e o acesso nasce com o padrão
seguro — administrativo de obra, preso à obra do cadastro.

**⚠️ O CAMINHO ERRADO QUE FOI DESFEITO NO MESMO DIA, e o motivo vale guardar:**
a primeira entrega foi uma **janelinha** na tela de Pessoal perguntando perfil
e obras. Estava certa no pedido anterior dele (*"tem que definir o acesso dele,
o perfil, as obras"*) e errada no desenho: virou **uma segunda tela com metade
dos campos** da tela de operadores que já existe. É exatamente a terceira porta
de que ele reclamou em 17/11 (*"era só para equalizar e complementar os
dados"*) — e um dia as duas divergiriam.

**A regra que isso confirma:** quando a resposta a "onde isso se configura?" já
existe numa tela, **leve a pessoa até ela**. Não recrie um pedaço dela onde a
ação começou.

**O que veio junto, e fica:**

1. **A resposta da fila emagreceu.** Ela mandava a lista de perfis e de cargos
   para desenhar aquela janelinha — uma consulta por abertura da tela de
   Pessoal, para uma lista que acabou morando em outro lugar. Agora manda só
   se a pessoa administra o acesso ou não.
2. **A trava do DP continua no servidor.** Nenhuma tela manda perfil ou obra,
   mas a rota segue recusando esses campos de quem não administra operadores —
   quem souber montar o pedido na mão não passa. Há teste.
3. **O recado não viaja escrito na URL.** Só uma marca conhecida e o link; o
   texto é montado na tela de destino, porque o aviso escreve HTML na página
   de quem administra o sistema.

**Mexeu em arquivo compartilhado** (importa para quem roda outro chat do ERP):
`routes.py`, `templates/erp_base.html` (ganhou um tipo de campo novo no
diálogo, uma lista de caixas de marcar — nasceu para a janelinha desfeita, mas
é genérico e qualquer tela pode usar) e `templates/erp_config.html`.

---

## Incidentes

(vazio)
