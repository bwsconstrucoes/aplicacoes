# Análise de SPs — onde o trabalho está, decisões, incidentes e o que falta

Este arquivo existe para uma sessão nova pegar o módulo sem repetir o que já
foi discutido e sem repetir o que já deu errado. O `README.md` ao lado explica
**como** ele é; este diz **em que pé está e por quê**. Leia os dois antes de
mexer; atualize este ao encerrar a sessão.

> Reconstruído em 03/09/2026 pela sessão do ERP a partir das mensagens de
> commit do módulo (que são detalhadas) — o chat que fez a conversão, no PC do
> dono, foi fechado antes de escrever isto. O que estava só na conversa e não
> nos commits se perdeu; o que segue é o que os commits permitem afirmar.

---

## Onde o trabalho está

Era um programa em Streamlit no computador do dono, lendo uma base local de
60 MB (59.055 SPs). Virou módulo Flask em `/analisesps`, no serviço que já
existe, com login próprio, dados no Postgres do ERP em schema próprio
(`analisesps`) e carga da planilha SPsBD em processo separado.

**Feito em 02/09/2026, em quatro commits:**

1. `da41ae2` — primeira tela (Solicitações), login com dois perfis, migração
   001, carga em processo separado com retomada por etapas.
2. `a8c3b16` — as outras sete telas: Lote, Relatório, Auditoria, Ratear,
   Bradesco, Agenda, Log; mais a tela de códigos de pagamento (QR Pix e código
   de barras). Migração 002 (agenda, feriados, lote, listas do rateio).
3. `f687e66` — correção: um import "curto" herdado do Streamlit quebrava em
   silêncio a conferência do Bradesco (boleto de 47 dígitos nunca casava).
4. `f58d097` — exportação CSV em todas as telas que geram número de reunião,
   relatório e lote em PDF (fpdf2), ações direto na ficha da SP.

**Estado em 02/09/2026:** 611 testes verdes sem banco (297 deste módulo);
57 com banco rodam só no GitHub Actions. Conferido contra a base real: a soma
das SPs bate na casa do centavo com o Streamlit (R$ 250.061.950,39).

**Em 03/09/2026 a pasta foi enxugada:** sobrou só o que roda no Render. O
Streamlit original saiu daqui — ver "Decisões já tomadas" abaixo.

### O que está pendente AGORA

Respondido pelo dono em **03/09/2026**:

- **Senhas:** cadastradas no Render. ✔
- **Migrações 001 e 002:** **NÃO aplicadas.** O dono achava que sim, mas o
  defeito abaixo prova que não — e explica por quê: a única tela que aplica
  as migrações era a que estourava.
- **Carga da planilha:** nunca rodou. A base está vazia no ar.
- **Streamlit no PC:** continua em uso, e continuará até este módulo estar de
  pé de verdade.

**Verificado em 04/09/2026, com um Postgres de verdade** (descartável, nesta
máquina — a produção não foi tocada), reproduzindo o estado exato da estreia:

- com o código que está **hoje no ar**, Configurações responde **500 "Deu
  erro"**. Confirmado, não deduzido.
- com a correção do ramo, ela responde **200**, lista as duas atualizações
  pendentes e mostra o botão.
- apertando o botão, as **duas migrações aplicam sem erro**, e em seguida as
  **nove telas abrem** com a base ainda vazia.

Ou seja: falta publicar. A correção resolve o impasse e a sequência seguinte
funciona.

**A sequência que falta, nesta ordem:**

1. Publicar a correção do defeito de estreia (abaixo) — sem ela a tela de
   Configurações não abre.
2. Na tela de Configurações, apertar **"Aplicar atualizações do banco"**
   (aplica 001 e 002).
3. Ainda ali, disparar a **Primeira carga**. Demora alguns minutos; pode
   fechar a página.
4. Só então navegar as nove telas com dado real — **ninguém nunca fez isso**.

---

## 04/09/2026 — a volta ao Streamlit

O dono navegou o módulo pela primeira vez e o veredito foi: **"muita coisa foi
mudada do Streamlit sem necessidade; ficaram funções perdidas e nomenclatura
alterada; eu não pedi isso"**. Ele tem razão, e isso muda o critério daqui em
diante:

> **Em dúvida, faça como o Streamlit fazia.** Quem for mexer nesta área tem o
> programa original no histórico do Git (saiu da pasta em 03/09, mas nada se
> perde): `git show 285d236:app/apps/analisesps/app/app.py`. Compare ANTES de
> inventar. O dono trabalhou anos com aquelas telas.

### O achado grave: "Cancelar" queria dizer duas coisas

No Streamlit, **Cancelar SP** abria o formulário público do Pipefy que PEDE o
cancelamento da solicitação. Na conversão virou um botão que grava
`Status Pgt = "Cancelado"` na planilha. Mesma palavra, ação diferente, e o
erro é silencioso: quem queria cancelar a SP marcava a planilha e ia embora
achando que tinha resolvido — enquanto o card seguia vivo no Pipefy.

Corrigido: o botão voltou a ser o formulário do Pipefy, e onde ficam os botões
que escrevem está dito, com todas as letras, que **daqui não se altera o
Pipefy — só a planilha SPsBD**.

### O que foi devolvido ao lugar

| O que era no Streamlit | Como estava | Agora |
|---|---|---|
| Coluna ID = link para o card do Pipefy | link para a ficha interna | link para o card |
| Ficha em **modal** por cima da lista | página inteira; voltar recomeçava tudo | modal no duplo clique na linha |
| Agendamento só com **Validação = "Sim"** | qualquer um agendava qualquer coisa | trava de volta |
| Barra de ações **fixa e única**, para a seleção inteira | uma barra por grupo, cada uma cega para as outras | uma só, colada no alto |
| Sem "Marcar Pago" no Lote | tinha | tirado |
| Agendar / Agendado / Desagendar / Falha Agendar | faltavam no Lote | os quatro, em todo lugar |
| **Enviar Lote** (marcadas → grupo "Novo Lote N") | sumido | de volta |
| **Abrir cards** (todas as marcadas de uma vez) | sumido | de volta |
| **Limpar Pgto** (Status → Pagar, Agendado vazio) | sumido | de volta |
| Links **Consulta** e **Atualizar** do Omie | sumidos | de volta, por variável de ambiente |
| Barra de filtros **única para o programa todo** | só nas Solicitações | Solicitações e Relatório |
| Filtro aplica ao clicar, sem botão | botão "Aplicar filtros" | aplica sozinho, com meio segundo de espera |
| Filtro **guardado** entre sessões | perdido a cada navegação | guardado por pessoa, no banco |
| Cores da tabela (vermelho/azul/roxo/verde/laranja) | paleta nova | as do Streamlit |

### O que o dono pediu de novo (não vem do Streamlit)

- **Nome no login.** O módulo não tinha noção de pessoa; o lote era um só e o
  registro de alterações sabia apenas o perfil. Agora cada um informa o nome ao
  entrar. **O nome não autentica**: quem manda continua sendo a senha. Ele
  serve para separar o lote, guardar os filtros e assinar o log.
- **Lote por pessoa.** Reverte a decisão de 02/09 ("o lote é compartilhado").
  O lote que existia não foi apagado: virou o "lote de antes", e a tela oferece
  trazê-lo por botão para quem ainda não tem o seu.
- **KPIs no cabeçalho de cada grupo do Lote**, com o total em destaque.
- **O total da seleção no meio da barra, grande.** É o número que decide se a
  remessa vai; estava num canto em letra miúda.

### Migração 003

`003_pessoa_e_preferencias.sql`: tabela de preferências, o lote passa a ser
chaveado por pessoa (a linha única `id = 1` vira a pessoa `''`), e o log ganha
a coluna `pessoa`. **Precisa do botão "Aplicar atualizações do banco"** logo
depois de publicar.

### Variáveis novas no Render

`SENHA_VALIDACAO` — a senha do botão Validar. Já existia no Streamlit, na aba
Credenciais da planilha; serve tanto de lá quanto da Environment do Render.
**Sem ela ninguém valida**, e a tela diz onde cadastrar em vez de só falhar.

`ANALISESPS_HOOK_OMIE` — o endereço do gancho do Make que o Streamlit usava
nos botões **Consulta** e **Atualizar** do Omie. **Não foi copiado para o
código de propósito**: é um endereço que dispara ação, e endereço assim não se
versiona. Sem a variável os dois botões simplesmente não aparecem — melhor do
que aparecerem quebrados. O valor está no `app.py` do Streamlit, na pasta que
foi movida para fora do repositório.

### Validar voltou — e a hesitação era minha, não do problema

Ficou de fora numa primeira passada com a desculpa de que "escreve numa coluna
que hoje é somente leitura". O dono cortou a conversa: *"qual o problema? Quando
aplicamos mudança de status muda a planilha da mesma forma, só que é coluna
diferente."* Está certo. É o mesmo caminho — banco, fila, log, planilha —, só a
letra da coluna muda (AH em vez de O ou AB).

O que realmente separa a Validação das outras não é o mecanismo, é o
significado: **ela é o que destrava o agendamento**. Por isso, como no
Streamlit, pede uma **senha própria** (`SENHA_VALIDACAO`, do Render ou da aba
Credenciais). Se a senha de Operador servisse, quem agenda seria o mesmo que
autoriza a agendar, e a trava não travaria nada.

Detalhe de construção que importa: `validacao` **continua fora de
`EDITAVEIS`**, e a validação tem porta própria (`/api/validar`). Se ela
entrasse na lista das colunas comuns, bastaria pedir `coluna: "validacao"` na
rota de sempre e a senha viraria enfeite. Há teste para isso.

O botão aparece em dois lugares: na barra de ações (validar várias de uma vez,
como no Streamlit) e dentro do próprio aviso de "agendamento bloqueado", que é
onde a pessoa descobre que falta validar.

### Segunda leva (04/09, depois do "prossiga")

- **A tabela voltou a ter as vinte colunas do Streamlit** (`tabela.py` espelha
  o `GRID_COLS` dele). A conversão tinha reduzido a nove, e a coluna que falta
  é sempre a de que se precisava naquele minuto: Validação, Nº NF, Data Pgt,
  Responsável, CPF/CNPJ.
- **Cada pessoa escolhe o que vê**, e a escolha fica guardada — como a
  configuração de tabela do Streamlit. A ORDEM é sempre a da definição, nunca
  a da escolha: se cada um visse as colunas noutra ordem, um não conseguiria
  explicar a tela para o outro. Sem nenhuma marcada, volta ao padrão — tabela
  sem coluna não é escolha, é acidente.
- **Uma tabela só** (`analisesps_tabela.html`) para Solicitações e para o
  Lote. Eram duas cópias, e já divergiam: o Lote mostrava menos colunas sem
  que ninguém tivesse decidido isso.
- **Os números de baixo** (o `painel_kpis`): Σ por conta corrente, Σ por forma
  de pagamento e a divisão do agendamento. Sumira justamente a resposta de
  "quanto vai sair de cada conta". Tudo SQL sobre o filtro inteiro, não sobre
  as 200 linhas da página — somar a página daria um número menor e
  convincente, que é o pior tipo de número errado.
- **Painel por status no Lote**, as quatro listas que ficavam embaixo. Leem a
  BASE, não o lote: é ali que se acha a SP que ficou para trás e que ninguém
  colou em lote nenhum.
- **Remover Risco**, com o nome de quem revisou no texto gravado. Dizer "pode
  pagar, eu conferi" é responsabilidade, e responsabilidade sem nome não é
  responsabilidade.

**Uma diferença deliberada, e o número que a justifica:** no Streamlit as
quatro listas do painel vinham INTEIRAS ("sem teto: exibe todos"). Lá isso
custava memória do PC; aqui cada linha vira HTML que atravessa a internet.
Medido com as 59 mil SPs: com 200 por status a página do Lote dava **1,2 MB**.
Com vinte, **162 KB** — e as demais estão a um clique, nas Solicitações já
filtradas. Tempos com a base cheia: Solicitações 247 ms, Lote 261 ms,
Relatório 326 ms.

### Terceira leva (04/09, o dono olhando a tela)

- **Validação entra nas colunas padrão; Responsável sai.** Escolha dele: é a
  Validação que destrava o agendamento, e não vê-la é trabalhar às cegas.
- **O número da SP não é mais pintado.** Somado ao vencimento vermelho ao
  lado, a linha inteira ficava gritando. O alerta continua em selo, na coluna
  Alertas, onde não compete com nada.
- **"Cancelar SP" ficou discreto.** É ação séria, mas não é a principal da
  ficha; em vermelho puro puxava o olho toda vez que a ficha abria.
- **Período na Auditoria, nas sete checagens.** Auditar a base inteira dá o
  retrato de sempre; auditar um mês responde "o que entrou errado neste
  fechamento". Recorta por vencimento ou por data da solicitação — a coluna é
  escolhida de uma lista fechada, nunca vem de fora.
- **Nota repetida deixa de acusar parcelamento.** Uma nota parcelada em três
  gera três SPs com o mesmo número, e apontar as três todo mês é o jeito mais
  rápido de fazer alguém parar de olhar a auditoria.

  A regra, dita como ela é: **o grupo só sai da lista quando TODAS as SPs têm
  marca de parcela e essas marcas são todas diferentes.** Se duas dividem a
  mesma parcela, ou se alguma está sem marca, o grupo continua aparecendo —
  aí "é parcelamento" não explica. Na dúvida, aponta: conferir à toa custa
  pouco perto de pagar duas vezes. A marca sai da coluna Parcela ("001/003");
  quando ela está vazia, procura-se na descrição ("2/3", "parcela 2",
  "2ª parcela"). A coluna ganha da descrição, porque descrição é texto livre
  e erra mais.
- **A Agenda voltou a ter calendário.** "A agenda só tem uma lista", disse o
  dono — e o Streamlit tinha uma grade de mês, com ◀ ▶ e o que cai em cada
  dia. Lista não responde "como está a semana que vem".

### Quarta leva (05/09) — descrição, tipo de despesa e o nome lembrado

- **Descrição e Tipo de Despesa entram nas colunas padrão**, nas duas telas.
  Sobre o nome: o dono pediu "Categoria da Despesa". Na SPsBD existe **Tipo
  de Despesa** (coluna I) e é essa a classificação que a SP carrega —
  "Categoria de Despesa" no sentido do Omie é outra coisa e só aparece na
  tela de Ratear, não é gravada em cada SP. O rótulo ficou o da planilha.
- **A descrição tem tratamento próprio** (`tipo: "longo"`): letra menor,
  cortada na largura, texto inteiro no `title`. É a única coluna que compete
  com a tela toda.
- **Um clique esconde e devolve a descrição**, ao lado da lista de colunas.
  Abrir a lista inteira para mexer numa coluna só é caro demais para uma
  coisa que se faz dez vezes por dia.
- **As colunas são as mesmas nas duas telas** — sempre foram, porque as duas
  leem a mesma escolha. Agora há teste travando isso.

### O NOME é a chave — e o que se fez para ele não virar armadilha

O dono perguntou se o nome ficava gravado, e explicou por quê: *"como vão ser
salvas minhas informações de filtros e lote?"*. A pergunta expõe a fragilidade
real de usar o nome como chave — digitar "Marcelo" hoje e "Marcelo Leitão"
amanhã dá **duas pessoas**, e a segunda encontra tudo vazio.

Três coisas, nesta ordem de importância:

1. **O navegador lembra o nome** (cookie próprio, 180 dias). Ao voltar, o
   campo já vem preenchido — resolve o caso comum, que é a mesma pessoa na
   mesma máquina. **Só o nome**: a sessão continua morrendo quando o navegador
   fecha, porque é ela que diz que alguém digitou a senha. Guardar a senha
   "para facilitar" seria outra conversa, e a resposta seria não.
2. **O nome fica à vista no alto da tela.** Fora da vista, um nome digitado
   diferente daria outro lote sem ninguém notar.
3. **Nome novo com lote vazio recebe aviso**, dizendo de quem há lote
   guardado e que maiúscula e acento não separam, mas palavra a mais separa.
   Sem isso a pessoa abre o Lote, vê vazio e conclui que o sistema perdeu o
   trabalho dela.

A chave normaliza maiúscula, acento e espaço sobrando. **Não é controle de
acesso**: as quatro pessoas dividem a mesma senha, e a separação por nome é
organizacional, não uma tranca. Dito assim para ninguém confundir as duas.

### Quinta leva (05/09) — a Agenda volta a aceitar lembretes

*"e agenda? como faço pra adicionar lembretes? nao ta funcionando"* — e não
estava mesmo. A conversão deixou a agenda **só de leitura**: os compromissos
vinham da aba "Agenda" da planilha de Credenciais e não havia como pôr nada
nela pela tela. Sem alguém preencher a aba à mão, a agenda abria vazia e sem
explicar o porquê. O Streamlit tinha "Novo compromisso" e "Editar".

- **Novo lembrete, editar e desligar**, pela tela.
- **A PLANILHA CONTINUA SENDO A DONA.** Grava-se lá PRIMEIRO e só depois
  aqui. Se fosse ao contrário, uma falha de rede deixaria o lembrete vivo na
  tela e invisível na planilha — e a próxima sincronização não o traria de
  volta: ele existiria só aqui, até alguém reparar. Falhando a planilha,
  **nada é salvo** e a tela diz o que houve.
- **Sem fila, de propósito.** A fila das SPs existe porque são centenas de
  células e a conexão oscila. Aqui é uma linha por vez, algumas por mês —
  escrita direta, com erro na cara, é mais honesto e muito mais simples.
- **A aba "Agenda" é criada se não existir**, com o cabeçalho certo. É a
  causa mais provável de "não funciona": sem a aba, não há o que trazer.
- **Desligar não apaga.** O lembrete some da vista e continua guardado —
  desligar um lembrete de imposto por engano e não ter como trazê-lo de volta
  seria pior do que o engano.
- **Defeito corrigido de passagem:** "ligado" e "desligado" queriam dizer
  coisas diferentes em lugares diferentes — o calendário exigia status
  "ativo"; os próximos só descartavam "cancelado". Um compromisso marcado
  "inativo" aparecia num e não no outro. Agora há uma regra só
  (`agenda.esta_ativo`).

Os padrões do Streamlit ficaram: o **dia da repetição sai da primeira data**
(não há campo separado, para os dois não se contradizerem), **dia 31 quer
dizer "último dia do mês"**, e **imposto, FGTS e parcelamento antecipam**
quando caem em dia não útil.

### Sexta leva (05/09) — o Lote

- **"Remover informação" virou "Desagendar"**, o termo do Streamlit e o que o
  dono usa. O rótulo antigo descrevia o efeito, mas agora convive na mesma
  barra com "Remover do lote" — e dois "remover" com efeitos diferentes lado
  a lado é pedir para alguém errar.
- **"Remover do lote" entrou na barra fixa**, junto dos outros. Tira as SPs
  marcadas do lote, **em qualquer grupo, de uma vez**. Antes, tirar uma SP
  era editar o texto do lote na mão e achar o número no meio dos outros.

Três decisões que valem estar escritas:

1. **Não altera a SP.** Mexe só na lista: não escreve na planilha, não entra
   na fila, não toca no Pipefy. A confirmação diz isso — "remover" numa tela
   de pagamentos assusta, e com razão.
2. **Os títulos dos grupos ficam**, mesmo que o grupo esvazie. Apagá-los
   junto faria a remessa perder a divisão que alguém montou, e remontar custa
   mais do que uma linha vazia incomoda. Mesma decisão do "Tirar as pagas".
3. **O painel por status embaixo mostra SPs que NÃO estão no lote.** Marcar
   uma delas e mandar remover não é erro — não há o que tirar, e a tela diz
   isso em vez de fingir que fez. Quando a seleção mistura as duas coisas, ela
   conta quantas saíram e quantas já não estavam lá.

O botão **só existe na tela do Lote**. Nas Solicitações o botão vizinho é o de
MANDAR para o lote, e os dois na mesma barra seriam a confusão pronta.

### Defeito trazido do painel: senha com acento derrubava o login

Em 05/09 o painel descobriu, no uso real, que `hmac.compare_digest` **com
texto só aceita ASCII**: uma senha com "ç" ou "ã" fazia a comparação
ESTOURAR, e o login virava erro 500 em vez de "senha incorreta". Quem digitou
nunca descobriria que só errou a senha — concluiria que o sistema caiu.

**O código daqui era o mesmo**, nas TRÊS portas que comparam senha: o login,
o segredo do agendador e a senha de validação. Corrigido no mesmo dia:
`auth.confere` compara os BYTES, o que aceita acento sem perder o tempo
constante. Três testes travam isso, e foi conferido que os três falham com o
código de antes.

A lição, que vale para as outras áreas: **quando um módulo acha um defeito
num pedaço que foi copiado, os outros têm o mesmo defeito.** Procurar leva
minutos; descobrir em produção leva um susto.

### Sétima leva (05/09) — a busca por atualizações de 90 em 90 segundos

*"a busca por atualizacoes a cada 90s acho que nao tá acontecendo"* — e não
estava. Pior: **provavelmente nada estava atualizando a base sozinho.**

O Streamlit tinha "Auto-atualizar (90s)", ligado por padrão. A conversão
deixou de fora, apostando num **agendador externo** (cron-job.org chamando
`/api/sincronizar` com o `ANALISESPS_SECRET`) — e **não há sinal de que esse
agendador tenha sido configurado**. Sem os dois, a base só se atualizava
quando alguém apertasse o botão em Configurações.

O que foi feito:

- **A tela aberta pergunta a cada 90 s** (`/api/frescor`) e, se a última
  sincronização tiver mais de **cinco minutos**, **dispara** a sincronização
  no processo separado. Quem estiver com a tela aberta mantém a base viva
  para todo mundo — inclusive o perfil Consulta, porque a base é de todos.
- **Cinco minutos, e não 90 segundos, para o disparo.** Com quatro pessoas
  com a tela aberta o dia inteiro, disparar a cada 90 s seriam quarenta
  sincronizações por hora, todas lendo a planilha e gastando cota do Google.
- **A tela NÃO se recarrega sozinha quando há SPs marcadas** (nem com a ficha
  aberta). Recarregar por baixo de quem acabou de marcar vinte linhas
  apagaria a seleção, e isso é pior do que ver um número com dois minutos de
  idade: aparece um aviso discreto no rodapé e quem decide é a pessoa.
- **A hora da última sincronização ficou à vista, no alto.** "Está
  atualizando?" tem de ser respondível de relance.

**O agendador externo continua valendo** e continua sendo melhor: ele atualiza
a base de madrugada, com todo mundo dormindo. Isto aqui é a rede de segurança
para quando ele não existe.

### Defeito que esta mudança expôs: a trava não era do banco

"Uma atualização por vez" era conferida pelo **programa**: perguntava "está
rodando?" e, se não, abria uma execução. **Entre a pergunta e a resposta cabe
outra requisição.** Com o botão manual isso quase nunca acontecia — uma
pessoa, um clique. Com quatro telas perguntando quase ao mesmo tempo, passa a
acontecer: quatro processos de sincronização nascendo juntos, quatro leituras
da planilha, quatro vezes a cota, para o mesmo trabalho.

**Migração 004** põe um índice único parcial: no máximo UMA linha com
`fim IS NULL`. Agora quem recusa é o Postgres, e o programa traduz a recusa em
"já existe uma atualização em andamento". Conferido contra banco de verdade:
a segunda inserção é recusada pelo banco, e a tela recebe o recado em
português em vez de um erro.

### Oitava leva (05/09) — o filtro de obras

Uma célula de centro de custo pode trazer **mais de uma obra**: a planilha
aceita "CONS, CRECHE SWAP" quando a despesa é rateada entre elas. A lista do
filtro oferecia **a combinação inteira** como se fosse uma obra — e a obra
sozinha, que é o que se procura, não aparecia em lugar nenhum.

- **A lista agora vem separada**: cada obra uma vez, sem combinações.
- **O filtro ficou pesquisável.** Blocos com doze opções ou mais ganham um
  campo de procura. Ele filtra as caixas já carregadas — não vai ao servidor,
  não aplica nada, só ajuda a achar. Ignora acento e maiúscula ("sao" acha
  "SÃO"), e **nunca esconde uma opção já marcada**: escondê-la faria a pessoa
  achar que desmarcou sozinha.
- O bloco passou a se chamar **"Obra (centro de custo)"** — o nome que o dono
  usa, com o da planilha entre parênteses.

**Um defeito antigo corrigido junto.** O casamento era por "contém", copiado
do Streamlit: procurar a obra **CONS** trazia também **CONSTRUÇÃO DO GALPÃO**,
porque uma é pedaço da outra. Agora a célula é aberta nos separadores e a
comparação é com a obra INTEIRA. Silencioso do jeito pior: o número na tela
estava errado e ninguém tinha como notar.

**Os separadores são três, e isso veio de um teste, não de um palpite.** O
dono citou a vírgula; um teste escrito na conversão, a partir da base real,
usava **barra** ("OBRA-12 / OBRA-13"). Aceitar vírgula, barra e ponto e
vírgula custa nada e evita descobrir o terceiro em produção.

**O que NÃO foi mexido, e é decisão sua:** o **Relatório** continua agrupando
pela célula inteira — "CONS, CRECHE SWAP" aparece como uma linha só. Separar
ali exigiria dividir o VALOR entre as duas obras, e dividir por quanto é
pergunta de negócio: meio a meio? pelo rateio do Omie? Somar o valor cheio nas
duas faria o total do relatório passar do total real. Ficou como está até
alguém decidir.

### Nona leva (05/09) — a tela dos códigos de pagamento

Dois defeitos que o uso mostrou, e o segundo era caro:

**1. Faltava marcar dali.** O caminho normal é: gerar o código, pagar, marcar.
Sem a barra de ações na tela dos códigos, era voltar para a lista, procurar as
mesmas SPs de novo e marcar lá. No Streamlit os códigos apareciam LOGO ABAIXO
da barra, na mesma tela — a barra sempre esteve ao alcance.

Agora a barra está lá, e **as SPs já chegam marcadas**: quem entrou nesta tela
foi porque escolheu aquelas. Os botões que não fazem sentido aqui ficam de
fora — gerar o QR estando nele, e mexer no lote.

**2. Clicar no número da SP destruía o trabalho.** O número abria a ficha em
tela cheia; voltar trazia a lista, e **os códigos recém-gerados sumiam**. Quem
só queria conferir um dado tinha de refazer todo o caminho — escolher as SPs,
gerar de novo — e isso no meio de um pagamento.

Agora abre no **modal**, por cima dos códigos. Continua sendo um link de
verdade: ctrl+clique e botão do meio abrem a página inteira em outra aba, que
é o certo — um modal não sobrevive à aba nova.

O modal passou a abrir também **no clique de qualquer link marcado**, não só
no duplo clique de uma linha de tabela. Nas tabelas o número segue abrindo o
card no Pipefy, como o dono pediu; fora delas, abre a ficha.

### Décima leva (05/09) — o código de pagamento dentro da ficha

Pedido do dono: ao abrir a SP no modal, mostrar já o QR Pix ou o código de
barras. Quem abre a ficha para conferir um dado quase sempre está a caminho de
pagar, e voltar à lista só para gerar o código era um caminho a mais em cada
pagamento.

- **A montagem do código virou função** (`_codigo_de_pagamento`), usada pela
  tela de códigos E pela ficha. Duas cópias divergiriam no dia em que uma
  ganhasse um caso — e a que ficasse para trás mostraria um código errado a
  quem está pagando.
- **O botão "QR / Código" saiu da ficha**, por decisão do dono: com o código
  ali, virou redundante.
- **SP que já saiu recebe aviso antes do código.** Mostrar um QR de pagamento
  numa SP marcada como Paga é o caminho curto para pagar duas vezes. O código
  continua aparecendo — às vezes é justamente o que se quer conferir —, mas
  com o aviso na frente.
- **Forma sem código explica**, em vez de deixar um espaço em branco que
  pareceria falha do sistema.
- **"Remover do lote" perdeu a cor de alerta**, por decisão do dono: ele não
  altera nada na planilha, então não merecia se destacar. O aviso continua na
  confirmação do clique, que é onde importa.

**Um defeito antigo corrigido junto.** O gerador devolve o código de barras
como um SVG de ARQUIVO, com cabeçalho XML e `<!DOCTYPE>` próprios. Colado
dentro de uma página HTML isso é inválido, e alguns navegadores param de
desenhar o resto a partir dali. Agora só o `<svg>` vai para dentro. Estava
assim desde a conversão, na tela de códigos — foi um teste que apontou.

### Décima primeira leva (05/09) — a hora crua e a coluna Obra

**O carimbo aparecia cru na tela:** *"base de 2026-09-04T17:25:31.319885-03:00"*.
A última sincronização é guardada como **texto** em `analisesps.meta`, e o
formatador de data só sabia converter data de verdade — o resto passava
inteiro. Agora há `momento_br`, que aceita texto, data e data-e-hora, e
devolve **"04/09/2026 às 17:25"**, na hora de Brasília.

E aqui a **hora é o ponto**: "a base é de quando?" respondido só com o dia diz
"hoje", que é o que já se sabia. Pelo mesmo motivo, o **registro de
alterações** passou a mostrar a hora — duas mudanças no mesmo dia, sem ela,
ficam indistinguíveis.

**Um defeito de fuso corrigido junto:** uma sincronização das 22h daqui é 1h
do dia seguinte em UTC. Sem converter antes de cortar a hora, a tela mostraria
**a data de amanhã**. O `data_br` agora normaliza para Brasília antes.

**A coluna Obra entrou nas colunas padrão**, logo depois do Valor — é a
pergunta seguinte a "quanto é": "de qual obra?". Vale nas duas telas, que leem
a mesma escolha. O cabeçalho usa **"Obra"**, a palavra do dono, porque cabe na
coluna estreita; a barra de filtros diz "Obra (centro de custo)", que é onde a
ponte com o nome da planilha cabe.

### Décima segunda leva (05/09) — o botão que parecia quebrado

**"Clico em Agendado no modal e não acontece nada."** Não era defeito de
ligação, e vale registrar porque a conclusão é contraintuitiva: a **trava da
Validação** — restaurada do Streamlit — punha `disabled` nos quatro botões de
agendamento quando a coluna Validação não estava em "Sim". E **botão
desabilitado não recebe nem o clique**: para quem não leu o aviso logo acima,
ele é indistinguível de um botão quebrado.

A trava continua valendo (nada é gravado sem Validação = "Sim"), mas agora ela
**se explica**: o botão tem cara de cadeado, aceita o clique, e o clique diz
por que não foi — oferecendo validar ali mesmo. Trocar um bloqueio mudo por um
bloqueio que fala custa nada e evita o chamado.

> Nota de fidelidade, para quem for mexer nisso: no Streamlit a trava valia no
> **detalhe** e no **lote** ("Alterar Status" só habilitava com todos os
> selecionados validados). Na **tela de códigos**, o botão "📅 Agendado" era
> *sempre clicável*. Aqui a barra de ações de cima **não** exige Validação em
> nenhuma tela — é mais permissivo que o Streamlit. Está assim de propósito
> até o dono decidir: apertar a barra tiraria função que ele já usa hoje.

**A ficha foi virada de cabeça para baixo, a pedido do dono:**

- a **Descrição subiu para o topo**, logo abaixo do cabeçalho. É o que diz do
  que se trata a SP, e é a primeira coisa que se procura ao abrir; estava no
  fim de tudo, depois de vinte e sete campos.
- o **código de barras / QR desceu para o fim**. É o passo final de quem já
  conferiu o resto e vai pagar.

**Link escrito na descrição virou link clicável.** A descrição costuma trazer
o endereço de uma pasta ou de um contrato, e como texto puro era selecionar na
mão e colar no navegador.

> O cuidado que isso exige, para não ser desfeito por engano: a descrição vem
> da **planilha**, que qualquer um edita. O filtro `com_links` **escapa o texto
> inteiro primeiro** e só depois transforma em link o que sobrou — sem isso,
> uma célula com `<script>` dentro rodaria na tela de quem abrisse a SP. Por
> devolver HTML pronto, no template ele vai com `|safe`; quem mexer nele mexe
> nos dois lados. Há teste para o `<script>`, para a aspa dentro do endereço e
> para o ponto final da frase não entrar no link. Usa `html.escape` da
> biblioteca padrão de propósito — nenhuma dependência nova.

**"Cancelar SP" deixou de ser vermelho.** Ele só **abre** o formulário do
Pipefy; não cancela nada por si. Em vermelho puxava o olho toda vez que a
ficha abria, como se fosse a ação principal.

**Defeito achado de passagem, e sério:** abrir uma SP em **página inteira**
vindo do **Lote** estourava a tela. O endereço da volta era montado colando
`"analisesps."` com a origem, e dava `analisesps.lote` — que não existe; a tela
do Lote chama-se `tela_lote`. Só não aparecia sempre porque o caminho normal
hoje é o modal. Corrigido, com teste.

**Verificado:** 1342 testes verdes, agora **com Postgres de verdade** (local e
descartável — a produção não foi tocada), e os 18 blueprints sobem. O que
**não** foi verificado: nada disto foi exercitado no navegador com dado real —
são mudanças de tela, e o teste confere o HTML, não o que o olho vê.

### Décima terceira leva (05/09) — o BeeVale voltou

O dono perguntou pelas três funções do BeeVale ("gerar a planilha, cadastro,
e o gerar") e não as encontrou. **Estavam mesmo faltando**: na conversão do
Streamlit elas não vieram, e o `HISTORICO` registrava isso como "não voltou"
por causa de um erro 403 de cota no Drive. A decisão do dono foi: **criar
tudo, e ele informa a pasta depois.**

**O que voltou, com os mesmos nomes do Streamlit:**

- **Cadastro BeeVale** — cola-se a lista de e-mails/CPFs que o portal
  devolveu, e sai a planilha de cadastro para baixar. **Não escreve em lugar
  nenhum**: lê a planilha "Dados Documentos" e devolve um arquivo. Funciona
  hoje, sem depender de nada configurado.
- **Gerar BeeVale** — as SPs marcadas, uma tela de **conferência** primeiro
  (o que cada card tem, o que está impedido e por quê), e só então o botão que
  monta as duas planilhas por card, sobe no Drive e escreve os links e a
  Documentação Fiscal no card do Pipefy.

**Três coisas foram feitas diferente do Streamlit, e cada uma tem motivo:**

1. **A conferência antes.** No Streamlit o diálogo abria e o botão fazia tudo.
   Aqui a tela lista, ANTES, quem está pronto e quem está impedido — e mostra
   o valor do card **ao lado** do valor da base. São duas origens diferentes;
   é aqui que uma divergência aparece antes de virar recarga errada.
2. **A ordem é sagrada, e há teste para ela:** primeiro tudo o que pode falhar
   sem estragar (buscar, montar, subir no Drive), e **só no fim** a escrita nos
   cards. Se o Drive recusar, nenhum card foi tocado. Marcar o card e depois
   descobrir que o arquivo não subiu deixaria um card dizendo "pronto" quando
   não está — e ninguém teria como saber.
3. **Sucesso pela metade não conta como sucesso.** Arquivo no Drive com o card
   sem atualizar aparece como problema na tela, com os links à mão para colar
   no card manualmente.

**A resposta à pergunta "a pasta do Drive ficou salva?":** não dava para saber
de dentro do código — é uma variável do Render/planilha de credenciais, que
esta máquina não enxerga. Por isso **Configurações ganhou um cartão que
responde**: diz se `DRIVE_FOLDER_ID` e `PIPEFY_TOKEN` estão configurados
(sem mostrar o valor — só os **seis últimos caracteres** da pasta, o
suficiente para reconhecer qual é), e um botão **"Conferir a pasta do Drive"**
que olha a pasta **sem escrever nada** e diz o nome dela.

> **A ARMADILHA DA COTA, escrita uma vez para não se perder de novo.** A conta
> de serviço do Google **não tem espaço de armazenamento próprio**. Ela grava
> numa pasta de **Drive Compartilhado** (Shared Drive) onde seja membro com
> permissão de gravar. Numa pasta comum do "Meu Drive" — **mesmo
> compartilhada com ela como Editor** — o Google recusa com
> `storageQuotaExceeded`, cuja tradução ao pé da letra ("cota estourada") faz
> pensar em falta de espaço e manda consertar a coisa errada. O conserto é
> **mover a pasta para um Drive Compartilhado**. O `drive.py` traduz esse erro
> para essa instrução, e o botão de conferir avisa antes de qualquer geração.
>
> Vale notar: o `email_financeiro`, neste mesmo repositório, já sobe arquivo no
> Drive com a **mesma** conta de serviço, numa pasta que funciona. Ou seja, o
> caminho é viável — o que falhou em 02/09 foi a pasta, não a conta.

**Trava mantida do Streamlit:** "Gerar BeeVale" só habilita quando **todas** as
SPs marcadas têm forma de pagamento BeeVale. Não é preciosismo: gerar a
recarga de uma SP que se paga por boleto põe dinheiro no cartão de quem não
devia receber, **e** marca o card como resolvido.

**Arquivos novos:** `beevale.py` (as regras e os dois arquivos `.xlsx`),
`pipefy.py` (o pouco que se lê e escreve lá) e `drive.py` (a subida). O
`pipefy.py` é o **único lugar do módulo que escreve fora** da planilha SPsBD —
está dito no alto do arquivo. Nenhuma dependência nova: o `openpyxl` já estava
no `requirements.txt` por causa do painel, e a autenticação do Drive usa o
`google-auth` que o gspread já traz. A credencial é a de sempre
(`GOOGLE_CREDENTIALS_BASE64`).

**O que FALTA para funcionar de verdade** (nesta ordem):

1. o dono informar a pasta do Drive → `DRIVE_FOLDER_ID` no Render, **de um
   Drive Compartilhado**;
2. conferir que `PIPEFY_TOKEN` está no Render (Configurações diz);
3. apertar "Conferir a pasta do Drive" e ver "em Drive Compartilhado";
4. **gerar UMA SP primeiro**, conferir o card, e só então usar em leva.

**Verificado:** os testes cobrem o layout das duas planilhas (contrato com o
portal do BeeVale), o CPF saindo como texto (o zero da frente some se virar
número, e o portal recusa), a descrição do card sendo preservada, os links não
empilhando a cada geração, a ordem Drive→Pipefy, o Drive falhando sem tocar no
card, o card sem CPF não parando os outros, e o id de card não numérico sendo
recusado (ele entra na consulta sem aspas — texto ali seria injeção).

**NÃO verificado, e é a parte que importa:** nenhum teste encosta no Drive ou
no Pipefy de verdade — os dois são dublados. A primeira geração real **é** o
teste. Faça com uma SP só.

### Décima quarta leva (05/09) — a lentidão, medida em vez de deduzida

O dono reclamou: *"funcional, mas não é legal — você está toda hora esperando
a tela carregar"*, e disse que o Streamlit, que ele já achava lento, é **mais
rápido** que isto. Uma sessão anterior já tinha apontado uma causa; esta
**mediu**, e o número mudou o plano.

**Como foi medido, para quem quiser repetir:** um Postgres local e descartável
com **59.055 SPs** sintéticas (a produção não foi tocada), cronometrando cada
consulta e depois a tela inteira pelo cliente de teste. Vale a ressalva: o
banco estava na MESMA máquina, sem a latência de rede que existe no Render.
Os números reais lá são maiores; as proporções, as mesmas.

| | Antes | Depois |
|---|---|---|
| Solicitações | 376 ms · 15 idas ao banco | **162 ms · 8 idas** |
| Solicitações filtrada | 359 ms | **154 ms** |
| Solicitações pelo menu | 357 ms | **151 ms** |
| Relatório pelo menu | 404 ms | **219 ms** |

**Correção da análise anterior, para o histórico não guardar número errado:**
ela dizia "doze idas ao banco". São **quinze**. E a primeira contagem que fiz
disse vinte — eu tinha instrumentado `consultar` e `consultar_um` ao mesmo
tempo, e `consultar_um` chama `consultar`, então tudo contou dobrado. Quinze é
o número certo.

**Causa 1, a maior: as sete listas do filtro, 194 ms por clique.** Cada uma
varre as 59 mil SPs inteiras para descobrir quais valores existem naquela
coluna. Os índices não ajudam — a consulta limpa o texto antes de agrupar.
**Índice de expressão foi tentado** (inclusive um que casa exatamente com a
expressão da consulta) e o Postgres continuou preferindo a varredura; não é
caminho, e fica registrado para ninguém tentar de novo.

O desperdício é que essas listas quase nunca mudam: os projetos e as contas da
empresa são os mesmos hoje e amanhã. Passam a ser calculadas **uma vez por
carga**, com o carimbo da última sincronização como chave. Isso funciona
**entre processos** sem combinação nenhuma: a carga roda num processo separado
e não tem como avisar o da tela, mas o carimbo que ela grava no banco é o
próprio aviso.

> **O custo, que é do dono e ele aceitou:** um projeto novo cadastrado na
> planilha só aparece na listinha do filtro depois da próxima sincronização
> (a tela dispara uma a cada 5 min). A SP nova aparece na LISTA normalmente —
> é só o menu de filtro que demora a saber do valor novo.

**Causa 2: duas varreduras da mesma tabela filtrada.** O resumo (44 ms) e a
divisão do agendamento (48 ms) percorriam separadamente exatamente as mesmas
linhas. Juntos numa consulta só: **59 ms**, porque a varredura é uma e as
contagens vão de carona. Conferido com dado real em quatro filtros diferentes:
as contas batem exatamente com as das duas funções antigas.

> **Tentado e DESCARTADO:** juntar também as duas somas (por conta e por forma
> de pagamento) numa consulta com CTE. Ficou **pior** — 67 ms contra 51 ms —,
> porque o banco precisa guardar o resultado do meio. Ficam separadas. Está
> aqui para não ser "otimizado" de novo por intuição.

**Causa 3: quem clica no menu carrega a tela duas vezes.** Chegar sem filtro na
barra de endereço dispara um redirecionamento para o endereço COM o filtro
guardado — e a função inteira roda duas vezes por clique. O redirecionamento
continua (é ele que faz o filtro sobreviver à troca de tela), mas agora é a
**primeira coisa** que a tela confere: antes ele já tinha perguntado o tamanho
da base para nada. A perna que só redireciona caiu de 4 idas ao banco para 1.

**O que NÃO foi mexido, e por quê:** as duas somas por conta e por forma
(51 ms) e o resumo (59 ms) varrem a tabela filtrada e não têm como não varrer —
somar o que o filtro alcança é a pergunta. O **Relatório** ainda faz 12 idas
(oito agregações); é o próximo lugar a olhar se ele continuar pesado, e é uma
mudança maior do que estas.

**Há teste para o ganho não se desfazer sozinho:** que as listas não são
refeitas sem carga nova, que uma carga nova as refaz, e que o resumo junto
varre a tabela uma vez só. É o tipo de correção que uma refatoração distraída
desmancha, e cujo efeito só aparece em produção, como lentidão sem culpado.

### Décima quinta leva (05/09) — a pasta do Drive vira campo na tela

O dono pediu: *"deixa esse campo lá pra poder colar a informação da pasta e
salvar"*. Feito, em **Configurações**. Guardado na tabela `meta`, que já
existe — **sem migração nova**, então funciona no dia da publicação.

Três decisões que valem registro:

1. **O que é colado na tela GANHA do Render e da planilha.** É o contrário da
   regra geral da casa ("ambiente ganha da planilha"), e de propósito: se um
   valor do Render vencesse em silêncio, o dono colaria a pasta, apertaria
   salvar, veria "salvo" — e nada mudaria. Um campo que aceita e ignora é pior
   do que campo nenhum. Para a regra não virar surpresa, a tela **diz de onde**
   o valor que está valendo veio.
2. **Aceita o endereço inteiro da pasta**, copiado da barra do navegador, e
   guarda só o identificador. Exigir que a pessoa recorte o pedaço certo de
   uma URL é pedir para errar. O campo mostra depois o que FICOU salvo.
3. **A pasta aparece no campo; o token do Pipefy, nunca.** A pasta não é
   segredo — é o endereço de uma pasta — e ele precisa poder conferir e trocar
   o que colou. O token é segredo de verdade: com ele se lê e se escreve nos
   cards da empresa, e a tela só diz se está configurado. Há teste para os dois.

### Décima sexta leva (09/09) — a lista de quem entra, e o fim da espera pelo botão

Duas coisas, e a segunda é a que importa.

**O dono perguntou:** *"basta colocar o nome idêntico toda vez para acessar os
meus filtros e o meu lote?"* A resposta era "sim, mas" — e o "mas" era grande
demais para deixar como está.

**1. A entrada virou LISTA.** MARCELO, THIAGO, KARLA e RAFAEL, escolhidos num
menu em vez de digitados. O nome é a chave de tudo o que é "seu"; com campo
livre, digitar "Marcelo" hoje e "Marcelo Leitão" amanhã dava DUAS pessoas, e a
segunda abria o Lote, via vazio e concluía que o sistema tinha perdido o
trabalho dela. Não há como digitar diferente aquilo que não se digita.

O que a tela manda é **conferido contra a lista** e volta com a grafia oficial:
um pedido montado à mão não cria uma quinta pessoa por fora, e o registro de
alterações para de mostrar o mesmo colega escrito de três jeitos. A lista se
edita em **Configurações** — a tabela `meta`, sem migração, então funciona no
dia da publicação.

> **Isto NÃO é cadastro de usuário e não dá acesso a ninguém.** As quatro
> pessoas usam a MESMA senha, e é a senha que decide o que se pode fazer.
> Escolher "KARLA" não dá poder nenhum a mais. Quem um dia precisar impedir
> que alguém se passe por outro tem de usar o cadastro do ERP; aqui o nome é
> etiqueta honesta entre colegas, não tranca. Há teste para isso.

**2. O ARMÁRIO DE RESERVA — e este era um defeito de verdade, não uma
melhoria.** O dono pediu: *"faça de alguma forma que os filtros e o lote
fiquem salvos"*. Fui olhar por quê não estavam:

A tabela `preferencias` e a coluna `lote.pessoa` nascem na **migração 003**, e
migração só entra quando alguém aperta "Aplicar atualizações do banco". O botão
não foi apertado — e ficou **dias** sem ser. Nesse período:

- o filtro **não era guardado**. A leitura caía no `except`, e a tela abria sem
  filtro. Em silêncio.
- o lote voltava a ser **um só, de todo mundo**: quem salvasse depois apagava o
  trabalho do outro sem aviso.

O dono digitava o nome todo dia achando que estava separando o trabalho dele, e
não estava. **Depender de um botão para uma coisa que a pessoa espera que "só
funcione" é um jeito de nunca funcionar** — a lição desta leva.

Agora há um segundo lugar, `analisesps.meta`, que existe desde a **migração
001** e portanto está no ar desde o primeiro dia. É (chave, valor), e a chave
carrega dentro dela a pessoa e a preferência (`pref:<pessoa>:<chave>`). O lote
de cada um usa o mesmo caminho.

> **E quando o botão finalmente for apertado, nada se perde.** A tabela boa
> passa a valer, e o que estiver no armário de reserva é **copiado para lá na
> primeira leitura**. Sem essa passagem, apertar o botão pareceria apagar os
> filtros e os lotes de todo mundo — o que teria sido um estrago causado
> justamente pela correção. Há teste para a passagem.

Detalhe que evita perder trabalho em andamento: quem ainda não salvou nada no
armário **herda uma vez** o lote antigo, o de quando ele era compartilhado.
Começar do zero seria o mesmo que apagá-lo.

**Verificado, e desta vez do jeito que importa:** 1424 testes verdes com
Postgres de verdade, e o fluxo inteiro exercitado contra um banco montado no
**estado exato da produção de hoje** (só as migrações 001 e 002 aplicadas):
a lista aparece na entrada, nome de fora da lista não entra, o filtro é
guardado e volta sozinho ao trocar de tela, e os lotes de MARCELO e THIAGO
ficam separados. Depois, aplicando 003 e 004 no mesmo banco, **os três
sobreviveram** e continuaram separados.

**O que NÃO foi verificado:** nada disto foi aberto num navegador de verdade —
são telas, e o teste confere o HTML, não o que o olho vê.

### Décima sétima leva (09/09) — a Obra sumida, e o defeito maior por trás dela

*"dentre as colunas não está aparecendo a coluna com a obra, muito
importante"* — e a Obra **estava** nas colunas padrão desde 05/09. O que
acontecia é mais amplo do que uma coluna:

**Uma coluna criada depois ficava invisível para sempre para quem já tinha
escolhido suas colunas.** A escolha guardada era lida como a lista COMPLETA do
que a pessoa quer ver. Uma escolha feita antes de 05/09 simplesmente não
mencionava a Obra — porque ela ainda não existia —, e o programa lia essa
ausência como *"ele não quer essa coluna"*. Sem nenhuma pista de que a coluna
existia, e sem jeito de descobrir a não ser abrindo a lista inteira.

Vale notar que **o botão de esconder a Descrição** (usado dez vezes por dia)
grava a lista inteira: bastava usá-lo uma vez para congelar as colunas
daquele dia e nunca mais ver nada criado depois.

**A correção guarda, junto com a escolha, QUAIS COLUNAS EXISTIAM na hora de
escolher.** O que nasceu depois disso e é padrão entra sozinho; o que a pessoa
tirou de propósito continua fora, porque estava entre as conhecidas. Assim a
próxima coluna que alguém criar não repete o problema.

> **A escolha antiga não diz o que conhecia**, e para ela o desempate é: as
> colunas padrão que estiverem faltando voltam, **uma vez**. Custa um clique a
> quem tinha escondido alguma de propósito; a alternativa era deixar a Obra
> invisível justamente para quem mais precisa dela. Da primeira gravação em
> diante a escolha volta a ser exata.

**Verificado com banco de verdade**, no estado da produção de hoje: com escolha
antiga guardada (sem a Obra), a Obra volta em **Solicitações e no Lote**, as
duas telas com o mesmo conjunto; escondendo a Descrição pelo botão em seguida,
a Descrição sai e a Obra fica; e tirando a Obra de propósito, ela fica fora
mesmo. 2819 testes verdes.

**Ficou um teste de baixo nível** só para a Obra não sair da lista padrão por
descuido, e outro para o formato guardado registrar as colunas conhecidas — é
esse registro que impede o defeito de voltar na próxima coluna criada.

### Décima oitava leva (09/09) — a segunda revisão de velocidade

*"continuo achando lento quando mudamos de aba, ou quando vai carregar os
dados após o filtro"*. A primeira revisão (décima quarta leva) tinha mexido só
em Solicitações. Desta vez a medição foi mais larga — e o maior achado não
estava no banco.

**Medido com as 59.055 SPs, num Postgres local; a produção não foi tocada:**

| Tela | Antes | Depois |
|---|---|---|
| Solicitações | 171 ms · 10 idas · **430 KB** | 177 ms · 10 idas · **27 KB** |
| Lote | 321 ms · 18 idas · 171 KB | **135 ms · 12 idas · 13 KB** |
| Relatório | 388 ms · 13 idas · 72 KB | **245 ms · 9 idas · 8,7 KB** |
| Auditoria | 228 ms · 9 idas · 6,6 KB | **149 ms · 6 idas · 1,6 KB** |

**1. O ACHADO PRINCIPAL: a página ia CRUA pela internet.** A tela de
Solicitações são **430 KB** de HTML — 200 linhas com vinte colunas —, e nada
no caminho comprimia. Comprimida dá **27 KB**: dezesseis vezes menos, por
1,4 ms de processamento.

> É a maior diferença de todas para quem está do outro lado, e explica por que
> ele continuava sentindo lentidão mesmo depois da primeira revisão: o banco
> podia responder em 100 ms, mas meio megabyte ainda leva segundos numa
> internet ruim ou no celular na obra. **Nenhuma otimização de consulta
> compensa isso** — e é o tipo de coisa que não aparece medindo o servidor.
>
> Feito com a biblioteca padrão, num `after_request` do próprio módulo: nada
> de dependência nova, e nada que atravesse para as outras áreas. Nível 1 de
> compressão de propósito — 6,3% do tamanho por 1,4 ms; o nível 6 chega a 4,4%
> gastando o dobro, e esta instância tem 2 GB e histórico de morrer de
> memória.
>
> **Três coisas ficam de fora, cada uma por um motivo:** o que sai em fluxo (a
> exportação CSV, escrita em blocos justamente para não abrir a base na
> memória — comprimir obrigaria a juntar tudo antes); o que já vem comprimido
> (PDF, xlsx); e o que é pequeno demais para valer. Há teste para os três, e
> para o navegador que não aceita comprimido continuar recebendo a página
> normal.

**2. O painel do Lote fazia OITO varreduras da base.** Uma lista e um resumo
para cada um dos quatro status de agendamento, cada um percorrendo as 59 mil
SPs: 185 dos 200 ms da tela. Agora são **duas** — `row_number` separa os
quatro grupos numa passada e devolve só as vinte de cada, em vez de mandar
oitocentas linhas para serem jogadas fora no Python.

**3. O Relatório somava quatro dimensões em quatro varreduras.** Projeto,
obra, tipo de despesa e conta são quatro perguntas sobre EXATAMENTE as mesmas
linhas. `GROUPING SETS` é a resposta que o Postgres já tem: uma varredura,
todos os agrupamentos juntos. Medido isolado: **183 ms → 96 ms**, com
resultado idêntico.

**4. A Auditoria contava quatro condições em quatro consultas.** Viraram uma,
com `FILTER` — o banco lê a tabela uma vez e incrementa quatro contadores.
Conferido: as quatro contagens batem exatamente com as de antes.

**Tentado e DESCARTADO nesta leva** (para não ser retentado por intuição):
- **Solicitações não melhorou em tempo de servidor**, e está certo assim: os
  177 ms restantes são somar 59 mil linhas para o rodapé (74 ms numa consulta
  só) e trazer a página. Somar o que o filtro alcança exige percorrer o que o
  filtro alcança. O ganho dela veio todo da compressão — 430 KB para 27 KB.
- **Índice de expressão** para as listas de filtro já tinha sido tentado e
  descartado na décima quarta leva; continua valendo.

**O que ficou de fora:** o `top_credores` do Relatório (59 ms, agrupa por
CPF/CNPJ) e o `numeros_do_relatorio` ainda são varreduras próprias. Dariam
para entrar no mesmo `GROUPING SETS`, mas agrupam por outra coisa e com outro
recorte — é mais risco do que os ~60 ms valem hoje.

**Verificado:** 2829 testes verdes com Postgres de verdade. Os testes novos
prendem a FORMA das consultas (`GROUPING SETS`, `row_number`, `FILTER`),
porque o efeito — a lentidão — só aparece com a base cheia, e aí é tarde.

**NÃO verificado:** os tempos são com o banco na mesma máquina. Na produção o
banco está noutro lugar e cada ida custa mais — por isso cortar o NÚMERO de
idas (10→6 na Auditoria, 18→12 no Lote, 13→9 no Relatório) vale ainda mais lá
do que aqui. E nada foi aberto num navegador de verdade.

### Décima nona leva (09/09) — a tela volta como estava

*"Eu filtro, vou para o Lote, volto para Solicitações — e ele refaz tudo de
novo. É como se eu tivesse duas abas do navegador e quisesse alternar entre
elas na hora."* A observação do dono estava certa, e era de concepção: **não
havia cache nenhum**. Toda troca de aba refazia as consultas e remontava a
tela inteira, mesmo três segundos depois.

**Agora a tela fica guardada no navegador por cinco minutos.** A volta não vai
ao servidor: aparece na hora, com o filtro e tudo. Cinco minutos foi escolha do
dono, com os riscos na frente.

**SÓ AS TELAS DE LEITURA ENTRAM** — Solicitações, Relatório, Auditoria e Log.
A razão é concreta e não é preciosismo: **Lote, Agenda, Ratear e Bradesco
recebem alterações NO PRÓPRIO ENDEREÇO** (o formulário manda para elas
mesmas). Guardá-las mostraria o estado ANTERIOR à mudança que a pessoa acabou
de fazer — que é pior do que ser lento. A **ficha da SP** também fica de fora:
ela mostra o status atual e tem botões que agem sobre ele.

As quatro que entraram só são alteradas por `/api/...`, e toda alteração por
lá termina recarregando a tela — o que substitui o que estava guardado. Há um
teste que prende a lista, porque entrar nela é uma decisão, não um detalhe.

> **O QUE FICA EM ABERTO, dito com todas as letras:** se OUTRA pessoa alterar
> algo, você pode ver o estado anterior por até cinco minutos. As redes de
> proteção já existiam e continuam valendo na tela guardada — o relógio no
> alto diz de quando é o dado, e a busca de 90 em 90 segundos avisa se a base
> mudou. Mas o atraso existe, e foi aceito.

**Sair apaga o que ficou guardado** (`Clear-Site-Data`). Sem isso, num
computador compartilhado, apertar Voltar depois de sair mostraria as telas da
pessoa anterior pelos minutos que faltassem. Sair tem de sair de verdade.

**A ROLAGEM E AS CAIXINHAS MARCADAS TAMBÉM VOLTAM.** A tela guardada voltava
no topo e sem as marcações — e quem marcou vinte SPs, foi conferir uma no Lote
e voltou, remarcava tudo. Ficam na memória da ABA (`sessionStorage`), não no
computador: fechou a aba, acabou. A chave inclui o endereço inteiro com o
filtro, então mudar o filtro não ressuscita a marcação de outra lista, e há
meia hora de validade para não trazer de volta uma seleção esquecida.

> A marcação reposta **nunca é invisível**: a barra do alto mostra quantas são
> e quanto somam, e nenhum botão age sobre ela sem confirmar.

### Vigésima leva (09/09) — enviar ao lote sem sair da tela

*"Ao enviar registro ao lote, não quero mudar de tela. Mantenha-se em
Solicitações, apenas avise que foi executada a ação."*

O botão mandava um formulário e levava a pessoa para o Lote — perdendo o
filtro, a rolagem e a marcação de quem só queria separar um grupo e continuar
conferindo a lista. Agora ele age no lugar e aparece um recado no canto
("12 SP(s) entraram no grupo Novo Lote 1"), com um link para quem quiser
conferir, que some sozinho em seis segundos.

A regra é a MESMA do formulário — grupo novo no topo, o que já estava fica
abaixo —, e é **chamada, não copiada**: duas cópias divergiriam no dia em que
uma delas mudasse. Há teste para as duas coisas.

**Verificado:** 2839 testes verdes com Postgres de verdade, e o envio ao lote
exercitado ponta a ponta contra o banco: as SPs entram, o grupo novo fica no
topo, o que já estava é preservado, e a resposta é 200 — não um
redirecionamento.

**NÃO verificado:** nada foi aberto num navegador de verdade. O comportamento
de guardar a tela depende do navegador respeitar o cabeçalho, e a reposição da
rolagem e das marcações é JavaScript — as duas coisas os testes não alcançam.
São as primeiras a conferir na tela.

### Vigésima primeira leva (09/09) — por que o cache não servia para nada

*"Não senti diferença nenhuma... nem indo nem voltando."* O dono estava certo,
e o defeito era meu: **o menu passava por fora do cache.**

O link da aba aponta para `/analisesps/solicitacoes`, **sem filtro**. O
servidor recebe isso, vê que há filtro guardado, e **REDIRECIONA** para
`/analisesps/solicitacoes?...&f=1`. Redirecionamento não se guarda — então
toda troca de aba ia ao servidor de qualquer jeito, e a cópia guardada, que
fica sob o endereço COM filtro, nunca era alcançada.

**A correção reescreve o link do menu no navegador**, apontando para o
endereço que a pessoa realmente usou. Sem redirecionamento, e a tela guardada
é servida na hora. Fica no navegador e não no servidor de propósito: montar
esses links no servidor custaria uma consulta a mais em TODA tela, inclusive
nas que não têm filtro nenhum — pagar em todas para economizar em duas.

> **A lição, e ela é geral:** eu publiquei o cache e disse "deve ficar
> instantâneo" sem ter como exercitar um navegador de verdade. O teste
> conferia o cabeçalho da resposta, que estava certo; o que estava errado era
> o CAMINHO que o navegador percorria até ela. Ficou um teste fixando o
> defeito — o endereço sem filtro redireciona e não é guardável — para
> ninguém "consertar" o link de volta.

**Um suspeito para o "às vezes demora alguns segundos", que cache nenhum
explica.** Medido nesta máquina: **subir o serviço custa 1,7 s** (importar os
18 módulos), e numa máquina rápida. O `Procfile` manda o gunicorn **reciclar o
worker a cada 150 requisições** (`--max-requests 150`), e com `--workers 1`
isso significa que, a cada ~150 requisições, TODA requisição espera essa
partida. Na instância do Render, de 2 GB e compartilhada, é razoável supor
vários segundos.

> **NÃO MEXI NISSO, e é decisão do dono.** O `Procfile` governa os 18 módulos,
> não só este; e o valor 150 foi posto justamente para conter o estouro de
> memória de julho de 2026 (`CONTEXTO.md` §9). Aumentar troca segurança de
> memória por velocidade. Some-se a isso a divergência já anotada no
> `CLAUDE.md`: há indício de que a produção rode com 8 threads via o campo
> *Start Command* do Render, que sobrescreve o `Procfile` — ou seja, não se
> sabe ao certo qual dos dois vale hoje. **Conferir isso é o primeiro passo**
> antes de qualquer ajuste.

**O botão de atualizar saiu de Configurações e foi para o lado da hora da
base**, em todas as telas. É a mesma "Atualização do dia"; só mudou de lugar —
quem olha a hora e acha que está velha quer atualizar ali, não noutra tela. Só
aparece para quem opera, e a porta já recusava quem só consulta.

**O que continua NÃO sendo medido, e é o limite honesto desta sessão:** o
proxy desta máquina **bloqueia o domínio da empresa**, então não consigo
cronometrar a produção. Todos os números aqui são locais, com o banco na mesma
máquina. A diferença entre eles e o que o dono sente é justamente onde mora o
que falta descobrir.

### Vigésima segunda leva (10/09) — a primeira medição da PRODUÇÃO

O dono mandou **a tela de rede do navegador dele**, aberta na produção. É a
primeira vez que esta área tem número de lá em vez de número desta máquina — e
ela mudou o diagnóstico em três pontos.

**O que a tela dele provou que estava CERTO:**

- **Solicitações vem do cache, "0 ms".** A correção do menu da leva anterior
  funcionou. A dúvida dele era legítima e a resposta é: funcionou, sim.
- **A compressão funciona.** O Lote aparece com 41 kB trafegados para 403 kB
  de página.

**O que a tela dele mostrou de errado, e foi corrigido:**

| O que aparecia | Custo | Correção |
|---|---|---|
| `analisesps.css` e `analisesps.js` respondendo "não mudou nada" | 402 ms + 423 ms **em toda tela** | valem um ano e `immutable`; o endereço carrega a versão publicada |
| `favicon.ico` dando 404 | uma ida perdida por tela | uma linha no cabeçalho |
| a rotina que pergunta a hora da base | **1.463 ms** | lê só o carimbo — 6 ms medidos |
| o Lote | 2.828, 2.936 e 4.055 ms | ver abaixo |

**A CONTAGEM DA BASE, que era o custo escondido em TODA tela.** Aqueles
1.463 ms da rotina da hora não tinham como vir de outro lugar: ela só fazia
duas coisas, e uma delas era `SELECT count(*) FROM analisesps.sps`. No
Postgres isso **percorre a tabela inteira** — e `base_carregada()` é chamada
por toda tela do módulo, para saber se a base foi carregada e para escrever
"de 59.055 na base" embaixo do total.

Nesta máquina a mesma contagem custa **5 ms**. A diferença é o banco de lá: a
base é reescrita a cada carga, e as linhas mortas ficam ocupando espaço até o
faxineiro automático do Postgres passar — a tabela que ele percorre é muito
maior do que as 59 mil linhas vivas.

A correção é a mesma ideia das listas de filtro, e pelo mesmo motivo:
**contar uma vez por carga, não uma vez por tela**. Quem conta agora é a carga
e a sincronização, no processo separado onde um segundo a mais não incomoda
ninguém; o número fica em `analisesps.meta` junto da hora a que se refere.
Conferido: **nenhuma das seis telas percorre a tabela para contar**, e há
teste que falha se voltar a percorrer.

> **De brinde, um defeito pequeno que ninguém tinha reportado:** a carga
> inicial não anotava a hora — só a sincronização do dia anotava. Quem fizesse
> a primeira carga via "base de —" no alto até a primeira sincronização
> passar, justamente no dia em que ninguém sabe se deu certo. Agora as duas
> anotam.

> **O limite, dito sem rodeio:** se alguém acrescentar ou apagar linhas POR
> FORA da carga e da sincronização, o número mostrado fica velho até a
> próxima. Hoje ninguém faz isso — a fila de volta altera SPs que já existem,
> não cria nem remove.

**O LOTE PASSOU A FICAR GUARDADO NO NAVEGADOR.** Era metade da ida e volta que
o dono reclamava (*"permaneceu a demora entre o Lote e as Solicitações"*): as
Solicitações já ficavam guardadas, o Lote não, então o caminho continuava
lento numa das direções.

Ele tinha ficado de fora **de propósito**, e a razão continua válida: é tela
que recebe alteração no próprio endereço. O que mudou é que agora há duas
travas, e só com as duas isso deixa de ser aposta:

1. A tela que volta de uma salvada traz `?aviso=` e **não** é guardada. Senão
   o recado de "salvo" reapareceria minutos depois, dizendo que algo acabou de
   acontecer quando não aconteceu.
2. A tela carrega **a hora em que o lote foi salvo**, e o navegador compara
   com a última que viu. Se a cópia guardada for anterior à última salvada,
   ela se recarrega sozinha, uma vez.

> **Por que a trava 2 existe, se a regra do HTTP já cobre isso.** A regra diz
> que um POST apaga a cópia guardada daquele endereço, e todo salvamento do
> Lote é um POST para o próprio endereço. Mas o preço de o navegador não
> cumprir seria a pessoa ver o lote SEM o que acabou de fazer e salvar por
> cima do próprio trabalho. Isso não se aposta em regra alheia. A recarga só
> dispara quando a tela veio DO CACHE (conferido pelo tamanho trafegado) —
> sem essa condição, a tela que volta de uma salvada se recarregaria à toa a
> cada salvamento.

**Medido aqui, com as 59.055 SPs e um lote de 150 SPs** (o banco na mesma
máquina, então os números absolutos não são os de lá):

| Tela | Tempo | Idas ao banco | Página |
|---|---|---|---|
| Solicitações | 208 ms | 7 (era 10) | 429 KB → ~27 KB comprimida |
| Lote | 176 ms | 7 (era 8) | 471 KB → ~45 KB comprimida |
| Relatório | 317 ms | 7 (era 9) | 72 KB |
| Auditoria | 194 ms | 5 (era 6) | 7 KB |

Uma ida a menos por tela é a contagem que saiu. **Aqui isso quase não aparece
no relógio — são 5 ms.** É lá que vale 1,4 segundo, e é honesto dizer que
essa parte NÃO foi medida na produção.

**O QUE CONTINUA EM ABERTO, e é onde eu apostaria o próximo olhar.** Os 2,8 a
4,0 segundos do Lote na produção **não são explicados** pelo que consigo ver:
41 kB comprimidos não levam três segundos, e sete consultas num banco na mesma
região também não. Sobram três suspeitos, nenhum deles verificável daqui:

1. **A partida do serviço.** Já anotada na leva anterior: `--max-requests 150`
   com `--workers 1` faz o worker reiniciar a cada ~150 requisições, e subir
   custa 1,7 s nesta máquina. É decisão do dono, e o primeiro passo é
   conferir se a produção roda com o `Procfile` ou com o *Start Command* do
   Render.
2. **O banco de lá**, pelo mesmo motivo que fazia a contagem custar 1,4 s. Se
   for isso, a contagem que saiu já ajuda, e o resto some com uma faxina
   (`VACUUM FULL` / `REINDEX`) — que não é coisa para fazer sem combinar,
   porque tranca a tabela enquanto roda.
3. **O tamanho da página do Lote**: 471 KB crus com 150 SPs no lote mais os 80
   do painel por status. Comprimida é pouco na rede, mas o navegador ainda
   monta ~230 linhas de vinte colunas. Dá para carregar o painel só depois da
   tela aparecer — **não foi feito**, porque muda o que a pessoa vê ao abrir e
   a regra da casa é fazer como o Streamlit fazia.

**NÃO VERIFICADO, e é o mesmo limite de sempre:** o proxy desta máquina
bloqueia o domínio da empresa. Todos os tempos são locais. E as duas travas do
Lote guardado são JavaScript — os testes conferem que o código está lá, não
que o navegador obedece. **É a primeira coisa a conferir na tela:** salvar o
lote, ir às Solicitações, voltar, e ver se o que foi salvo está lá.

### E a resposta apareceu no mesmo dia: o banco tem um décimo de um núcleo

Ainda em 10/09, o dono mandou as métricas do serviço de banco (`erp-db`). Elas
fecham a investigação, e o achado é maior do que esta área:

| | Limite do plano | Uso observado |
|---|---|---|
| CPU | **0,1 CPU** — um décimo de um núcleo | picos de 0,06 a 0,08: **60% a 80% do limite** |
| Memória | **0,25 GB** | 100 a 230 MB — **encostando no teto** |
| Disco | 1 GB | ~430 MB |

**São 430 MB de dados para 250 MB de memória.** Os dados não cabem, e o
Postgres ainda precisa de parte dela para outras coisas. Toda varredura da
tabela de SPs vai ao **disco** — sempre, não há cache que a segure. E vai ao
disco com um décimo de um núcleo, com o banco já estrangulado nos picos.

Isso explica os 1.463 ms da contagem sem sobrar nada: aqui, com 4 núcleos e o
dado quente na memória, a mesma consulta custa 5 ms. E explica os 2,8 a 4,0
segundos do Lote, que faz várias varreduras.

> **A conclusão que muda a estratégia desta área:** o trabalho de tirar
> varreduras — feito nas levas 14, 18 e 22 — **valeu, e vale ainda mais neste
> banco do que valeria num banco folgado**. Mas há um teto: nenhuma
> otimização de consulta torna rápida uma leitura de disco com 0,1 CPU. Dá
> para diminuir o NÚMERO de varreduras, não para torná-las rápidas.
>
> **Antes de gastar mais esforço aqui, subir o plano do banco tem efeito
> maior.** É decisão do dono, e o banco serve ERP, painel e esta área juntos —
> está registrado em `CONTEXTO.md` › "Histórico de decisões".

**Se o plano NÃO subir, o que ainda dá para fazer daqui**, em ordem de
proveito: (1) o painel por status do Lote sai numa varredura da tabela inteira
— um índice sob medida a transformaria em leitura de índice; (2) carregar esse
painel só depois da tela aparecer, para o lote em si abrir na hora; (3) as
contagens do topo das Solicitações, que também varrem. Nenhuma das três foi
feita, e as três são mais arriscadas do que o que já está aqui.

**Não verificado:** os números vieram da tela do Render, lida numa imagem. Os
testes não alcançam o banco de produção, então o tamanho de cada tabela lá
dentro não foi conferido.

### E aí a tela do banco entregou o culpado: 14,3 MILHÕES de gravações

Na mesma leva o dono mandou a aba de consultas do banco. A lista de "quem mais
chama" é a coisa mais reveladora que esta área já teve:

| Consulta | Chamadas | Tempo total |
|---|---|---|
| `INSERT ... analisesps.sp_fiscal` | **14.328.805** | 34 min 30 s |
| `INSERT ... analisesps.sps` | 662.556 | 11 min 27 s |
| `INSERT ... rateio` (painel) | 388.029 | 44 s |

**A documentação fiscal é, disparada, a consulta mais chamada de todo o
banco** — vinte e uma vezes mais que a gravação das próprias SPs. E são só uns
15 a 20 mil registros.

**A causa, e ela é uma linha de SQL.** `sincronizar_apoios()` lia a planilha
fiscal inteira e gravava TODAS as linhas, sempre — com `ON CONFLICT DO UPDATE`
sem condição nenhuma. Como essa etapa roda em toda sincronização, e a
sincronização é disparada de 5 em 5 minutos por quem estiver com a tela
aberta, o resultado é dezenas de milhares de gravações a cada cinco minutos
para reescrever exatamente os mesmos valores.

> **E no Postgres reescrever com o mesmo valor NÃO é de graça.** Cada
> reescrita deixa a versão antiga como lixo, para o faxineiro automático
> recolher depois. Dezenas de milhares de linhas de lixo a cada cinco minutos
> é o que engorda a tabela até ela não caber mais na memória do banco —
> **exatamente a lentidão que se estava caçando**. O sintoma e a causa se
> alimentavam.

**Duas correções:**

1. **`WHERE ... IS DISTINCT FROM`** nas duas gravações de apoio (documentação
   fiscal e contas por centro de custo): o banco só grava quando o valor mudou
   de verdade. Resultado final idêntico; o que some é o trabalho inútil.
   **Conferido contra um Postgres de verdade** olhando a versão interna de
   cada linha: a que não mudou continua com a versão original — não foi
   tocada; a que mudou ganhou versão nova. E o contador de atualizações do
   banco marca **uma**, não duas.

2. **A sincronização automática relê as planilhas de apoio no máximo de hora
   em hora**, e não a cada cinco minutos. Elas são dado de apoio — mudam
   raramente — e cada passagem ainda baixa a planilha inteira do Google, na
   instância de 2 GB que já morreu de memória uma vez. **A trava vale só para
   o disparo automático:** o botão de atualizar e o modo "Só as planilhas de
   apoio" continuam imediatos, e há teste prendendo isso.

> **O que fica em aberto por escolha:** um documento fiscal cadastrado na
> planilha pode levar até uma hora para aparecer, se ninguém apertar o botão.
> Antes eram cinco minutos. É dado de apoio, e o caminho imediato continua
> existindo — mas está escrito aqui para não ser descoberto por susto.

**A ordem de grandeza do que isso devolve:** eram 14,3 milhões de gravações e
34 minutos de processador num banco que tem **um décimo de um núcleo**. Some
quase tudo. É, de longe, a maior economia desta sessão — e não veio de medir a
tela, veio de olhar o que o banco estava fazendo.

### INCIDENTE (10/09) — o Relatório estourava sempre que havia filtro

*"Veja quando clico em relatório: Deu erro."* Reportado pelo dono minutos
depois da publicação da 22ª leva. **O defeito era meu, e estava no ar desde a
18ª leva (09/09)** — não veio da publicação de hoje.

**O que acontecia:** o Relatório abria normalmente **sem filtro** e estourava
**com qualquer filtro que tivesse valor** (uma lista suspensa, a busca, uma
faixa de valor, um período). Só os filtros de "situação" escapavam.

**A causa, em uma frase:** as somas das quatro dimensões saem de uma varredura
só (`GROUPING SETS`, 18ª leva), e os parâmetros estavam sendo passados **fora
da ordem em que aparecem no texto do SQL** — o WHERE antes dos `CASE`
repetidos dentro do `GROUPING(...)`.

Sem filtro, as duas ordens coincidiam por acaso e tudo funcionava. Com filtro,
os `CASE` do `GROUPING` recebiam o valor do filtro, deixavam de ser idênticos
aos do `SELECT`, e o banco recusava a consulta inteira.

> **POR QUE PASSOU POR TODA A SUÍTE, e é a lição que fica.** A sessão dublada
> **ignora WHERE** — lá o filtro nunca vira parâmetro de verdade. E os testes
> com banco de verdade que existiam somavam **sem filtro nenhum**, que era
> exatamente o único caso que funcionava. O buraco é o que este arquivo de
> testes existe para tapar, e ele estava aberto bem no meio.
>
> Mais fundo ainda: eu escrevi na 18ª leva que os testes "prendem a FORMA das
> consultas". Prender a forma **não é o mesmo que exercitar a consulta**. Uma
> consulta pode ter a forma certa e a ordem dos parâmetros errada.

**Ficaram onze testes com banco de verdade**, cobrindo sete filtros diferentes
em duas frentes: que o banco ACEITA a consulta, e que o número que ela devolve
**bate com a soma feita uma dimensão por vez** — porque uma ordem errada pode
não estourar e ainda assim somar a coisa errada, e aí ninguém percebe.
**Conferido que os onze falham sem a correção e passam com ela.**

**E foi feita uma varredura de todas as telas contra todos os filtros** —
285 endereços, cada tela do módulo contra 19 filtros diferentes. Fora o
Relatório, nada mais estourou. Os dois casos que respondem 400 respondem de
propósito, com recado ("Nada para exportar", "Lote vazio").

### INCIDENTE (10/09) — o botão que a tela mandava apertar falhava calado

*"Ratear... 'As listas de obras e categorias ainda não foram carregadas'.
Pelo que entendi essa mensagem é corrigida no botão 'Só as planilhas de
apoio', mas já cliquei e não atualizou."*

**Primeiro, o que NÃO era:** a trava de uma hora que entrou na 22ª leva vale
**só para o disparo automático**. O botão manda `disparo="manual"` e nunca é
travado. Conferido no código e com teste.

**O que era:** as listas do rateio vêm das abas "C. Diários" (colunas "Obra" e
"Código") e "Plano Financeiro" (colunas "Categoria" e "Código"). Se a aba tem
outro nome, se a coluna tem outro nome, ou se a aba está vazia, a leitura
devolvia lista vazia, um `continue` pulava, e o único registro do motivo ia
para o **log do serviço** — que o dono não tem como ler.

Resultado: a tela mandava apertar o botão, o botão dizia "concluída", e nada
mudava. Sem nenhuma pista. **Botão que a tela manda apertar não pode falhar
calado** — a pessoa aperta de novo, e de novo, e conclui que o sistema está
quebrado.

**Piorava porque a mensagem final do modo "apoios" era "0 SPs em 0.1 min."** —
este modo não traz SP nenhuma, então ele SEMPRE terminava dizendo zero. A tela
parecia dizer "não aconteceu nada" justamente quando algo tinha acontecido.

**A correção: o motivo passa a chegar à tela, com o que resolve.** A mensagem
da execução — que Configurações mostra logo abaixo do botão — passa a dizer o
que veio e o que não veio:

```
documentação fiscal: 1 · contas: 1 · obras: 0 · categorias: 1 —
a aba "C. Diários" não tem a(s) coluna(s) "Obra", "Código".
O cabeçalho dela é: Centro de Custo, Cod.
```

Os três casos ficam distintos, e cada um manda a pessoa para um lugar
diferente da planilha:

| O que aconteceu | O que a tela diz |
|---|---|
| aba com outro nome | `a aba "C. Diários" não existe nesta planilha. As que existem são: …` |
| coluna com outro nome | `não tem a(s) coluna(s) "Obra". O cabeçalho dela é: …` |
| aba certa e vazia | `tem as colunas certas, mas nenhuma linha preenchida em "Obra"` |

Listar o que a planilha REALMENTE tem é o que transforma "não carregou" em
algo que se resolve sozinho, sem precisar de outra sessão.

A tela de Ratear também mudou: em vez de só mandar rodar a sincronização, ela
diz onde está a explicação se a pessoa já tiver rodado.

**Verificado ponta a ponta contra o banco**, com a planilha dublada, nos quatro
cenários — tudo certo, aba com outro nome, coluna com outro nome, aba vazia —
lendo a mensagem que ficou gravada na execução. É o texto da tabela acima.

**E o recado respondeu na primeira tentativa.** O dono publicou, apertou o
botão, e a tela disse:

> documentação fiscal: 13.695 · contas: 182 · obras: 0 · categorias: 0 — a aba
> "C. Diários" não tem a(s) coluna(s) "Obra", "Código". O cabeçalho dela é:
> **Código Primário, Conta de Pagamento, Projeto, Código Omie**. A aba "Plano
> Financeiro" não tem a(s) coluna(s) "Categoria", "Código". O cabeçalho dela
> é: **Plano Financeiro, Código Omie**.

**A causa raiz, e ela é mais velha do que parecia: a conversão do Streamlit
PERDEU a lista de nomes aceitos por coluna.** O original procurava
`"Código Primário"` e, **só se não achasse**, `"Obra"`; e `"Código Omie"`
antes de `"Código"`. A conversão ficou com a segunda opção de cada par —
justamente a que a planilha não tem.

Ou seja: **a lista do rateio nunca carregou, desde a estreia do módulo.**
Ninguém tinha percebido porque a tela só dizia "ainda não foram carregadas",
que soa como "falta rodar a sincronização", e não como "está quebrado".

> **A regra da casa resolveu isto, e vale registrar que resolveu:** *em
> dúvida, faça como o Streamlit fazia*. O código original está recuperável no
> histórico do git (`git show dab6ee2^:app/apps/analisesps/app/gsheets.py`), e
> foi ele quem deu a resposta — não a adivinhação. Os nomes voltaram na mesma
> ordem de preferência que ele usava.

Voltou junto uma segunda coisa que a conversão tinha perdido: **linha sem o
Código Omie fica de fora**. Sem o código a linha não serve para gerar o JSON,
e oferecê-la na lista levaria a pessoa a montar um rateio que o Omie recusa —
e ela só descobriria na hora de lançar.

**Verificado contra o banco, com o cabeçalho REAL das duas abas:** as obras e
as categorias entram com o Código Omie certo, o nome vai para a lista e o
código para o JSON (trocar os dois geraria um lançamento no lugar errado, e há
teste prendendo a ordem), e a linha sem código não entra. Os nomes antigos
também continuam funcionando, com teste.

### Vigésima terceira leva (10/09) — colar uma tabela no Ratear

*"Imagina que eu tenho trinta obras para ratear. Se eu for colocar uma a uma é
trabalhoso, e essa informação normalmente vem de uma planilha do Excel. Queria
poder copiar e colar uma tabelinha e o sistema já interpretar."*

Feito, nos dois lados — centro de custo e categoria de despesa. Uma caixa
**fechada por padrão** em cada cartão (quem rateia duas obras não precisa
dela), com um botão "Interpretar o que colei".

**A interpretação é no SERVIDOR, e essa foi a decisão de projeto.** O caminho
óbvio era fazer no navegador — instantâneo, sem recarregar. Mas esta sessão
inteira ensinou que **o que roda no navegador esta máquina não consegue
exercitar**, e um rateio na obra errada não avisa: o Omie aceita e lança. No
servidor, a interpretação tem teste de verdade e reusa o `_to_float`, que já
sabia ler "1.234,56", "R$ 994,12" e até colagem em padrão americano.

**O que ela entende**, cada um testado porque cada um é um jeito real de
copiar: a tabulação do Excel, o ponto e vírgula do CSV, duas colunas separadas
por espaços, um espaço só, com "R$" na frente, com o cabeçalho colado junto e
com linhas em branco no meio. **O valor é o último pedaço que parece número, e
o nome é tudo o que vem antes** — é isso que faz funcionar com qualquer
separador e com nome que tem espaço no meio ("CRECHE SWAP 3").

**A REGRA QUE GOVERNA O RESTO: nunca adivinhar.** Nome que não bate NÃO entra
— volta escrito na tela. E toda interpretação que não seja o nome exato
aparece no recado, porque acertar a obra errada é pior do que não achar
nenhuma.

| O que foi colado | O que acontece |
|---|---|
| o nome exato | entra, sem recado |
| o código do Omie | entra, e o recado diz que foi pelo código |
| o nome pela metade, sem dúvida | entra, e o recado pede para conferir |
| o nome pela metade, com dúvida | **não entra**, e o recado diz com quais combinou |
| nome que não existe | **não entra**, e o recado diz qual |
| valor que não é número positivo | **não entra**, e o recado diz qual linha |
| a mesma obra duas vezes | entram as duas, com recado |

> **O erro que eu mesmo cometi e o teste pegou:** a primeira versão comparava
> "um contém o outro", e **"OBRA-1" casava com "OBRA-12"**. É exatamente o
> erro que não pode acontecer. A comparação passou a ser por COMEÇO, com
> desempate pelo nome mais longo, e só quando não sobra dúvida. Há teste com
> duas obras de nome parecido conferindo que ela RECLAMA em vez de escolher —
> e conferido que ele falha na versão errada.

**Três detalhes que só aparecem usando:**

1. **Interpretar um lado não apaga o outro.** O que já estava digitado nas
   categorias, e a base, voltam intactos. Tem teste.
2. **Apertar Enter num campo continua GERANDO, e não interpretando.** O
   navegador usa o primeiro botão de envio do formulário, que passou a ser o
   "Interpretar" da caixa de cima; um botão escondido de "gerar" ficou antes
   de todos. Tem teste prendendo a ordem.
3. **O texto colado volta para a caixa, e ela fica aberta** ao lado do recado
   — quem precisa corrigir uma linha não cola tudo de novo.

**De brinde, uma feiura antiga:** a tela mostrava uma caixa chamada **"erro"
com a palavra "None" dentro** sempre que dava tudo certo — o laço percorria as
três chaves que o gerador devolve. E, quando dava erro de verdade, o motivo
aparecia dentro de uma caixa de copiar, como se fosse para colar no Omie.
Agora o erro é um aviso vermelho e as caixas mostram só os JSONs.

**Verificado contra o banco, ponta a ponta pela tela:** colar preenche as
linhas com a obra certa selecionada e o valor no campo, o outro lado e a base
sobrevivem, o recado aparece, e o JSON gerado em seguida sai com os códigos do
Omie certos e os percentuais fechando 100%.

**NÃO verificado:** nada foi aberto num navegador de verdade. A caixa que abre
e fecha é `<details>`, do próprio HTML, sem JavaScript — mas o efeito de colar
com Ctrl+V uma seleção do Excel de verdade não foi visto. **É a primeira coisa
a conferir na tela.**

### Vigésima quarta leva (11/09) — seis defeitos que o dono achou usando

**1. A procura dentro do filtro não filtrava nada.** *"No filtro tipo de
despesa existe o campo, mas se eu escrever, ele não está filtrando as
possibilidades."*

O javascript estava certo e funcionando. **O ESTILO é que anulava.** O
navegador esconde `[hidden]` com `display: none`, mas isso vem da folha DELE —
e qualquer regra nossa ganha, por mais fraca que seja. Como `.opcao` tem
`display: flex`, a opção era marcada como escondida e continuava na tela,
parada, enquanto a pessoa digitava.

A correção é uma linha (`[hidden] { display: none !important; }`) e vale para
a folha inteira de propósito: **o mesmo tropeço aconteceria em qualquer
elemento com `display` próprio** que alguém mandasse esconder — e já havia
outros. Há teste, e conferido que ele falha sem a correção.

> **A lição, e ela é do mesmo tipo das outras desta semana:** o código estava
> lá, o teste do código passaria, e mesmo assim a função não existia para quem
> usa. Conferir que o código está escrito não é conferir que ele funciona.

**2. Faltava o total POR CONTA do que está marcado.** *"Aparece o total dos
selecionados; era só o total por conta que estava faltando."*

O total geral diz se a remessa é **grande**; o total por conta diz se ela
**cabe** — é por conta que o dinheiro sai. Agora aparece embaixo do total, na
barra do alto, ordenado do maior para o menor (com seis contas, a que importa é
a que concentra), e **some quando há uma conta só**, porque aí repetiria o
número que está logo acima.

**3. A exportação e o PDF do lote entregavam um lote CONGELADO.** *"Eu
atualizei o lote, e o relatório permanece desatualizado."*

As duas rotas chamavam `lote.ler()` **sem a pessoa**. O argumento tinha valor
padrão `""` — e `""` é o **lote antigo**, de quando ele era um só e
compartilhado, parado no tempo desde que o lote passou a ser de cada um
(migração 003). Ou seja: a pessoa salvava o lote dela, e o arquivo saía com
outra coisa. **Sem erro nenhum**, porque um lote congelado não estoura: ele só
fica errado.

> **Como isso sobreviveu à suíte, e é a parte que incomoda:** havia teste do
> PDF do lote. Ele dublava `lote.ler` com uma função **sem argumento** — ou
> seja, **imitava exatamente a chamada errada**, e por isso passava. O teste
> não estava conferindo o comportamento; estava congelando o defeito.

**A correção fecha a armadilha, e não só o buraco:** `ler` e `salvar`
perderam o valor padrão da pessoa. Quem esquecer de passar agora quebra alto,
na hora. Quem quiser mesmo o lote antigo chama `lote_de_antes()`, que diz isso
no nome. Há teste prendendo a ausência do padrão, e outro conferindo que as
duas rotas leem o lote da pessoa logada — conferido que ele falha com o
defeito de volta.

**4. A tela do Bradesco ficava em branco.** *"Cliquei conferir e ficou tudo em
branco"*, com o texto colado junto.

O interpretador estava **certo**. Reproduzido com o texto dele: o **mesmo
texto com tabulação dá duas operações; com espaços, nenhuma**. Copiar a tabela
do Bradesco traz tabulação na maioria das vezes — não sempre, e depende do
navegador e de como a seleção é feita. Quando vinham espaços, o texto inteiro
era ignorado **em silêncio**.

Duas correções, e a segunda vale mais do que a primeira:

- a linha da operação passa a ser separada por tabulação **ou por dois ou mais
  espaços**. É seguro porque uma linha só vira operação se tiver, ao mesmo
  tempo, data, agência|conta e valor — e nome com espaço simples ("JOSE THIAGO
  DA SILVA") continua inteiro. Há teste com o texto real do dono, nas duas
  formas, e conferido que ele falha com o defeito de volta.
- **a tela deixa de ficar muda.** Quando não reconhece nada, ela diz o que
  precisa haver na linha e quantas linhas foram coladas. Ficar em branco é o
  pior resultado possível: quem colou não sabe se o sistema leu, se travou, ou
  se não havia o que conferir.

**De brinde, um defeito que ninguém tinha reportado:** a caixinha "focar nos
agendados" **não desligava**. Caixinha desmarcada não chega no formulário, e o
valor padrão entrava justamente aí — então marcar ou desmarcar dava no mesmo.

**5. O título do grupo que esvazia na limpeza vai junto.** *"Quando limparmos
um lote tirando pagas e canceladas e ele estiver vazio, apagar o cabeçalho."*

**Mas só quem esvaziou AGORA.** Um grupo que já estava vazio antes continua:
alguém escreveu aquele título de propósito, para encher depois, e apagar o que
a pessoa acabou de digitar seria pior do que o cabeçalho sobrando.

**6. A marcação voltava depois de a pessoa agir — e esse defeito é meu.**
*"Para toda ação que faço no lote, tipo marcar agendado, agendar... são
reaplicadas seleções que talvez estejam salvas. Está errado. Eu já desmarquei.
Não pode retroagir."*

A memória da marcação (19ª leva) existe para quem **sai da tela e volta**. Mas
depois de uma ação a tela recarrega, e a marcação era reposta — fazendo as SPs
voltarem marcadas **depois de já terem sido tratadas**.

> **E não é só incômodo:** uma marcação que reaparece sozinha convida a agir
> duas vezes sobre a mesma SP — agendar de novo, mandar ao lote de novo. O
> incômodo era o sintoma; o risco era o problema.

Agora **agir sobre a seleção apaga a memória dela**. As quatro ações que
alteram alguma coisa chamam isso; a tela de QR **não**, de propósito — ela não
altera nada, só abre outra tela, e quem volta de lá quer a seleção inteira de
volta. Há teste para as duas coisas.

### Vigésima quinta leva (11/09) — as críticas da conciliação fiscal

A conciliação já sabia **casar** nota com lançamento (24ª leva). Esta leva é a
outra metade: decidido o par, **o que se faz com ele**. É a regra de negócio da
tela nova de Documentação Fiscal, escrita e travada em teste antes de existir
tela — porque é aqui que mora o risco, não no desenho.

**A correção do dono que reescreveu o miolo.** Foi proposto apontar como
divergência o caso "o tipo de despesa é Material Elétrico, mas o card está como
Não Dedutível". Ele recusou:

> *"Categoria de despesa não vai ser regra para dedutibilidade ou não, porque
> você pode comprar um material elétrico sem nota fiscal. Então nesse caso vai
> ser não dedutível. O fato de ter a nota fiscal é que vai ser o balizador.
> A simples divergência de material elétrico nem adianta mostrar."*

Ele está certo, e isso derrubou uma regra inteira que já estava escrita aqui: a
que sugeria categoria **por palavra** no tipo de despesa ("material" → NF-e).
Sugerir NF-e para uma compra feita sem nota é propor dedução de despesa que não
dá dedução — o erro exato que ele apontou. **A regra foi removida, e um teste
guarda a remoção**: se alguém a reintroduzir, a suíte quebra citando a frase
dele. Conferido que o teste falha quando a regra volta.

**O que sobrou para o tipo de despesa, e só isso:** as sete despesas que
**nunca** têm nota eletrônica — aluguel tem contrato, veículo tem apólice, água
e energia têm fatura, cartório tem taxa. Nessas, e apenas quando nenhuma nota
foi encontrada, ele diz qual documento procurar. Fora delas, sem nota o sistema
**cala**.

**E a categoria não precisa mais ser adivinhada: ela está DENTRO da chave.** Os
dígitos 21 e 22 da chave de acesso são o modelo do documento, por definição da
Receita — 55 é NF-e, 57 é CT-e, 65 é NFC-e. Conferido nas chaves reais da
planilha do dono, e bate exatamente com o que ele classificou à mão. **Achada a
nota, a categoria é certeza, não palpite.** É essa diferença que autoriza o
sistema a propor em lote.

**O que a tela vai apontar, em ordem de urgência:**

| O que | O que o sistema faz |
|---|---|
| A nota que está no card está **cancelada** | Aponta como crítico. **Nunca propõe** — pagar contra documento cancelado é decisão de gente |
| A chave do card **não é desta SP** | Levanta a suspeita de notas trocadas entre dois lançamentos |
| O card diz "não há nota" e **a nota foi encontrada** | **Propõe a correção**, com a categoria lida da chave |
| O card afirma NF-e e **não há chave nem nota** | Levanta dúvida — pode ser classificação sem documento, pode ser nota que ainda não veio |
| Nada encontrado, nada afirmado | Diz que **procurou e não achou** — que é diferente de não ter procurado |

**Duas pilhas, e elas existem por causa de um risco real.** Propor cria fadiga
de aprovação: se vinte e oito de trinta estão sempre certas, na terceira semana
ninguém confere mais — é o mesmo olho cansado, só que mais rápido. Por isso o
que tem dúvida **não vem marcado** e é decidido um a um; só o que não tem dúvida
nenhuma vai marcado para aprovação em lote.

**Um achado que nasceu escrevendo os testes.** Quando o card tem chave mas a
nota não veio no relatório do FSist, ainda dá para conferir alguma coisa **sem
o relatório**: o CNPJ de quem emitiu está dentro da própria chave. Se ele não é
o do credor da SP, a chave veio de outro lançamento — a troca de anexo
detectada sem depender de achar a nota certa. Antes disso, esse caso caía num
"nada a apontar" silencioso.

**A revarredura que o dono pediu está travada em teste:** "Emissão Futura" e
"Não Dedutível" — e mais cinco categorias de ausência — voltam a ser
examinadas a cada relatório novo do FSist. A nota que faltava em julho pode
estar no relatório de setembro, e era justamente esse o caso que ele descreveu.

**A segunda visão também entrou:** as notas emitidas contra a BWS que não estão
em lançamento nenhum. É ela que fecha com a contabilidade — *"se tem uma nota
emitida, tem uma despesa para estar associada"*. As canceladas ficam de fora de
propósito: nota cancelada sem despesa é o esperado, não um achado.

**Verificação.** 4.286 testes verdes com Postgres de verdade (descartável,
nesta máquina — a produção não é alcançável), 129 pulados. 26 testes novos,
um por caso. Os dois que guardam decisões do dono foram conferidos **quebrando
o código de propósito** para provar que mordem. A aplicação sobe com os 18
blueprints.

**O que NÃO foi feito ainda, e é o próximo passo:** a tela em si. Hoje isto é
regra sem interface — nada disso aparece para ninguém. Faltam também a gravação
em lote de volta no Pipefy e a leitura dos anexos por IA, que o dono decidiu
manter no escopo.

### Levantamento (11/09) — o Nº da nota vive em DOIS lugares, e só ele

Pergunta do dono: quando a conciliação atualizar o card no Pipefy, a planilha
SPsBD precisa ser atualizada junto? *"Pelo menos o número de nota, porque os
outros dados não têm na planilha."*

**Conferido no código.** Ele está certo, e o levantamento é curto:

| O que a conciliação decide | Na SPsBD? | No card? |
|---|---|---|
| **Nº da nota** | **sim — coluna AA** | sim |
| Documentação Fiscal (a categoria) | não existe | sim |
| Chave de acesso | não existe | sim |
| Gerou nota | não existe | sim |

**O Nº da nota é o único campo que pode divergir**, porque é o único que existe
dos dois lados. Os outros três não têm onde divergir — a planilha não os
conhece. Isso simplifica o problema bastante: é uma coluna, não quatro.

**O caminho de volta já existe.** Não é mecanismo novo: toda alteração feita
pela tela já percorre banco → fila → log → planilha, grava no banco na hora,
enfileira a célula e escreve no Sheets pelo processo separado. Se a internet
cair, a célula fica na fila e sobe sozinha. É assim que Status Pgt e Agendado
funcionam desde a estreia.

**A trava que existe hoje, e ela é boa:** a coluna AA está marcada como somente
leitura, e a rota de alteração recusa qualquer coluna fora da lista. Duas
colunas escapam disso por porta própria — Validação (senha própria) e Análise
("Remover risco"). **A conciliação deve seguir esse desenho: porta própria, não
entrada na lista geral.** Pôr `nf` na lista comum daria a qualquer operador o
poder de reescrever o número da nota de qualquer SP pela tela de sempre, e
número de nota é prova fiscal, não campo de trabalho.

**RESPONDIDO pelo dono no mesmo dia:** *"quem alimenta a planilha são
scripts."* Isso resolve e simplifica: **o card é a fonte, a planilha é o
destino**. Escrever o Nº da nota no card BASTA — o script leva o valor para a
coluna AA sozinho. Escrever nos dois lados criaria duas verdades para a mesma
informação, e no dia em que discordassem ninguém saberia qual vale.

**Decidido, então: a conciliação escreve no card e NÃO toca na planilha.** A
coluna AA continua somente leitura. O efeito colateral, dito sem esconder:
entre a gravação no card e a próxima rodada do script, a coluna Nº NF de
Solicitações e do Lote ainda mostra o número velho. A tela de Documentação
Fiscal não sofre disso — ela lê o registro paralelo, que sabe o que foi
decidido e o que já foi escrito.

**Os identificadores dos campos do Pipefy também já estavam dados**, na
estrutura do pipe que o dono colou mais cedo em 11/09 — eu tinha dito que
faltavam, e estava errado. Conferidos um a um e presos no código, com o UUID
de cada um ao lado: "A despesa gerou emissão de Nota Fiscal?", "Nº da Nota
Fiscal", "Documentação Fiscal", "Chave de Acesso", "Análise Dedutibilidade" e
"Etiquetas".

> **Por que isso virou teste.** Errar um identificador do Pipefy **não dá
> erro**: a chamada é aceita e nada é gravado. Não haveria como perceber pela
> tela do Análise de SPs — só abrindo o card e vendo que continua vazio. E as
> 22 opções de Documentação Fiscal batem exatamente com as do pipe, na mesma
> ordem: o Pipefy **recusa o card inteiro** quando o texto não é uma das
> opções, então um acento diferente não erraria uma SP, derrubaria a gravação
> do lote todo.

**O que ainda não se sabe:** o TIPO de dois campos. O JSON traz identificador,
rótulo e UUID, mas não o tipo. Para "Análise Dedutibilidade" — onde o dono quer
o link da nota baixada — isso importa: se for campo de seleção e não de texto,
o link não cabe ali. É uma consulta à API, e fica para quando a gravação for
construída.

### A leitura dos anexos por IA — perguntado em 11/09, e a resposta é NÃO AINDA

Pergunta do dono: *"está entrando aí a análise dos anexos? quando a gente não
conseguir cruzar de forma fácil os dados?"*

**Não, ainda não.** O que está construído cruza só TEXTO — credor, CNPJ, valor,
número da nota, data. Quando isso não fecha, a SP cai em "procurei e não achei"
e para ali. O anexo não é aberto.

E é exatamente aí que a IA entra, porque é aí que o cruzamento textual acabou.
A ordem importa: primeiro o texto, que é de graça e resolve a maioria; só o que
sobrar vai para a leitura do anexo. Mandar todo anexo para a IA seria pagar
caro para responder o que já se sabia. E a IA **propõe, nunca decide** — nota
lida errado de um PDF torto é dedução indevida com cara de decisão tomada.

**A decisão está tomada: a IA não foi adiada, está no escopo.** Falta ser
construída. O custo é dependência nova, cobrada por documento lido, e o volume
da fila só vai ser conhecido quando a tela rodar uma vez contra a base inteira
— a conta muda muito se forem 50 anexos por mês ou 5.000.

> **Um atalho que o dono levantou e que pode dispensar boa parte disso:** se os
> certificados digitais da empresa entrarem no Análise de SPs, as notas poderiam
> ser baixadas direto da Receita pela chave, sem IA e sem FSist — e a IA
> sobraria só para o que não é nota eletrônica. **Não foi verificado se é
> viável.**

### Vigésima sexta leva (11/09) — a SP repetida no lote

Três coisas, todas pedidas pelo dono no mesmo minuto.

**1. A marcação voltava na linha ERRADA quando a SP estava repetida.** *"Quando
eu marco alguma coisa no lote, e esse registro está repetido, ele marca também
o outro. Está bagunçando."*

A memória da marcação (19ª leva) guardava o **número da SP**. No Lote a mesma SP
pode estar em dois grupos — e aí repor pelo número marcava **as duas cópias**: a
que a pessoa marcou e a que ela não marcou. Não era a marcação retroagindo (isso
foi a 24ª leva); era ela pegando a linha errada.

Agora guarda a **chave da linha** — grupo mais posição mais número —, então volta
marcada só a linha que a pessoa marcou.

> **Nas Solicitações continua valendo o número, de propósito.** Lá cada SP
> aparece uma vez só, então o número já identifica a linha; usar a posição faria
> a marcação se perder toda vez que a base sincronizasse e empurrasse as linhas
> — que é justamente a memória que a 19ª leva criou. Há teste travando isso.

> **Trade-off escrito:** no Lote, mexer no conteúdo (remover pagos, por exemplo)
> muda as posições e a marcação guardada não volta. É o lado certo de errar —
> deixar de repor não faz nada; repor na linha errada faz agir sobre o pagamento
> errado.

**2. "Tirar" virou "Remover".** *"Esse termo tirar não é legal, é melhor remover
pagos e remover cancelados."* Numa tela de pagamentos "tirar as pagas" chega a
soar como desfazer o pagamento — e o botão só mexe na lista do lote.

**3. Botão "Remover duplicados", novo.** *"Mantém o registro mais superior, e os
que estão mais para baixo no lote remove."*

Fica a **primeira** aparição, e não a última, porque o lote é lido de cima para
baixo e o grupo mais recente entra no topo — guardar a de baixo mudaria a SP de
grupo sem ninguém ter pedido.

> **Por que a repetição atrapalha, e não é só feiúra:** o mesmo número em dois
> grupos aparece duas vezes na tela, **é somado duas vezes no total do lote**, e
> convida a agir duas vezes sobre o mesmo pagamento. Era também o que fazia a
> marcação pegar a linha errada, no item 1.

O botão **só aparece quando há o que remover**, e diz quantas são ("Remover
duplicados (3)"). Botão que não faz nada quando apertado é pior do que botão
nenhum: a pessoa aperta, nada muda, e passa a desconfiar dos outros botões.

**O cabeçalho do grupo que esvaziou sai junto**, como ele lembrou no mesmo
pedido — a mesma regra das outras duas limpezas. Para as três não divergirem, a
lógica do cabeçalho órfão virou **um lugar só** (`_limpar`), e cada limpeza só
diz quem sai. Em três cópias, a terceira nasceria sem a regra e ninguém notaria
até o lote encher de título solto.

**Verificação:** 4.304 testes verdes com Postgres de verdade, 129 pulados. 16
testes novos. Os que guardam as decisões do dono foram conferidos **quebrando o
código de propósito** — trocar "fica a primeira" por "fica a última" derruba
três deles; tirar a guarda que prende a chave de linha ao Lote derruba outro. A
aplicação sobe com os 18 blueprints.

**O que NÃO foi verificado:** nada disto foi aberto num navegador. A marcação
reposta em particular é comportamento de tela — os testes conferem o código que
a governa, não o clique.

### INCIDENTE (11/09) — a SP paga que a base insistia em mostrar como "Pagar"

O dono, olhando a **SP 1443253428** no lote: *"na planilha, consulta BD, esse
registro está pago. E ele está aparecendo no lote como PAGAR. A base está
atualizada, eu acabei de atualizar, o relógio está batendo."*

**Não era atraso. Era permanente.** Aquela linha nunca mais seria relida.

**A causa.** A sincronização do dia lê só as colunas A (ID) e V (carimbo) e
traz apenas as linhas cujo carimbo é mais novo que o da última rodada. É isso
que faz a atualização custar segundos em vez de minutos. Só que **o carimbo é
escrito pelo gatilho `onEdit` da própria planilha — e esse gatilho NÃO DISPARA
quando quem escreve é um script.** E quem alimenta a SPsBD são scripts, como o
dono confirmou no mesmo dia.

Ou seja: o script grava "Pago" na coluna O, o carimbo da coluna V fica como
estava, e a sincronização conclui que nada mudou naquela linha. Para sempre —
até alguém editar a célula na mão.

> **Conferido na planilha de verdade**, não deduzido: entre as 63 primeiras
> linhas legíveis da SPsBD, **5 estão com o carimbo VAZIO** e as outras 58 têm
> todas **exatamente o mesmo carimbo** (`2026-09-04 16:05:23`) — a assinatura
> de uma gravação em massa feita por script.

**E o relógio da tela não acusava nada**, o que foi o que despistou: a hora da
última sincronização é gravada no fim de TODA rodada, tenha vindo linha ou não.
"O relógio está batendo" prova que a rotina rodou, **não** que algum dado desceu.

#### As três correções

**1. A sincronização passou a CONFERIR o conteúdo das colunas que decidem
dinheiro** (hoje: Status Pgt), além do carimbo. Se o que está na planilha
difere do que está na base, a linha é trazida — com carimbo ou sem. Custa **uma
leitura de coluna a mais** por rodada; é barato perto do estrago de mostrar como
"a pagar" o que já foi pago. A lista de colunas conferidas é uma linha só de
código, para crescer quando for preciso: cada uma acrescentada é mais uma
leitura, e por isso não entra a planilha inteira.

**2. A marca d'água agora fica UM SEGUNDO ATRÁS do maior carimbo visto.** Um
script que grava 800 linhas carimba todas com o mesmo segundo. Se a varredura
pegasse metade delas, a marca d'água subiria para aquele segundo e a outra
metade — carimbada igual — nunca mais satisfaria "maior que": sumiria para
sempre. Recuando um segundo, a borda é reexaminada na rodada seguinte. Custa
reler um punhado de linhas.

**3. A sincronização passou a registrar QUANTAS linhas desceram** e quantas
foram achadas sem carimbo novo. Vai para o log do Render: se o número de "sem
carimbo novo" for alto todo dia, é sinal de que o gatilho da planilha não está
carimbando o que os scripts escrevem, e a conferência é o que está segurando a
base de pé.

**Verificação:** 4.310 testes verdes com Postgres de verdade. Seis testes novos
reproduzem o caso com banco de verdade, e foram conferidos **desligando a
correção**: com o código que estava no ar, a SP com carimbo vazio continua
"Pagar" depois da sincronização — o defeito exato que o dono viu.

> **Enquanto a correção não estava publicada**, o contorno era editar a célula
> na planilha à mão: edição de gente dispara o gatilho, o carimbo é escrito, e
> a sincronização seguinte traz a linha.

### Vigésima sétima leva (11/09) — os comprovantes arrastados para a tela

*"Eu arrasto esses comprovantes pra dentro e dispara a automação, sem nem
precisar passar pelo Make."* E, logo depois: *"se eu sair da tela e voltar, a
informação vai ser me dada ainda ou eu vou perder se eu mudar de tela?"*

**A descoberta que poupou um módulo inteiro: o robô já existe.** O
`baixabradesco` roda em produção há meses — recebe o PDF, descobre a SP, dá
baixa no Omie, marca paga na SPsBD, move o card no Pipefy e guarda o
comprovante. E já aceita o PDF dentro do próprio pedido. **O que faltava não era
a baixa: era a porta de entrada.** Nada foi mudado naquela área.

**A aba nova é "Comprovantes"**, e ela é própria de propósito: nas Solicitações
a tela já está cheia de linhas, e arrastar arquivo por cima de uma lista de
pagamentos é convite a soltar no lugar errado.

**As levas de dez não são invenção daqui.** O dono descreveu o que o script
dele já faz: *"ele divide o PDF em dez páginas; se eu mandar cinquenta num
único PDF, ele quebra em cinco e manda um por um"*. Manter o mesmo tamanho tem
uma razão a mais do que a simetria — é o tamanho de lote que o robô já recebe
há meses, então não se estreia carga nova nele.

> **E a conta é por PÁGINA, não por arquivo.** O robô trata cada página como um
> comprovante separado. Cortar por quantidade de arquivos deixaria um PDF de
> cinquenta páginas passar inteiro numa chamada só — o caso que as levas
> existem para evitar. A numeração mostrada é a do arquivo original: a página 1
> da terceira leva aparece como "página 21", que é onde ela está no PDF que a
> pessoa soltou.

#### A resposta à pergunta dele: a informação FICA

O resultado mora no **banco**, não na tela. Sair da aba, fechar o navegador,
voltar no dia seguinte — está tudo lá. Se morasse na tela, trocar de aba
perderia tudo, e o serviço ainda reinicia sozinho a cada ~150 requisições.

**E essa informação hoje é jogada fora.** O robô já devolve, a cada leva,
exatamente o que o dono pediu para ver — *"esse deu certo, esse deu errado,
esse tem duplicidade, esse faltou aquilo"*:

| O que a tela mostra | De onde vem |
|---|---|
| **Baixado** | o robô localizou a SP e executou |
| **Já tinha sido baixado** | a trava de duplicidade fez o trabalho dela |
| **Não achei a SP** | com o motivo escrito pelo robô |
| **Falta liberar antes de baixar** | achou a SP, mas ela não está liberada |
| **Pagamento não efetivado** | o comprovante não confirma o pagamento |

Isso tudo voltava para o Make.com e morria lá. **O que pede ação aparece em
cima**, e o que baixou fica por último: quem abre a tela quer saber o que ficou
de fora — o que baixou é o esperado, e esperado não é notícia.

#### Decisões de construção que não são óbvias

- **A baixa roda no processo separado**, como a carga da planilha. Ela fala com
  Omie, Pipefy, Sheets e Dropbox e leva minutos; dentro do worker seria morta
  pelo reinício do gunicorn — foi o que matou a carga três vezes na conversão
  do painel.
- **Cada leva é gravada na hora, não no fim.** Se o serviço reiniciar no meio
  de um PDF de cinquenta páginas, as levas já feitas estão no banco. Há teste
  olhando o banco de dentro do processamento para provar isso.
- **O PDF não entra no banco.** Ele espera em disco e é apagado quando o lote
  termina. O banco tem 1 GB e já usa 430 MB, e o comprovante já é guardado pelo
  robô no destino definitivo.
- **Se o contêiner reiniciar antes de processar**, o arquivo some do disco e o
  lote vira "falhou" com um recado que diz para arrastar de novo — e que fazer
  isso é seguro, porque a trava do robô barra a baixa repetida.
- **Teto de 25 MB por arquivo e 20 arquivos por vez.** A instância tem 2 GB
  divididos com 17 módulos e já morreu de memória em julho de 2026.
- **A chamada ao robô é direta, não por HTTP.** Falar com a própria rota
  ocuparia uma das QUATRO threads do gunicorn por vários minutos. O pedido
  montado é o MESMO que aquela rota passa adiante, então o contrato é o
  documentado.

**Verificação:** 4.421 testes verdes com Postgres de verdade, 129 pulados; 22
testes novos, 10 deles com banco. **E a tela foi exercitada de verdade**, contra
um Postgres descartável: soltar um PDF de 12 páginas cria o lote com 2 levas,
grava o arquivo, dispara o processo e mostra o recado; a tela do histórico
mostra a linha baixada e a que não achou a SP, com o motivo; arquivo que não é
PDF vira recado em português; e o perfil Consulta recebe 403 ao tentar enviar.

> **PRECISA DO BOTÃO.** A migração **006** cria as duas tabelas. Ao publicar,
> apertar **"Aplicar atualizações do banco"** em Configurações no mesmo
> momento. Sem ela a tela abre, mas avisa que falta a atualização em vez de
> estourar.

**O que NÃO foi verificado:** nenhuma baixa de verdade foi feita — o robô foi
dublado em todos os testes. Omie, Pipefy, Dropbox e a planilha não foram
tocados. O primeiro comprovante de verdade é o teste que falta, e o certo é
começar com UM.

### Vigésima oitava leva (12/09) — o nome do credor

*"O Pipefy é frouxo no campo credor. Um lança 'Aço Cearense Limitada', outro
bota só 'Aço Cearense', o outro escreve errado."* O pedido foi comparar credor e
CPF/CNPJ e equalizar para o melhor nome.

**A MEDIÇÃO DERRUBOU A REGRA QUE EU MESMO TINHA PROPOSTO.** Rodando contra a
planilha de verdade (aba Lançamentos, 116 lançamentos, 41 CNPJs): **8 CNPJs —
um em cada cinco — aparecem com mais de um nome**. E "fica o nome mais
completo" não serve, com a prova nos dados dele:

    MAGNA LOCAÇÕES LTDA      3x
    MAGNA LOCAÇÃOES LTDA     1x   <- o MAIS LONGO é o digitado errado

Aquela regra trocaria o certo pelo errado em todas as SPs daquele fornecedor. E
há casos em que **nenhum dos dois é erro**: CELPE virou NEOENERGIA (a empresa
mudou de nome) e MAFEMA aparece como razão social e como nome de fantasia. Isso
não é divergência para corrigir: é decisão de gente.

**A regra que ficou**, e é a mesma da conciliação fiscal — *juntar errado é pior
do que não juntar*:

| Caso | O sistema |
|---|---|
| Só acento, cedilha, pontuação ou espaço a mais ("MBP ISOBLOCK" = "MBP ISO BLOCK") | **resolve sozinho** |
| Um nome é o **começo** do outro ("TRI" → "TRIBUNAL DE JUSTIÇA DO CEARÁ") | **resolve sozinho** |
| Nomes de verdade diferentes | **pergunta — uma vez por CNPJ** |

Entre grafias do mesmo nome fica **a mais usada**, não a mais longa: a equipe
reconhece o nome que ela escreve, e trocá-lo pelo que alguém digitou uma vez
seria piorar.

**A decisão do dono MANDA sobre a proposta.** Uma vez escolhido NEOENERGIA, a
regra não pode voltar a propor CELPE na semana seguinte — senão ele decidiria a
mesma coisa para sempre, que é o contrário do que ele pediu. É para isso que
existe a migração **007**.

> **Nos oito casos reais: 2 o sistema resolve sozinho, 6 esperam ele.** Uma vez
> cada. Com 59 mil SPs a primeira lista vai ser maior; depois disso é só o
> fornecedor novo.

#### Dois defeitos MEUS, achados rodando contra o dado de verdade

1. **Eu contava divergência por grupo de nome.** Com isso "MASSA PRONTA …
   SERVIÇOS LTDA" e "… SERVICOS LTDA" caíam no mesmo grupo, o CNPJ sumia da
   lista e a planilha ficava como estava — justamente o caso mais seguro de
   arrumar, porque é a mesma palavra com e sem cedilha. Passou a contar
   **grafias escritas**.
2. **O "quantas faltam arrumar" também contava por grupo**, e por isso dizia
   "0 a arrumar" no mesmo caso. Passou a contar **grafia por grafia**.

Os dois só apareceram porque a regra foi rodada contra os dados dele, e não
contra exemplo inventado. Cada um virou teste.

#### Onde a tela mora, e por quê

**Fora das abas de cima**, alcançada por Configurações. É arrumação ocasional,
não trabalho do dia — e a barra de abas é para o que se abre todo dia. Encher
a barra com manutenção faria o que importa ficar mais longe.

**A reescrita passa pelo caminho de sempre** — banco, fila, log, planilha —,
que é o que garante que a mudança apareça no Log com o valor anterior e com
quem mexeu, e que chegue à planilha mesmo se a internet cair no meio. E entra
por **porta própria**, como a Validação e o "Remover risco": a coluna do credor
**continua fora de `EDITAVEIS`**, então ninguém reescreve nome de fornecedor
pela tela comum. Há teste travando isso.

**O que já foi decidido não some da lista** — muda de lugar, para "já
decididos". Sumir faria parecer que o problema desapareceu sozinho, e no dia em
que alguém lançasse o nome velho de novo ninguém entenderia por que voltou.

**Verificação:** 4.457 testes verdes com Postgres de verdade, 129 pulados; 35
testes novos, montados com os **oito casos reais**. Conferido pondo a regra do
"nome mais completo" de volta: sete testes caem. **E a tela foi exercitada de
verdade** contra um Postgres descartável, semeado com os oito casos e com o
mesmo CNPJ formatado de dois jeitos: ela mostra 2 automáticos e 6 para decidir;
aplicar os 2 reescreveu 2 SPs, pôs 2 células na fila da planilha e 2 linhas no
log; "TRI" e "SERVICOS" sumiram da base; e escolher NEOENERGIA ficou gravado com
o nome de quem decidiu.

> **PRECISA DO BOTÃO.** A migração **007** cria a tabela da memória das
> escolhas. Ao publicar, apertar "Aplicar atualizações do banco" no mesmo
> momento. Sem ela a tela abre e avisa que falta, em vez de estourar.

**O que NÃO foi verificado:** nenhuma escrita chegou à planilha de verdade — a
fila foi conferida, mas quem a esvazia é a sincronização, e ela precisa da
credencial do Google, que não existe fora do Render. E a lista nunca foi vista
contra as 59 mil SPs: o tamanho real dela é desconhecido.

### Vigésima nona leva (12/09) — o relatório do lote com descrição e obra

*"Está bacaninha, só reduz a fonte consideravelmente pra caber mais
informação. Quero que tenha a descrição. Quero que tenha obra. E pode usar a
quebra de linha."*

**Feito:** fonte de 8 para **6,5**, duas colunas novas (**Descrição** e
**Obra** — que é o centro de custo, a palavra que ele usa) e o texto agora
**quebra em até três linhas** dentro da célula em vez de ser cortado.

A descrição ficou com a maior fatia da largura (44 mm de 190) porque é o único
texto realmente livre; as outras colunas cabem numa linha quase sempre. O "R$"
saiu das células e foi para o título da coluna: repetido em trinta linhas, ele
só gastava a largura que a descrição queria.

**Detalhes que a tabela precisou ganhar:**

- **Quebra por palavra**, e só parte a palavra quando ela sozinha não cabe —
  um código emendado sem espaço não pode empurrar o valor para fora da página.
- **Passando de três linhas, a última termina em "…"**. Sem isso a pessoa lê
  meia frase achando que é a frase inteira.
- **A régua vai embaixo da linha inteira**, desenhada depois de escrever todas
  as células. Com alturas diferentes por célula, a borda de cada uma sairia
  numa altura diferente e a tabela ficaria serrilhada.

#### INCIDENTE achado conferindo o papel: o vencimento saía um dia antes

Gerado o PDF e **aberto como imagem para conferir de olho**, um vencimento de
**20/09** apareceu impresso como **19/09**.

**A causa, e ela é antiga:** uma data escrita sem hora (`"2026-09-20"`) virava
meia-noite sem fuso, e a conversão para Brasília levava esse instante para as
21h do **dia anterior**. Meia-noite de uma data sem hora não é um instante no
mundo: é o dia. Converter fuso ali inventa uma hora que ninguém informou.

> **Por que ninguém tinha visto:** nas telas as datas chegam das colunas DATE
> do banco, que vêm como data de verdade e não passam por esse caminho. O
> defeito só aparece onde a data viaja como TEXTO — que é o caso do PDF e do
> JSON. Um relatório de pagamento com o vencimento um dia antes é o tipo de
> erro que faz alguém pagar na data errada.

A conversão de fuso continua valendo onde ela é certa: uma sincronização de
00h30 em UTC continua aparecendo como 21h30 do dia anterior, em Brasília. Há
teste para as duas coisas.

**Verificação:** 4.466 testes verdes com Postgres de verdade, 129 pulados; 12
testes novos. **O PDF foi gerado e olhado como imagem**, não só lido por
extração de texto — é assim que o defeito da data apareceu. Conferido
desligando cada correção: sem o conserto da data, dois testes caem; sem a
quebra de linha, outros dois.

### Trigésima leva (12/09) — a tela de Documentação Fiscal (primeira parte)

*"Eu quero minimizar a interação do humano (…) é muito falho o olho humano, e
nós não temos esse tempo."* A regra de negócio já estava escrita e travada em
teste (25ª leva); esta leva é a **tela** que a mostra.

**O que ela faz hoje:** varre as SPs do filtro, procura a nota de cada uma no
relatório do FSist, e entrega a análise pronta, separada por urgência.

**Os números do alto são atalho:** "Proposta de correção: 12", "Em dúvida: 3",
"Precisa de decisão: 1". Clicar filtra por aquele grupo. Sem isso, achar as três
que precisam de gente no meio de duzentas linhas seria rolagem.

**As duas pilhas estão na tela, e a diferença está num atributo — não no olho
de quem lê.** O que o sistema propõe com confiança **já vem marcado**, para
aprovar em lote; o que tem dúvida vem **desmarcado**, e é decidido um a um. A
tela não decide isso: quem marca é o servidor, onde a regra mora.

#### O que faz a tela ABRIR, e não é detalhe

São 59 mil SPs e milhares de notas, e o banco tem **um décimo de um núcleo**.
Pontuar toda SP contra toda nota seriam centenas de milhões de comparações — a
tela nunca carregaria.

O que evita isso: **a nota de um lançamento quase sempre foi emitida pelo credor
dele**. Então busca-se, para a página que está na tela, só as notas daqueles
CNPJs. De centenas de milhões, cai para algumas dezenas por SP.

Duas portas de busca, e a segunda tem motivo próprio:

- **pelo CNPJ de quem emitiu** — pela RAIZ de oito dígitos, porque a nota sai
  da filial que entregou e o cadastro do credor quase sempre tem a matriz;
- **pelo número da nota** — para quando quem lançou digitou o número e o
  CPF/CNPJ do credor está errado no cadastro. Sem essa porta, a nota certa
  nunca seria nem considerada.

#### Decidir e gravar são DUAS coisas

`decidida_em` é quando alguém escolheu; `escrita_em` é quando o card aceitou.
Enquanto a segunda estiver vazia, a decisão está pendente e volta na próxima
leva. **É isso que permite tentar de novo quando o Pipefy recusa** — se as duas
fossem uma coisa só, uma falha de rede apagaria a decisão de trinta cards. E
mudar de ideia depois de o card já ter sido escrito **devolve a SP para a fila
de escrita**, senão a correção ficaria só aqui dentro.

**A categoria é conferida contra as 22 opções do Pipefy antes de gravar.** Ele
**recusa o card inteiro** quando o texto não é uma das opções — então um valor
inventado não erraria uma SP: derrubaria a gravação do lote todo.

**Verificação:** 4.478 testes verdes com Postgres de verdade, 129 pulados; 12
testes novos com banco. **A tela foi aberta de verdade** contra um Postgres
descartável, com duas SPs e duas notas do FSist semeadas: mostrou a proposta
marcada e a dúvida desmarcada, confirmar gravou no diário com o nome de quem
decidiu, a SP entrou na fila de escrita do card, a categoria inventada foi
recusada, e a nota órfã apareceu na segunda visão.

**O QUE AINDA NÃO EXISTE, e é o resto desta frente:**

1. **A gravação em lote nos cards do Pipefy.** A fila está pronta e os
   identificadores dos campos estão travados em teste; falta o passo que fala
   com a API.
2. **A IA lendo os anexos**, com a opção por SP que o dono desenhou: os
   pendentes ganham *"analisar com IA"* e ele escolhe quais.
3. **O download autônomo das notas** pela chave.
4. **A segunda visão na tela.** A função que acha as notas sem lançamento
   existe e está testada, mas ainda não tem tela — hoje só a primeira visão
   (lançamento → nota) aparece.

> **PRECISA DO BOTÃO.** A tela usa a migração **005**, que ainda não foi
> aplicada em produção. Sem ela a tela abre e avisa que falta, em vez de
> estourar.

### Trigésima primeira leva (12/09) — a análise fiscal chegando ao card

Fecha o ciclo: a análise sai da tela e vira campo preenchido no Pipefy. Sem
isto ela ficava bonita e não saía do lugar.

**Quatro campos são escritos**, e os identificadores estão travados em teste
desde 11/09: Documentação Fiscal, Chave de Acesso, "A despesa gerou emissão de
Nota Fiscal?" e o Nº da Nota Fiscal.

#### Três regras que protegem o card, e cada uma tem um porquê caro

1. **Campo vazio NÃO é mandado.** Mandar chave vazia para um card que já tem a
   chave preenchida **apagaria** a chave — e apagar o que outra pessoa
   preencheu à mão seria o pior efeito possível desta tela.
2. **"Gerou nota" só vira "Sim", nunca "Não".** Com a chave na mão, a resposta
   é sim. **Não ter encontrado não prova que não existe** — pode ser nota fora
   do relatório do FSist. Escrever "Não" ali seria afirmar o que este módulo
   não sabe, num campo que outras pessoas usam.
3. **Chave pela metade não é mandada.** 44 dígitos ou não é chave. Meia chave
   num card é pior que nenhuma: parece decidida.

#### O que acontece quando o Pipefy recusa

A função devolve **quais** cards passaram, não quantos — só esses podem ser
marcados como escritos. Quem recusou **continua na fila**, com o motivo
gravado, e volta na próxima leva. Uma ida à API que cai leva só os vinte cards
daquele bloco; os seguintes continuam.

> **Roda no processo separado**, como a baixa dos comprovantes e pelo mesmo
> motivo: são até duzentos cards falando com a API, e dentro do worker isso
> seguraria uma das quatro threads do gunicorn por minutos.

Confirmar na tela **dispara a gravação na hora**. Se já houver uma rodada em
andamento, o disparo é recusado e a tela diz isso — a decisão fica na fila e
entra na próxima. Nada se perde por causa disso.

**Verificação:** 4.489 testes verdes com Postgres de verdade, 129 pulados; 11
testes novos. Os testes olham **o que é mandado para a API**, e não só se a
função roda: o estrago de mandar errado não aparece na tela, aparece no card. O
Pipefy foi dublado em todos — **nenhum card de verdade foi tocado**.

**O que falta nesta frente:** a IA lendo os anexos, o download autônomo das
notas, e a segunda visão (notas sem lançamento) na tela.

### Trigésima segunda leva (12/09) — a segunda visão, e um defeito que ela achou

**A visão nota → lançamento entrou na tela.** É ela que fecha com a
contabilidade: *"se tem uma nota emitida, tem uma despesa para estar
associada"*. Nota órfã é problema fiscal, e até agora ninguém a enxergava — a
conciliação só olhava do lado do lançamento, e o que nunca virou lançamento
nenhum não aparecia em lugar nenhum.

**Ela não para em "esta nota está órfã".** Cada linha já traz as **SPs
candidatas** — as do mesmo CNPJ, com as de mesmo valor na frente — e a
**categoria lida de dentro da chave**. Saber que a órfã é um CT-e já diz onde
procurar a despesa. Dizer só "está órfã" seria meio caminho: quem vai resolver
precisa de por onde começar.

As canceladas ficam de fora de propósito, e a comparação da chave ignora
pontuação — uma chave gravada com espaço no meio faria a mesma nota voltar a
aparecer como órfã depois de conciliada, e ninguém entenderia por quê.

#### INCIDENTE: o valor da nota NUNCA batia

Escrevendo o teste que esperava a SP de mesmo valor em primeiro lugar, ela veio
em segundo. A causa:

> A coluna do valor da nota é **NUMERIC**, e o banco devolve NUMERIC como
> **`Decimal`** — que não é `int` nem `float`. Sem tratar esse tipo,
> `Decimal("269.00")` caía no caminho do texto brasileiro, onde o ponto é
> separador de milhar: virava **26.900**.

**O estrago não aparecia na tela. Aparecia como ponto que faltava:** o valor
nunca batia, e **toda** conciliação perdia os 25 pontos do valor exato. A tela
continuava propondo — pelo CNPJ do emitente e pelo número da nota —, só que com
menos confiança do que devia, e os casos que dependiam do valor para chegar aos
60 pontos ficavam em dúvida sem motivo.

É o tipo de defeito que faz a tela "quase funcionar" para sempre: nada quebra,
nada acusa, e o resultado é pior sem ninguém saber. Só apareceu porque o teste
foi escrito contra o **banco de verdade**, e não contra um valor digitado à mão
no teste.

**Verificação:** 4.497 testes verdes com Postgres de verdade, 129 pulados; 8
testes novos. A segunda visão foi **aberta de verdade**: a nota órfã apareceu
com a categoria certa lida da chave, e a que já tinha sido conciliada não.

### Trigésima terceira leva (12/09) — a IA lendo o anexo

**A descoberta que poupou o maior pedaço desta frente: o leitor já existe.** O
ERP tem um leitor de documentos rodando em produção
(`erp/core/documentos/leitor.py`) que já faz exatamente o que faltava — XML de
NFe por **parser exato, sem IA nenhuma**; PDF com camada de texto; e foto ou
PDF escaneado por **leitura visual** —, devolvendo a chave de acesso, o tipo do
documento, o emitente, o número, o valor e um nível de confiança.

**Escrever um segundo leitor seria ter duas verdades sobre o mesmo PDF.** Aqui
só se chama a função pública dele. Nada foi alterado naquela área.

**A ordem é a que o dono definiu:** primeiro o cruzamento de texto, que é de
graça e resolve a maioria; só o que sobrar vai para a IA. Mandar todo anexo
para a IA seria pagar caro para responder o que já se sabia.

**E ela NUNCA roda sozinha**, com as palavras dele: *"aí você pode até fazer a
sugestão, analisar com IA, e a gente seleciona ou não seleciona, que me permita
selecionar alguns que eu queira testar."* Botão próprio, ele marca as SPs, e a
confirmação diz que cada leitura é cobrada. **Teto de 50 por vez**, de
propósito: ele manda uma leva, vê o resultado, e decide se continua.

#### O que protege o resultado, e cada um tem um porquê

- **A chave manda sobre o palpite da IA.** Se ela leu 44 dígitos, a categoria
  sai dos dígitos 21-22 da própria chave — definição da Receita. Aí é certeza,
  não interpretação, e é isso que autoriza propor. Se a IA disser "NFe" e a
  chave disser CT-e, **vale a chave**.
- **O que a IA leu é conferido contra a SP.** Se quem emitiu o documento não é
  o credor daquela SP, a proposta é barrada e a linha diz que o anexo pode ser
  de outro lançamento. É o erro mais caro possível, e ele acontece — o dono
  descreveu: *"colocar uma nota de um registro para outro"*.
- **Documento que não é fiscal não ganha categoria chutada.** Um orçamento ou
  um comprovante bancário voltam como "não sei", e não como uma categoria
  inventada.
- **Confiança BAIXA não vira proposta marcada.** O leitor devolve
  ALTA/MEDIA/BAIXA; marcar o que ele mesmo desconfia seria transformar a dúvida
  dele em decisão nossa.
- **A IA grava como PROPOSTA, nunca como confirmada**, e **não atropela o que
  uma pessoa já decidiu**. Quem quiser refazer desfaz primeiro. Uma nota lida
  errado de um PDF torto é dedução indevida com cara de decisão tomada.

**A fila mora no banco**, e não em memória: o processo separado pode ser
reiniciado no meio, e quem escolheu trinta SPs não pode perder a escolha por
isso. O anexo é baixado em streaming com teto de 20 MB — `resposta.content`
traria o arquivo inteiro para a memória antes de qualquer conferência, que é
como esta instância morreu em julho de 2026.

#### Um defeito de tela que a IA expôs

A caixinha de marcar só existia nas linhas que **tinham proposta**. Só que são
justamente as linhas **sem** proposta que precisam da IA — ou seja, era
impossível escolher para a IA exatamente o que a IA existe para resolver. Agora
a caixinha existe sempre; "Confirmar" age só no que tem proposta, "Analisar com
IA" age em qualquer marcada, e o contador diz quantas das marcadas têm
proposta.

**Verificação:** 4.515 testes verdes com Postgres de verdade, 129 pulados; 15
testes novos. **Nenhum chamou a OpenAI** — o leitor do ERP foi dublado em
todos. Aproveitei para trocar um teste frágil: o que guardava os modos que não
contam SPs prendia a linha exata do `if` e quebrava a cada modo novo, dizendo
"defeito" quando o que havia era código novo. Agora é teste de lista.

**O que falta nesta frente:** o download autônomo das notas pela chave — o
único item do desenho do dono que ainda não existe.

### Trigésima quarta leva (12/09) — as notas do FSist, e um defeito grave

#### O DEFEITO: ninguém importava o relatório de notas

A importação do relatório do FSist existia desde 11/09, estava escrita, testada
e funcionando — e **nenhum código a chamava**. A tabela de notas ficaria vazia
para sempre, e a conciliação fiscal não teria contra o que casar: uma tela
inteira funcionando sobre nada.

Achado em 12/09 procurando quem importava o relatório, ao ligar o último pedaço
da frente. **Agora ela roda junto com as outras planilhas de apoio**, e há teste
travando a chamada — porque "existe e está testado" não quer dizer "acontece".

> Nada disso chegou a ir para o ar: a migração 005 ainda não foi aplicada, e a
> tela é desta mesma semana. Mas se tivesse ido, a conciliação abriria vazia e
> ninguém saberia por quê.

#### Os três números que o dono pediu

*"Quando importar vai dizer quantos importou, que conseguiu, que já tinha, que
não tinha."* Agora a importação responde **novas · mudaram · já tinha**, além
das ignoradas (linha sem chave de 44 dígitos: total de rodapé, linha em branco).

**"Mudaram" é o número que interessa, e ele não existia.** Uma nota que volta no
relatório com status **CANCELADA** é notícia — pode ser despesa já paga contra
documento que não existe mais. Antes ela se escondia no meio das "atualizadas",
que na verdade contavam as inalteradas junto.

A contagem usa o **relógio do banco**, e não o de Python: o servidor pode estar
em outro fuso, e comparar carimbo do banco com hora daqui erraria a conta
inteira. E ela é exata porque o carimbo da nota só é tocado quando algo mudou de
verdade — a gravação já ignorava reescrita idêntica desde 10/09.

#### O download autônomo das notas: o que trava, e não é código

Levantado e escrito em `CONCILIACAO_FISCAL.md`. Em resumo: a consulta pública
por chave no portal da Receita **exige captcha**, e automatizar isso seria
construir algo que quebra no primeiro dia. O caminho que funciona é o
**certificado digital A1** — que o próprio dono levantou. Com ele, o XML vem
direto do webservice da SEFAZ pela chave, e o leitor do ERP **já sabe ler esse
XML por parser exato, sem IA e sem custo**.

**Falta do dono, e não é programação:** o arquivo do certificado A1 e a senha;
a decisão de onde ele fica guardado (é credencial sensível — variável de
ambiente no Render, nunca arquivo no repositório); e o aviso de vencimento,
porque A1 vale um ano e para de funcionar em silêncio.

**Verificação:** 4.520 testes verdes com Postgres de verdade, 129 pulados; 5
testes novos. Um deles guarda o defeito de cima: se alguém desligar a chamada,
a suíte acusa.

### Trigésima quinta leva (12/09) — o lote em Excel de verdade

*"Relatório do lote em Excel, por lote e de todos os lotes juntos."* Era o
último pedido da fila.

**Por que agora dá, e até 05/09 não dava:** o `exportar.py` diz que a
exportação é CSV *"porque gerar Excel de verdade exigiria uma biblioteca nova,
e a regra da casa é não acrescentar dependência sem combinar"*. Isso deixou de
valer quando o `openpyxl` entrou por causa do BeeVale. **Nada novo no serviço.**

**O CSV continua, e não é redundância:** ele sai em BLOCOS e é o único que
aguenta exportar a base larga sem estourar a memória. O Excel monta o arquivo
inteiro antes de enviar — por isso é do LOTE, que tem dezenas de linhas, e não
da base, que tem 59 mil. Tem teto de linhas pelo mesmo motivo.

**O que o Excel resolve e o CSV não:**

- **Valor é número.** No CSV ele vai como texto "1.234,56" para o Excel
  brasileiro entender; aqui é número de fato, então dá para somar, ordenar e
  filtrar sem converter nada antes.
- **Código não vira notação científica.** O Excel transforma um código de 47
  dígitos em `1,23457E+46`, e o número volta **arredondado, irrecuperável**. As
  colunas de código vão como texto de propósito.
- **O total é FÓRMULA** (`SUBTOTAL`), então acompanha o filtro. Um número fixo
  mentiria em silêncio — e é justamente para filtrar que se pede Excel.
- **Uma aba por pessoa** na exportação de todos, com um **resumo na primeira**:
  quem abre um arquivo de oito abas quer ver o tamanho do todo antes de escolher
  em qual entrar. Responde a pergunta que hoje não tem resposta em lugar nenhum
  — *"quanto está separado para pagar somando o que cada um montou?"*.

**Um cuidado que só existe por o título ser texto livre:** o Excel **recusa**
`: \ / ? * [ ]` num nome de aba e corta em 31 caracteres. "Pagar 15/09" tem
barra e "Depois: urgente" tem dois pontos — deixar passar faria o arquivo
**inteiro** não abrir por causa de um título. Nomes repetidos também: dois
"Marcelo" acontecem, e o Excel recusa abas de mesmo nome.

**Dois defeitos meus, achados conferindo o arquivo gerado:**

1. **A expressão que limpa o nome da aba estava escapada errada** e não limpava
   nada — o arquivo estourava no primeiro título com barra. Apareceu porque o
   teste usou um nome de lote de verdade, com barra, e não "Lote 1".
2. **A soma do resumo incluía a própria célula** — referência circular, e o
   Excel abre com erro em vez de com o número. A última linha estava sendo lida
   *depois* de a linha do total já existir.

**Verificação:** 4.540 testes verdes com Postgres de verdade, 129 pulados; 17
testes novos. **As duas rotas foram exercitadas de verdade** contra um Postgres
descartável, com dois lotes de pessoas diferentes: os arquivos saem, o valor
chega como número, o ID como texto, as abas saem por pessoa com o resumo na
frente, e lote vazio responde avisando em vez de entregar planilha em branco.

### 12/09 — o plano das notas fechado, e a trava que ele exige

**Nem o FSist nem a Receita guardam o passado**, e o dono confirmou: *"o passado
é o que eu tenho, que eu já baixei de relatório lá. O relatório mais antigo que
eu tenho a gente vai importar pra dentro do Análise de SPs, e deixar lá dentro;
e a partir de então você vai começar a fazer o download."*

**Duas metades:** o passado vem dos relatórios que ele já tem, colados na aba,
uma vez cada; daqui para a frente vem da Receita, pela chave, com o certificado.
**Nota de serviço está fora por decisão dele** — é municipal, não tem serviço
nacional, e continua chegando pelo anexo do card (que a IA já lê).

**O que isso exige da importação, e foi conferido:** a aba do FSist é uma
JANELA que ele troca a cada relatório; a tabela de notas é o ARQUIVO, e ela só
cresce. Se a importação apagasse o que não está no relatório do dia, **o
histórico dele se perderia na primeira colagem** — e é histórico que não se
recupera de lugar nenhum, porque nem o FSist nem a Receita o guardam.

Conferido com banco de verdade: colar o relatório de janeiro e depois o de
fevereiro na mesma aba deixa as duas levas guardadas. E há um teste varrendo o
módulo inteiro atrás de qualquer `DELETE` nessa tabela — a garantia não pode
depender de alguém lembrar dela daqui a seis meses.

**Na prática, para ele:** cola o relatório mais antigo, manda atualizar as
planilhas de apoio, cola o seguinte, manda de novo. Cada leva entra e fica, e a
tela diz quantas entraram, quantas mudaram e quantas já tinha.

### Trigésima sexta leva (12/09) — a busca automática de notas na Receita

*"Um dos corações dessa atualização é essa busca automática por novas notas."*
E ele estava certo em cobrar: eu tinha tratado isso como **bloqueado pelo
certificado**, quando na verdade só a última peça precisa dele. Tudo o mais
dava para construir e provar.

**Como o serviço da Receita funciona, e é isso que explica o desenho:** ele não
responde "me dá tudo de setembro". Responde **"me dá o que veio depois do número
N"** — um contador por CNPJ, o NSU. Cada resposta traz um lote e diz qual foi o
último número entregue; a consulta seguinte começa dali.

**Por isso o ponteiro mora no banco** (migração 008). Se ele se perdesse, a
busca recomeçaria do zero toda rodada — e **a Receita limita consultas**: quem
rebobina toda hora bate no limite e **para de receber**. Guardar onde parou não
é otimização; é o que faz a busca funcionar.

**Um ponteiro por CNPJ e por TIPO.** A BWS tem mais de um CNPJ, e NF-e e CT-e
são serviços separados na Receita, cada um com a sua contagem. Um ponteiro só
faria um sobrescrever o outro e perder notas em silêncio.

#### Biblioteca de terceiro, e o dono autorizou sabendo

`erpbrasil.edoc` + `erpbrasil.assinatura`, no `requirements.txt`. O que ela
resolve é a **assinatura digital do pedido com o certificado A1** — a parte
onde escrever do zero custa caro, porque o erro volta como "recusado" sem dizer
por quê. Ele perguntou se "biblioteca" era código de terceiro, eu confirmei, e
ele mandou fazer.

**A biblioteca NÃO cobre CT-e.** Esse pedido é montado aqui, reusando o
transporte e o certificado dela. E os dois são chamados **separados de
propósito**: o caminho de CT-e nunca foi exercitado contra o serviço de verdade,
e não pode derrubar a busca de NF-e, que é a maior parte do volume. Há teste
para isso.

#### Três formas de saber que o lote acabou — e todas são respeitadas

Insistir depois do "não há nada novo" é o caminho curto para o bloqueio por
consulta demais. O teto de lotes por rodada é **rede de segurança, não
critério**:

1. a Receita responde que não há mais;
2. o ponteiro não andou (protege de laço infinito, quando ela diz "há mais" e
   devolve o mesmo número);
3. chegou no maior número que ela informou.

#### Detalhes que custariam nota perdida

- **O NSU é guardado com os zeros à esquerda.** Sem eles, a comparação de texto
  faria "9" parecer maior que "10", o ponteiro recuaria e a busca releria tudo.
- **O ponteiro só avança, nunca recua.** Uma resposta vazia traz NSU zero.
- **O número da nota sai de dentro da chave** quando o resumo não traz campo
  próprio (posições 26 a 34, definição da Receita). Sem isso, a conciliação
  perderia os 25 pontos do número em TODA nota vinda por aqui.
- **"Cancelada" na Receita vira a mesma palavra do FSist.** Ela responde código
  3; se cada origem gravasse do seu jeito, a mesma nota teria dois status
  conforme a porta de entrada, e a crítica de nota cancelada deixaria de
  disparar para metade delas.
- **A gravação da nota virou UM caminho só** para as duas origens. Duas
  gravações divergiriam no dia em que uma ganhasse um campo.
- **O FSist roda DEPOIS da Receita**, e a ordem importa: quem chega por último
  manda, e assim uma nota cancelada no relatório não é sobrescrita pelo
  "autorizada" que a Receita entregou antes do cancelamento.
- **Falha vira recado gravado**, não queda: "consumo indevido" e "certificado
  vencido" chegam os dois como erro e pedem coisas completamente diferentes.

**Verificação:** 4.659 testes verdes com Postgres de verdade, 129 pulados; 22
testes novos. **Nenhum liga para a Receita** — a conversa está isolada em duas
funções, e tudo o mais é exercitado com a resposta dublada, inclusive o laço
inteiro contra banco de verdade.

> **O QUE NÃO FOI PROVADO, e é o que falta:** nenhuma consulta de verdade foi
> feita. Não há certificado fora do Render. O primeiro teste real é com **um
> CNPJ só**, olhando o recado que fica no ponteiro. E o caminho de **CT-e** é o
> mais provável de precisar de ajuste, porque é o que não veio pronto da
> biblioteca.

**O que o dono precisa pôr no Render:** `ANALISESPS_CERT_A1_BASE64` (o
certificado em base64), `ANALISESPS_CERT_A1_SENHA` e `ANALISESPS_CNPJS` (os
CNPJs vigiados, separados por vírgula). Enquanto faltarem, a busca não roda e
diz isso — as notas continuam entrando pelo relatório do FSist.

### Pedido na fila, ainda NÃO feito

**Nada do dono esperando código.** O que falta não é programação — é o certificado digital A1, para o download autônomo das notas (ver a 34ª leva).

### A janela entre publicar e apertar o botão

Esta entrega foi publicada **com o dono dormindo**, e isso obrigou a resolver
um risco que estava latente: o código sobe para o Render ANTES de alguém
apertar "Aplicar atualizações do banco". Nesse intervalo o programa é novo e o
banco é velho — foi exatamente assim que o módulo travou na estreia, em 03/09.

Agora, onde uma coluna nova é usada, **pergunta-se antes se ela existe**
(`db.tem_coluna`, com a resposta guardada). Sem a migração 003 aplicada:

- o **lote volta a ser um só**, como era na véspera — em vez de a tela
  estourar;
- as **alterações continuam funcionando**, só que o registro fica sem o nome
  de quem mexeu. Recusar a alteração seria pior: o pagamento não espera o
  botão;
- a tela de **Log** abre sem a coluna "Quem".

E **aplicar as migrações zera o que o processo sabia** do formato do banco —
sem isso, o worker continuaria pelo caminho antigo até o próximo reinício, e
o dono apertaria o botão sem ver efeito nenhum.

Verificado montando o estado exato da produção (001 e 002 aplicadas, 003 não):
as nove telas abrem, alterar funciona, enviar ao lote funciona, e depois do
botão tudo passa a usar o formato novo na mesma sessão.

### Discrepâncias procuradas e NÃO encontradas

Varredura pedida pelo dono, comparando com o Streamlit do histórico:

- **Ordenação** — as seis opções batem (vencimento ↑↓, valor ↑↓, credor, ID).
- **Situações do filtro** — as cinco batem (pendências, risco, cadastro
  incompleto, boleto inválido, boleto duplicado).
- **Relatório** — os três recortes, os três períodos e as dimensões de quebra
  batem; o Streamlit tinha cinco dimensões, aqui há sete.
- **`conta_fmt`** — no Streamlit era só `conta` sem espaços. Não havia
  normalização escondida a copiar.

**Uma sobra conhecida:** a coluna **SP Fiscal** do grid do Streamlit não
existe na tabela daqui — ela mora noutra tabela (`sp_fiscal`) e exigiria um
JOIN na consulta da lista. Ficou de fora de propósito: mexer na consulta
principal para uma coluna a mais, na véspera de uma publicação sem ninguém
acordado, não vale o risco.

### O que ainda NÃO voltou
- **Cancelar a SP por dentro do Pipefy** (o botão abre o formulário deles,
  como lá).

  *(O **BeeVale** saiu desta lista em 05/09 — ver a décima terceira leva. O
  código está pronto; falta o dono informar a pasta do Drive.)*
- **A coluna SP Fiscal na lista** (ver acima).
- **Reenviar comprovante por e-mail** (depende de SMTP no serviço).

---

## Regras que não se discutem

### 1. Nada de abrir a base inteira em memória
Foi o número que decidiu a conversão: cada pessoa no Streamlit segurava
162 MB (pico 195 MB). Aqui quem soma é o Postgres; a tela recebe 200 linhas.
As somas do relatório e as sete checagens da auditoria são SQL.

### 2. Trabalho longo roda em processo separado, nunca dentro do gunicorn
`executar_sync.py`. O `--max-requests` do gunicorn matou três cargas do
painel por isso. **Publicar na `main` reinicia o serviço e mata a
sincronização em curso — perguntar ao dono antes de juntar.**

### 3. Nenhum módulo é importado pelo nome curto, e nada de pandas
Há teste que varre o pacote: import só pelo caminho completo; proibidos
`pandas`, `streamlit`, `numpy`, `altair`, `reportlab`, `openpyxl` (o serviço
não os tem); tudo que se importa tem de estar no `requirements.txt`.

### 4. Autorização padrão NEGAR
Rota que não declara o que exige é recusada. A resposta a uma escrita sem
alçada é sempre 403 — antes de qualquer outra checagem (defeito corrigido no
Lote).

### 5. Toda tela abre com recado quando o banco cai
Nunca 500. Há teste para as nove telas. Downloads leem o banco **antes** de
começar a mandar o arquivo — senão sai HTTP 200 com arquivo pela metade.

### 6. PDF é fpdf2 (não `fpdf` 1.7), e o texto passa por conversão para latin-1
O fpdf2 com fonte embutida estoura no meio da geração com travessão ou aspas
curvas. Todo texto é convertido antes, no mesmo caminho do emissaonf.

### 7. A produção não é alcançável a partir dos testes
Mesma regra do ERP e do painel.

---

## Decisões já tomadas — não reabrir sem motivo novo

- **Blueprint no monorepo, não serviço separado.** Um segundo serviço seria
  assinatura à parte, e o Streamlit não cabe em memória com quatro pessoas.
- **O lote é compartilhado** e a tela diz isso: duas pessoas veem o mesmo
  lote; a segunda a salvar sobrescreve, e a tela mostra quem salvou e quando.
- **Quatro módulos do Streamlit reaproveitados quase inteiros** (QR Pix/BR
  Code, código de barras, conferência do Bradesco, matemática do rateio) e a
  matemática de calendário da agenda sem alteração.
- **CSV, não `.xlsx`**; gráficos em CSS, sem biblioteca (o Plotly custava
  3 MB por tela).
- **Datas:** a conversão recupera 857 autorizações que apareciam vazias
  (1.664 SPs com data em duplicidade e quebra de linha) e recusa cinco com ano
  digitado errado (202, 203, 204, 260, 2925).
- **A pasta do módulo só guarda o que roda no Render** (decidido pelo dono em
  03/09/2026). Saíram daqui: o Streamlit original (`app/`), os quatro atalhos
  `.bat` que o abriam, a base local `spsbd_cache.db` de 60 MB, um ambiente
  Python obsoleto de 35 MB e duas pastas vazias. Saiu também o `render.yaml`,
  que já se declarava inerte no próprio cabeçalho.

  **Movido, não apagado.** O Streamlit ainda é o que o dono usa no dia a dia
  até o módulo online estar de pé, e os `.bat` procuram tudo ao lado deles —
  então o conjunto inteiro foi para uma pasta irmã fora do repositório,
  `analise-sps-streamlit-pc/`, e continua funcionando como antes. Nada do
  serviço importava aquilo: nem o código, nem a suíte, nem o Render.

  A base de 60 MB é regenerável — sai da planilha SPsBD. Quando o dono parar
  de usar o Streamlit, a pasta inteira pode ser apagada.

- **Cancelar SP no Pipefy** e **gerar BeeVale**: ações sem volta, e o BeeVale
  depende de um Shared Drive (erro 403 de cota da service account).
- **Enviar comprovante por e-mail**: depende de SMTP no serviço.
- **Excel**: exigiria biblioteca nova.

## Incidentes

- **02/09 — import silencioso quebrado** (`f687e66`): o `bradesco.py`
  importava `pagamentos` pelo nome curto dentro de um try/except; a falha não
  aparecia e o boleto de 47 dígitos nunca casava com a SP.
- **02/09 — `fpdf` antigo no PC** mascarava diferenças com o `fpdf2` do
  serviço. Ambiente local foi alinhado.
- **03/09 — o impasse de estreia: a tela que conserta era a tela quebrada.**
  Na primeira vez que o dono abriu o módulo no ar, Solicitações mostrou "a
  base ainda não foi carregada" e Configurações estourou com "Deu erro".

  Causa: `tarefas.ultima_concluida()` lia `analisesps.execucoes` **sem
  proteção**, e essa tabela só nasce na migração 001. Com o banco de pé e as
  migrações por aplicar — o estado exato de qualquer estreia — a leitura
  estourava e derrubava a tela. E era a **única** tela com o botão que aplica
  as migrações: sem ela, não havia como sair do estado.

  Por que passou por 611 testes: os testes de Configurações ou derrubavam o
  banco **inteiro** (aí `listar_estado()` falha primeiro, e a leitura da
  última execução nem é tentada), ou dublavam `ultima_concluida`. Nenhum
  cobria o meio-termo — e `listar_estado()` sobrevive sem migração nenhuma
  porque ela mesma cria o schema e a tabela de controle.

  A lição, que vale para o módulo todo: **"banco fora do ar" e "banco de pé,
  estrutura ainda não criada" são dois cenários diferentes**, e o segundo é o
  que todo mundo encontra no primeiro dia. Corrigido protegendo
  `ultima_concluida()` (igual à `estado()` ao lado) e a leitura de
  `analisesps.meta` em `base_carregada()`, que tinha o mesmo defeito. Um
  teste novo monta a tela nesse estado.

- **04/09 — a tela afirmava o que não tinha conseguido perguntar.** Com o
  banco fora de alcance, Configurações mostrava o recado de erro certo e, logo
  abaixo, "0 aplicada(s), 0 pendente(s) — **O banco está em dia**" e "A base:
  **vazia**". As duas frases dizem o contrário do que estava acontecendo:
  quem lê conclui que a estrutura está pronta e que não há SPs, e para de
  procurar a causa no lugar certo. Agora as duas dizem **"não deu para
  saber"**, e a estrutura ganha um aviso explicando que a pergunta não chegou
  a ser feita. `base_carregada()` passa a devolver `desconhecida`, que separa
  "consultei e deu zero" de "não consegui consultar". Teste novo trava as
  duas frases; conferido que ele falha sem a correção.

## Coisas pequenas que mordem

- As telas **quase não foram vistas num navegador**. O chat que as fez não
  conseguiu conectar a extensão e as conferiu só estruturalmente. Em 03/09
  duas foram abertas de verdade, num navegador, contra a aplicação rodando no
  PC: a de **entrada** (o logo, o campo, o texto de ajuda e a folha de estilo
  carregam; o login funciona) e a de **Configurações** com o banco fora de
  alcance (o recado aparece no lugar certo, sem estourar). **As outras sete
  continuam sem nenhuma navegação real**, e nenhuma foi vista com dado de
  verdade — não há dado no ar ainda.
- ~~Na tela de Configurações com o banco inalcançável, o resumo da estrutura
  diz "0 aplicada(s), 0 pendente(s) — O banco está em dia".~~ **Corrigido em
  04/09/2026** (ver abaixo).
- Os 57 testes com banco só rodam no GitHub Actions. Sem `ERP_TEST_DATABASE_URL`
  são pulados.
