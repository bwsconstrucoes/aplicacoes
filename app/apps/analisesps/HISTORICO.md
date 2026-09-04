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
