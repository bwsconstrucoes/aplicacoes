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

~~**O que o dono precisa pôr no Render:** `ANALISESPS_CERT_A1_BASE64`,
`ANALISESPS_CERT_A1_SENHA` e `ANALISESPS_CNPJS`.~~ **Substituído no mesmo dia
pela 37ª leva:** o certificado passou a ser subido pela tela de Configurações,
e a lista de CNPJs vigiados saiu de variável — é quem está guardado. A única
variável que continua de pé é `ANALISESPS_CHAVE_COFRE`. Enquanto não houver
certificado guardado, a busca não roda e diz isso — as notas continuam entrando
pelo relatório do FSist.

### Trigésima sétima leva (12/09) — o certificado sobe pela tela

*"Não daria pra adicionar o certificado a partir da tela de configurações,
inserir o arquivo, e adicionar lá, que facilitaria uma troca ou a inclusão de
outros certificados de outras empresas."*

Ele viu o problema antes de ele acontecer. O certificado A1 **vence todo ano**.
Guardado em variável de ambiente, cada renovação é mexer no Render, converter o
arquivo para texto e reiniciar o serviço — coisa que ele não faz sozinho. E
cada empresa nova do grupo seria mais uma variável. Pela tela, é escolher o
arquivo, digitar a senha e pronto.

**O que mudou na prática:** em Configurações há um cartão "Certificados
digitais". Ele mostra de quem é cada certificado, até quando vale, quem subiu e
quando — e avisa em amarelo quando faltam menos de 30 dias para vencer, e em
vermelho quando já venceu. Subir um certificado do mesmo CNPJ **substitui** o
antigo: é assim que a renovação acontece, sem passo extra.

**A lista de CNPJs que a busca vigia deixou de ser configuração.** Antes era uma
variável de ambiente escrita à mão, que podia discordar do certificado que
existe. Agora a busca percorre **os certificados guardados que ainda valem** —
duas listas que podiam divergir viraram uma. Certificado vencido sai da busca
sozinho, em vez de gerar erro de conexão sem explicação.

#### O cuidado, porque esta é a credencial mais perigosa do sistema

Com o arquivo e a senha, alguém **emite nota fiscal em nome da empresa**. Não é
senha de sistema; é a assinatura da empresa. Por isso:

1. **Arquivo e senha ficam cifrados no banco.** Um backup esquecido ou um
   acesso indevido ao banco entrega bytes embaralhados. A chave que decifra
   vive fora do banco, no ambiente do Render.
2. **Não existe caminho de volta.** Nenhuma tela, nenhum endereço devolve o
   arquivo ou a senha. O conteúdo só é decifrado dentro do próprio sistema, na
   hora de falar com a Receita. Há teste que falha se alguém criar essa rota
   sem perceber.
3. **Sem a chave, o sistema RECUSA guardar.** Não guarda em texto puro "por
   enquanto". A conveniência da tela com o arquivo aberto no banco seria pior
   que as duas situações anteriores.
4. **Só quem opera sobe ou remove**, e fica registrado quem foi.

#### Detalhes que evitam erro silencioso

- **O CNPJ e a validade são lidos de DENTRO do arquivo**, não digitados. Data
  digitada à mão erra, e o erro só apareceria no dia em que a busca parasse.
  CNPJ digitado errado faria o sistema consultar em nome de outra empresa.
- **Senha errada é recusada na hora de subir**, com recado claro — em vez de
  virar falha de conexão semanas depois.
- **A tela nunca lê as colunas cifradas.** A listagem seleciona apenas os
  campos que ela mostra; o conteúdo só é buscado pela função que usa.
- **Remover apaga de verdade.** Credencial desativada que continua no banco é
  credencial vazada com passo a mais.

**Verificação:** 4.682 testes verdes com Postgres de verdade, 129 pulados; 20
testes novos, sendo 13 sem banco e 7 com banco. Os testes fabricam um
certificado de mentira na hora, inclusive um já vencido — nenhum certificado de
verdade encostou nesta máquina.

> **O QUE NÃO FOI PROVADO:** a tela não foi aberta num navegador com arquivo de
> verdade, porque não há certificado aqui. O caminho todo foi exercitado contra
> banco de verdade com certificado fabricado, mas o primeiro arquivo real é o
> teste real. E a busca na Receita continua sem nunca ter falado com o serviço
> dela — isso não mudou nesta leva.

**O que o dono precisa fazer, NESTA ORDEM:**

1. Criar no Render a variável `ANALISESPS_CHAVE_COFRE` com uma frase secreta
   qualquer, longa. **Antes de subir qualquer certificado** — sem ela o sistema
   recusa guardar.
2. **Nunca trocar essa frase depois.** Trocar torna ilegível o que já foi
   guardado, e os certificados teriam de ser subidos de novo. Guardar a frase
   em lugar seguro é parte do trabalho.
3. Aplicar as atualizações do banco (migrações 008 e 009).
4. Subir o certificado A1 pela tela de Configurações.

### Trigésima oitava leva (13/09) — a Documentação Fiscal refeita para GERIR

Ele abriu a tela publicada, usou, e voltou com oito coisas de uma vez. A frase
que resume: *"não dá pra fazer a gestão dessa documentação fiscal da forma que
está. O meu objetivo é categorizar e associar nota. Mas do jeito que está aqui
não dá pra fazer isso."*

**O erro de fundo era um só, e vale escrever:** eu tinha reaproveitado a barra
de filtros das Solicitações. Ela responde *"o que tem para pagar"* — pendências,
boleto duplicado, risco de duplicidade. Aqui a pergunta é outra: *"o que falta
documentar, o que já está documentado, o que provavelmente está errado"*.
Reaproveitar economizou trabalho meu e custou a tela inteira: com o filtro
errado, tudo o mais vira uma lista para rolar.

#### O que mudou

**1. O filtro principal virou o do trabalho fiscal.** Doze recortes, abertos no
alto da barra: sem documentação, provavelmente errado, com/sem chave de acesso,
já categorizado, na fila da IA, lido pela IA, decidido por pessoa, confirmado
(falta gravar no card), já gravado no card, com/sem anexo. Marcar dois exige os
dois — *"sem documentação" + "com anexo"* é a fila que dá para resolver lendo o
anexo, e é a mais útil que existe aqui.

**2. O bloco "Situação" saiu**, como ele mandou: *"esse primeiro filtro aqui que
fica aberto, a situação, pendência, risco, isso aqui já tira logo."* Os outros
(obra, tipo de despesa, vencimento, valor) ficaram, também como ele disse — são
recorte de contexto, não a pergunta da tela. E "Ordenar por" ficou: ele olhou e
disse *"não, pode deixar"*.

**3. Totalizadores no alto, e cada um é um atalho.** *"Onde é que eu vejo aqui
como é que está a situação, uma espécie de totalizadores, pra saber o que que
está faltando, onde é que eu tenho que focar?"* Clicar num número aplica o
recorte dele.

> **A parte que mais importa e não se vê:** os números são do **banco**, sobre a
> base inteira dentro do filtro — não sobre as 200 linhas carregadas. Contar a
> página responderia "o que falta NESTA PÁGINA", que é inútil para decidir onde
> focar **e parece certo**. As etiquetas da conciliação (Em dúvida, Sem nota
> encontrada) continuam sendo da página, e agora dizem "nesta página" do lado —
> sem isso, os dois conjuntos de número pareceriam discordar.

**4. As ações vieram para dentro da tela de trabalho.** *"Ao buscar na Receita
as notas emitidas contra a BWS, não tem absolutamente nada a ver eu estar com um
botão desse fora da tela de trabalho. (…) Ler, é pra estar dentro da tela.
Gravar nos cards, é pra estar dentro da tela. Eu estou trabalhando lá, estou
tratando lá, e vou operacionalizar por lá."*

Os cinco botões — buscar na Receita, importar o relatório do FSist, ler com IA,
gravar no Pipefy, devolver à planilha — ficam no alto da tela, com o andamento
embaixo e a tela se relendo sozinha quando a tarefa acaba.

**A causa do engano era boba e por isso ficou registrada no código:** todas as
tarefas longas nascem da mesma lista, e a tela de Configurações desenhava a
lista INTEIRA como botões. Quem criasse um modo novo ganhava um botão lá sem
querer. Agora a divisão é explícita.

**5. Dá para escrever à mão — e este era o buraco maior.** *"Tudo aquilo que
você sugeriu (…) mas o que você NÃO sugeriu, como é que eu adiciono a
informação? Porque a planilha ela me permite adicionar, e a tela não permite."*

Cada linha tem "✎ informar": abre uma janela com a categoria e a chave de
acesso. **E a conferência é a parte útil**, porque chave errada não dá erro —
grava no card uma nota que não é a da despesa, e ninguém descobre, que é
exatamente o defeito que esta tela existe para achar. Então recusa-se o que dá
para recusar sozinho: tamanho diferente de 44, modelo que não é NF-e/NFC-e/CT-e,
e chave emitida por CNPJ que não é o do credor daquela SP. Enquanto se digita, a
tela diz de quem é a nota.

Informando **só a chave**, a categoria sai de dentro dela — o modelo do
documento está na própria chave, então não é palpite. Do lado da nota há
"associar", que fecha o par ali mesmo e tira a nota da lista de órfãs.

**6. As duas visões ficaram lado a lado**, sempre visíveis. *"Eu preciso de duas
visões. Uma é: eu estou olhando para os registros financeiros e buscando bater
as notas. E outra é: eu estou olhando para as notas e buscando o registro
financeiro."* A segunda ganhou painel próprio: quantas notas existem, quantas
estão sem lançamento, quantos CT-e, quantas canceladas.

**7. O filtro parou de sumir.** *"Eu saio e volto e o filtro que eu estou
trabalhando eles somem. Eu vou pra configurações pra fazer alguma coisa, aí
volto pra cá e o filtro some."* Ele agora fica guardado — numa **gaveta própria,
separada da de Solicitações**, porque são perguntas diferentes e quem trabalha
nas duas telas no mesmo dia perderia o recorte toda vez.

**8. A margem branca da esquerda.** *"O filtro ficou deslocado pra direita,
ficou uma margem branca do lado esquerdo."* A tela montava uma **segunda grade
dentro da área de conteúdo**: a coluna de filtros do esqueleto ficava vazia —
288 px de branco — e a barra aparecia depois dela. Corrigido usando a coluna que
já existe. Há teste que falha se alguém refizer isso.

#### Dois defeitos que só apareceram abrindo a tela de verdade

Os dois passaram por toda a suíte e pelos testes com banco. Só apareceram com a
tela montada num navegador, contra um Postgres com dado dentro — e é por isso
que esse passo continua na lista.

- **O painel dizia "3 já categorizados" e a linha mostrava "—".** A documentação
  chega por duas portas: o diário deste módulo e o espelho do card que a planilha
  de apoio traz. Os totalizadores (que são SQL) liam as duas; a LISTA lia só o
  diário. Duas afirmações contrárias na mesma tela, e nenhuma delas com jeito de
  errada. Agora as duas leem a mesma coisa, com a mesma precedência: o diário
  manda onde existe, porque é mais novo.
- **O contador de CT-e dava zero com CT-e na base.** Ele contava pela coluna
  "tipo", que vem preenchida de jeitos diferentes conforme a porta de entrada — a
  Receita grava "CT-e", e o relatório do FSist grava o que estiver escrito na
  planilha, que ninguém controla. Agora conta pelas posições 21 e 22 da chave,
  que são o modelo do documento por definição da Receita e valem para toda nota.

#### Respostas às perguntas dele

- **"Eu não vou poder importar o relatório do FSist mais?"** Vai, e as duas
  fontes são para conviver. A Receita só devolve o que é recente; o histórico
  antigo entra pelo relatório colado na planilha de apoio. As duas escrevem na
  mesma tabela de notas, pelo mesmo caminho de gravação. O botão está na tela.
- **"Se eu gravar um certificado com a senha errada, eu vou saber?"** Sim, na
  hora. A senha é usada para abrir o arquivo no momento de subir; se ela não
  abrir, o certificado **não é guardado** e a tela diz que a senha não confere.
  Não existe caminho em que ele fique guardado com senha errada para falhar
  semanas depois.

**Verificação:** 4.842 testes verdes com Postgres de verdade, 129 pulados; 60
testes novos. A tela foi **aberta num navegador** contra um Postgres com dado
dentro (descartável, nesta máquina): a barra começa no pixel zero, as duas
visões trocam, a janela de escrever à mão abre e grava, o filtro sobrevive a ir
em Configurações e voltar, Configurações não oferece mais os botões fiscais, e a
700 px de largura não há rolagem lateral nem caixa de diálogo fechada aparecendo
solta.

> **O QUE NÃO FOI PROVADO:** nada disso foi visto com a base real de 59 mil SPs —
> os totalizadores são uma varredura a mais por abertura de tela, e num banco
> com um décimo de um núcleo (o incidente de 10/09) isso precisa ser medido com
> dado de verdade. Se a tela abrir devagar, é o primeiro lugar para olhar.

### INCIDENTE (13/09) — "estava baixado, mas na planilha não ficaram pagos"

**O mais grave até aqui, e estava no ar desde a estreia dos comprovantes.**

Ele mandou dois comprovantes pela tela, a tela respondeu **"Baixado"** nos dois,
e nada aconteceu: *"na planilha eles não ficaram como pagos. Tem alguma coisa
errada aí."*

**A causa, em uma linha:** o robô da baixa (`baixabradesco`) assume **modo de
ensaio** quando o pedido não diz o contrário — `payload.get('modo_teste', True)`.
O pedido montado pela tela mandava só o arquivo. Então **toda baixa feita por
esta tela desde a estreia foi simulação**: o robô localizava a SP, montava o
plano, respondia "dá para executar" — e não escrevia em lugar nenhum. Nem Omie,
nem SPsBD, nem Pipefy, nem o comprovante guardado no Dropbox.

**E a tela dizia "Baixado" porque lia "pode executar" como "foi feito".** Essa é
a segunda metade do defeito, e a pior: um padrão errado é um descuido; anunciar
como pronto o que não aconteceu é o sistema mentindo para quem confia nele.

#### O que foi corrigido — duas travas, de propósito independentes

1. **O pedido agora diz `modo_teste: False`**, e diz também cada opção
   (Omie, SPsBD, Pipefy, guardar o comprovante — todas ligadas; **WhatsApp
   desligado**, porque mandar mensagem para fornecedor é efeito para fora da
   empresa e ninguém pediu isso a partir daqui). Um padrão que muda do outro
   lado deixa de mudar o que esta tela faz.
2. **A tela nunca mais chama de "Baixado" o que não foi executado.** São três
   conferências novas, e qualquer uma sozinha teria pego o defeito no primeiro
   dia:
   - se a resposta vier marcada como ensaio, **nenhuma** página é baixa —
     todas viram erro com "reenvie este comprovante";
   - se o Omie recusou, aparece o erro dele, não um "Baixado" por cima;
   - se não há resposta do Omie para ler, também não é baixa — "pode executar"
     não é "executou".

#### E a linha com SP "—" que dizia "Baixado"

Aquela (FERNANDO CARVALHO, R$ 15.000) tem outra explicação, e é legítima: o robô
classificou como **transferência sem SP** e lança direto no Omie, sem card e sem
planilha. A baixa seria real — mas dizer só "Baixado" faz quem lê ir procurar a
SP na planilha e concluir que o sistema mentiu. Agora essas dizem com todas as
letras: *"lançado no Omie como transferência. Não há SP para marcar como paga na
planilha."*

> **O QUE ELE PRECISA FAZER, e não dá para eu fazer por ele:** **reenviar os
> dois comprovantes** depois de publicar. Nada foi gravado, e — importante — o
> registro que impede baixa em duplicidade **também** só é escrito em produção,
> então reenviar não corre risco de baixar duas vezes. Vale para **tudo** que
> passou por esta tela desde a estreia: nenhuma baixa feita por aqui aconteceu
> de verdade. O caminho do Make.com, que é o de sempre, nunca foi afetado — ele
> manda `modo_teste` correto.

### Trigésima nona leva (13/09) — navegar, e saber que a conferência acontece

Três pedidos dele no mesmo dia, e os três são de "não estou enxergando o que o
sistema faz".

#### A ordem do menu, e o menu que cabe

*"Numa tela grande é tranquilo de navegar, porque todos aparecem, mas numa tela
pequena ele fica escondido, as últimas."*

**Medindo, era pior do que parecia:** a faixa de abas ocupa cerca de 1.000
pixels, e com a marca e o canto direito ela já começa a ser cortada perto de
1.400 — ou seja, **num notebook comum a última aba já sumia**, e nada na tela
dizia que tinha sumido. A rolagem lateral existia, mas sem seta e sem sombra:
quem não soubesse arrastar não descobria.

**A solução não tem número mágico.** Em vez de escolher uma largura de corte no
chute, a própria página mede: se a faixa não couber inteira, ela sai e entra um
botão de menu que lista **todas** as telas em coluna, com a atual marcada e o
nome dela escrito no próprio botão. Acerta sozinho em qualquer tamanho de
janela, e continua acertando quando o canto direito cresce (a hora da base e o
botão Atualizar só aparecem em algumas telas). Sem JavaScript nada quebra: a
faixa fica como era.

**A ordem é a dele:** Solicitações, Lote, Comprovantes, Relatório, Doc. Fiscal,
Agenda, e depois os demais. A lista passou a morar no Python e alimenta a faixa
e o menu ao mesmo tempo — duas cópias divergiriam, e a que ficaria de fora seria
a do menu, que é o caminho de quem está no celular.

#### "Como é que eu sei que isso está sendo analisado?"

*"Aquela varredura pra conferir se o que nós já temos está ok, como é que eu sei
se isso está acontecendo? É toda vez que eu abro, é uma vez? E se eu quiser
fazer uma reanálise das informações que a gente já gravou? E se eu quiser
selecionar um determinado registro e reprocessar ele pra ver se está batendo? E
se o que tiver pra trás tiver coisa errada, como é que eu sei?"*

**A resposta honesta tem três partes, e duas delas já eram verdade — a tela é
que nunca disse.**

1. A conferência **roda a cada abertura da tela**, sobre os lançamentos da
   página — **inclusive os que já foram decididos e já foram gravados no card**.
   Nada fica "conferido uma vez e esquecido". Agora está escrito na tela, com o
   número de lançamentos reconferidos.
2. O que está **fora da página** é varrido pelo banco, sobre a base inteira do
   filtro: é de lá que sai o número **"Provavelmente errado"** — nota cancelada,
   categoria que afirma nota sem haver chave, e chave emitida por outro CNPJ.
   Esse é o número que responde "tem coisa errada para trás?".
3. **Faltava mesmo reconferir sob demanda**, e entrou: "↻ reconferir" em cada
   linha e "Reconferir as marcadas" para a seleção. O resultado aparece na
   tela, com a conclusão em cima (quantos mudaram, quantos têm algo a apontar) e
   uma linha por SP dizendo se mudou ou continua igual.

**Uma diferença de propósito:** a reconferência sob demanda procura nota **mesmo
para as categorias que a lista normalmente não concilia** (apólice, contrato,
guia de tributo). Na lista isso seria ruído — procurar nota de aluguel todo dia.
Pedido registro a registro, é exatamente a pergunta que se quer fazer: *"será
que classificaram errado?"* E **não grava nada**: por isso não pede confirmação,
não custa IA, e vale também para quem só consulta.

**Verificação:** 4.861 testes verdes com Postgres de verdade, 129 pulados; 19
testes novos nesta leva. A navegação foi **medida num navegador** em seis
larguras (1600 a 390 px): a faixa fica enquanto cabe, o menu entra quando não
cabe, lista as onze telas na ordem pedida, marca a atual, fecha com Esc e some
quando a janela volta a alargar. A reconferência foi exercitada contra banco de
verdade, uma linha e em lote.

> **O QUE NÃO FOI PROVADO:** a correção da baixa de comprovantes **não pôde ser
> testada de ponta a ponta aqui** — testar de verdade seria dar baixa real no
> Omie da empresa. O que está provado é que o pedido sai com `modo_teste: False`
> e que a tela recusa anunciar baixa sem confirmação. **O teste de verdade é o
> primeiro comprovante reenviado depois de publicar**, conferindo na planilha.

### Quadragésima leva (13/09) — a PROVA por trás da proposta, e o defeito que ela achou

*"Você sugere e eu quero ver de forma completa os dados do que você está
sugerindo. Os dados do relatório FSist. Como faço? Ou quero ver os dados do
registro, não dá pra ver pra validar. Isso pra eu ter que confiar somente no
que você observou."*

**Ele está certo, e o desenho anterior era ruim de um jeito específico:** a tela
mostrava a CONCLUSÃO ("nota 1430 · FORNECEDOR") e escondia o que a sustenta.
Numa tela cujo trabalho é achar erro, pedir confiança cega é o pior arranjo
possível — quem confere sem poder ver vira carimbo, e carimbo não acha nada.

**O que entrou:** um botão "🔍 ver os dados" em cada linha, que abre os dois
lados inteiros —

- **a SP completa**, campo a campo, com os rótulos da planilha e na ordem dela;
- **a nota completa** como está guardada: número, série, emissão, valor,
  situação, emitente e CNPJ, UF, destinatário, as NF-e de dentro de um CT-e, a
  chave, e quando ela entrou aqui;
- **a conta dos pontos, regra a regra** — e aqui está o que mais importa: **as
  regras que NÃO pontuaram aparecem também**, com o que tem de cada lado. "35%"
  não diz nada; *"o nº da nota no card está vazio e o nome do emitente é
  'FORNECEDOR' em vez de 'ACME'"* diz onde olhar;
- **todas as candidatas**, não só a vencedora, cada uma com a conta aberta e um
  botão **"usar esta nota nesta SP"**. Ver a segunda colocada é o que permite
  discordar da escolha — e o botão é o que transforma "discordo" em trabalho
  feito, em vez de reclamação.

Quem só consulta abre igual: olhar o dado não é alterar dado, e é justamente
quem não pode corrigir que mais precisa poder apontar.

#### E a prova achou um defeito grave na hora em que foi ligada

**A mesma SP pontuava 65% na janela e 35% na lista.**

A base guarda cada valor **duas vezes**: o texto que veio da planilha (`valor`,
`vencimento`) e a versão já convertida (`valor_num`, `vencimento_d`). A **lista**
da tela traz só as convertidas; a **ficha completa** traz as duas. E a pontuação
lia só as de texto.

**O efeito, na tela de verdade:** toda SP perdia os **25 pontos do valor e os 5
da data** — 30 de 100. E o corte para o sistema propor é 60. Ou seja: **o
sistema quase nunca propunha**, e as duas pilhas — aprovar em lote o que é
certo, decidir um a um o que tem dúvida — **nunca chegaram a existir**. Tudo
caía na pilha da dúvida, e ninguém tinha como desconfiar: 35% parece um número
perfeitamente legítimo.

É primo do defeito do Decimal (12/09), e a lição é a mesma, agora escrita no
código: **quando o mesmo dado tem duas formas, a leitura tem de aceitar as duas
— e num lugar só**, senão a próxima leitura esquece de novo. As duas formas
passaram a sair de duas funções (`valor_do_lancamento`, `datas_do_lancamento`)
que a pontuação e a conta aberta usam juntas.

> **Por que nenhum teste pegou antes:** todos montavam a SP com os campos de
> texto, que é a forma da ficha. Nenhum montava com a forma que a LISTA entrega
> — e é a lista que a tela usa. Agora há teste exigindo que as duas formas deem
> o mesmo número, e outro exigindo que a conta aberta feche com a pontuação.

**Conferido na tela, depois da correção:** a lista e a janela dizem 65% as duas,
e a SP passou a vir **marcada** — a pilha do "aprovar em lote" funcionando pela
primeira vez.

### Pergunta (13/09) — "qual o filtro pra aparecer só as que o sistema marcou?"

**A resposta honesta é que não havia um, e agora há — mas só para metade da
pergunta.** As duas metades são diferentes e vale separar:

- **"O que o sistema JÁ MARCOU"** (decisão gravada, sem gente no meio): passou a
  ser filtro na barra. São três origens diferentes, e misturá-las mentiria:
  **Decidido por pessoa**, **Decidido pelo sistema** (proposta aprovada ou
  leitura por IA) e **Já veio preenchido do card** — esta última não é decisão
  de ninguém aqui, e contá-la como trabalho do sistema faria o número parecer
  maior do que é.
- **"O que o sistema ESTÁ PROPONDO agora"**: continua sendo a etiqueta
  *Proposta de correção*, acima da tabela — e ela vale **só para a página
  aberta**. A proposta é calculada quando a tela monta e não fica gravada em
  lugar nenhum.

> **A LIMITAÇÃO, dita para não ser esquecida:** com 59 mil SPs, varrer as
> propostas de página em página não é trabalho de gente. O conserto de verdade
> é rodar a conciliação sobre a base inteira num processo separado e **gravar a
> proposta**, como já é feito com a busca de notas — aí a proposta vira coluna
> no banco e pode ser filtro, totalizador e fila de aprovação em lote. É uma
> decisão do dono, porque custa uma varredura pesada no banco pequeno; foi
> apresentada a ele em 13/09/2026 e ainda não há resposta.

### INCIDENTE (13/09) — "a tela por nota não abre"

**Não era erro: era lentidão, e de um tipo que dá exatamente essa cara.**

A lista de notas montava, para CADA nota da página, uma consulta buscando as
SPs daquele CNPJ. Com 200 notas na tela, **200 consultas** — e cada uma varria
as 59 mil SPs comparando os oito primeiros dígitos do documento, que é uma
EXPRESSÃO, e índice de coluna não serve para expressão.

**Medido aqui, com 59.000 SPs e 4.000 notas: 28 segundos** só para montar as
candidatas — nesta máquina, muito mais rápida que o banco do Render (um décimo
de um núcleo). Lá, o navegador desiste antes.

**Duas correções, e as duas fazem falta:**

- **Uma busca para a página inteira**, em vez de uma por nota. São duas
  consultas: a primeira traz as SPs de valor exatamente igual ao de alguma nota
  (são as que fecham o par) e a segunda as mais recentes de cada CNPJ, para
  haver o que mostrar quando o valor não bate.
- **Migração 010: o índice sobre a expressão** — a mesma que o código usa,
  caractere por caractere. Se ela mudar num lugar e não no outro, o índice
  deixa de ser usado em silêncio e a lentidão volta sem ninguém entender por
  quê. Isso está escrito no `.sql`.

**Depois: 0,19 segundo.** A tela por nota inteira abre em 0,11s.

#### E a medição pegou outro, na tela que ele mais usa

Com o mesmo volume, **"por lançamento" levava 15,6 segundos**. O culpado eram
os totalizadores que entraram ontem: nove contagens, cada uma com uma
subconsulta correlacionada, na mesma varredura das 59 mil linhas.

As duas tabelas da documentação passaram a entrar por **junção**, uma vez, e as
contagens leem colunas já prontas: **1,18s → 0,14s**, e a tela inteira caiu para
**0,26s**.

> **O risco disso é óbvio** — a mesma regra escrita de dois jeitos, uma para o
> filtro (subconsulta, que o índice resolve linha a linha) e outra para o painel
> (junção). É por isso que `test_o_painel_e_o_filtro_CONCORDAM_sempre` existe,
> com banco de verdade, comparando cada contagem com o filtro correspondente.
> Sem esse teste, a otimização não valeria o preço.

### Quadragésima primeira leva (13/09) — a tela das notas, de verdade

*"Eu coloquei pra baixar notas mas não tenho nem ideia de que se baixou, se não
baixou, não consigo visualizar numa tela o que temos de notas e o que não temos.
Ver as notas do dia ou consultar as notas, ver as informações do que foi emitido
contra a BWS, conforme vemos no FSist e no relatório que baixamos e como víamos
na planilha."*

A tela "por nota" mostrava **só as órfãs**. É um recorte útil, e é só um
recorte: quem quer saber "chegou a nota da semana?" não tinha onde olhar.

**Agora a lista é de tudo**, com barra de filtros própria — porque os recortes
do lançamento (obra, tipo de despesa, vencimento) não se aplicam quando a linha
é a nota. Os recortes daqui são: **o lançamento** (sem / já está em um), **a
situação na Receita** (autorizada / cancelada), **o tipo de documento** (NF-e,
CT-e, NFC-e, lido de dentro da chave) e **a data de emissão**. A busca acha por
número, chave, CNPJ ou nome do emitente — os quatro jeitos de procurar uma nota
que se tem na mão.

**"Notas por dia de emissão"**, das últimas duas semanas, responde o *"ver as
notas do dia"* sem obrigar a filtrar: o número de ontem ao lado do de hoje já
diz se a busca está trazendo coisa ou se parou. Cada dia é um atalho.

#### "Baixou ou não baixou?"

O ponteiro da busca na Receita **sempre guardou tudo** — até onde leu por CNPJ e
por tipo, quando consultou, quantos documentos vieram e o recado de erro — e
**nada disso aparecia em tela nenhuma**. Informação guardada e não mostrada é
informação que não existe para quem usa, e a pergunta dele é a prova.

Entrou um quadro "A busca na Receita" com uma linha por CNPJ: última consulta,
documentos trazidos, **quanto falta buscar** (a Receita entrega em lotes, e uma
rodada não traz tudo) e a situação — em dia, ainda há lote, ou o recado de erro
em vermelho. Quando nunca rodou, a tela diz isso e diz o que falta: o
certificado.

### Correção (13/09) — os filtros que ninguém entendia

*"A nomenclatura dos filtros tá estranha, a compreensão tá ruim, muito ruim
mesmo. Eu não consigo filtrar como eu faria numa planilha facilmente. Não dá nem
pra entender o que estamos filtrando, quais dados."*

Três coisas, e as três eram culpa minha:

1. **Um defeito de verdade:** *"se eu clico Sem documentação aparece Proposta de
   correção 28, e aí se eu clico em cima de Proposta de correção 28, ele filtra
   para apenas 2."* Havia **um conjunto de parâmetros só**, e dele o recorte era
   retirado — porque os totalizadores precisam TROCAR o recorte ao serem
   clicados. As etiquetas usavam o mesmo conjunto, e por isso clicar numa delas
   **apagava o recorte**. O número mudava debaixo do dedo dele. Agora são dois
   conjuntos, com nomes que dizem para que servem, e há teste para as etiquetas,
   para a paginação e para os totalizadores.
2. **Os nomes não diziam de que dado falavam.** Numa planilha ele filtra
   clicando no cabeçalho da coluna e sabe exatamente o que está recortando. Os
   recortes passaram a vir **em grupos, e o título do grupo é o nome do dado**:
   *A categoria — é a coluna "Está como"*, *A nota fiscal*, *Quem preencheu*,
   *Em que pé está o trabalho*, *O anexo da SP*. Cada opção traz uma linha
   explicando embaixo.
3. **A tela não dizia o que estava filtrando.** As caixas marcadas ficam na
   barra lateral, fora do campo de visão de quem olha a tabela. Agora há uma
   linha acima dela: *"Mostrando as SPs em que a categoria está vazia e tem
   anexo"*, com um ✕ em cada recorte para tirá-lo dali mesmo.

E os **totalizadores passaram a usar as mesmas palavras** dos filtros
("Categoria vazia", e não "Sem documentação"): eram diferentes, e isso era
metade da confusão — o número dizia uma coisa, o filtro dizia outra, e nada
indicava que eram a mesma pergunta.

**Verificação:** 4.900 testes verdes com Postgres de verdade, 129 pulados. As
medições de tempo foram feitas com **59.000 SPs e 4.000 notas** num Postgres de
verdade, descartável, nesta máquina.

> **O QUE NÃO FOI MEDIDO:** o banco do Render é muito mais lento que esta
> máquina. Os números aqui são a ORDEM DE GRANDEZA, não a previsão. O recorte
> ligado na tela de lançamentos ("categoria vazia") ainda leva **1,3s aqui** —
> é o mais caro que sobrou, porque o filtro usa subconsulta. Se lá ficar
> pesado, é o próximo lugar para mexer.

### Correção (13/09) — o botão que pedia um arquivo e não tinha onde pôr

*"Importar relatório FSist — e ele diz que vai rodar no sistema? E cadê a opção
de incluir o arquivo? Como é que ele vai rodar? De onde vai tirar essa
informação, se eu não estou nem colocando?"*

Ele está certo, e o defeito é de **nome**, não de função. O botão lia a aba
"Relatório FSIST" da planilha de apoio — o fluxo antigo, de colar o relatório
lá. Funciona. Só que *"importar relatório"* pede um arquivo, não havia onde pôr,
e **nada na tela dizia de onde ele tirava a informação**.

**Duas coisas mudaram:**

1. **O botão passou a dizer o que faz:** "Ler a aba do FSist na planilha". Botão
   que pede um arquivo e não tem onde pôr é botão que mente.
2. **Agora dá para subir o arquivo**, na tela das notas. Aceita **.xlsx, .csv e
   .txt**, descobre sozinho o separador (o Excel brasileiro salva com ponto e
   vírgula, o de fora com vírgula) e lê acento em qualquer das codificações que
   aparecem na prática — o arquivo salvo pelo Excel brasileiro **não** é UTF-8,
   e recusar por isso obrigaria a converter antes, que é o trabalho manual que
   esta tela existe para tirar.

**AS DUAS PORTAS FICAM.** Colar na aba é o hábito da equipe; subir o arquivo é o
caminho curto — e é como está o relatório antigo que ele quer trazer para
dentro. E as duas passam pelo **mesmo mapeamento de colunas, a mesma procura de
cabeçalho e a mesma gravação**: um segundo caminho de leitura divergiria no dia
em que o FSist mudasse uma coluna de nome, e só um dos dois seria corrigido. Há
teste estrutural cobrando isso.

**Detalhes que evitam chamado:**

- **O cabeçalho não precisa estar na primeira linha** — no relatório do FSist a
  primeira é o título. Procura-se a linha que TEM a coluna "Chave".
- **Arquivo errado diz o que encontrou no lugar.** Subir o extrato do banco por
  engano responde *"não achei a coluna Chave; o que encontrei foi: Data,
  Histórico, Valor"* — é o que permite descobrir o próprio engano sem
  perguntar a ninguém.
- **Rodapé e totalizador são ignorados em silêncio**: são o formato do
  relatório, não erro, e não podem virar recado de falha.
- **Subir o mesmo relatório duas vezes não duplica nada** — a chave é a
  identidade, e só se regrava o que mudou. Reimportar é o que ele vai fazer sem
  pensar, e tem de ser inofensivo.
- **A nota que voltou CANCELADA é atualizada**, e é o achado que mais importa
  num reenvio: pagar contra nota cancelada é problema fiscal.
- **`.xls` (Excel antigo) é recusado dizendo o que fazer** — salvar como .xlsx
  ou .csv. Recusa que não diz o que fazer é recusa que vira chamado.

**Verificação:** 11 testes novos sem banco (os três formatos, a codificação, o
separador, o arquivo errado) e 5 com banco de verdade (o caminho inteiro, a
reimportação, a nota cancelada, o rodapé, e a nota aparecendo na tela). E o
arquivo foi subido **pela tela, num navegador**, com relatório de mentira salvo
na codificação do Excel brasileiro: importou 1 nota, ignorou as 2 linhas de
rodapé e listou a nota; e o extrato do banco subido por engano foi recusado com
o recado certo.

### Quadragésima segunda leva (13/09) — o clique padronizado, e o que os botões fazem

Cinco coisas que ele achou navegando, e quatro eram incoerência minha.

#### O clique fazia coisas diferentes em telas do mesmo assunto

*"Na tela por lançamento eu clico no registro, aí ele abre o card. Aí na tela
por nota ele abre o registro do sistema. Está meio perdido assim. (…) Eu acho
que o certo é dois clique na linha, abre o registro. E o linkzinho do card, aí
abre o card do Pipefy. E não abrir direto, e sempre abrir modal, porque aí você
permanece na tela."*

Ele está certo, e a proposta dele é exatamente o que as **Solicitações** já
faziam desde a conversão — o modal de duplo clique existia e as duas telas
fiscais não usavam. Agora usam as três:

- **dois cliques na linha** abrem a ficha por cima da lista, sem perder a
  rolagem, o filtro nem a marcação;
- **o número da SP deixou de ser o link do card**, e o card virou um link
  próprio ("card ↗"), que abre em outra aba.

E o "Voltar" parou de desligar as telas uma da outra: *"quando você bota
voltar, ele volta pra solicitações, fica totalmente desvinculado da
documentação fiscal"*. Como a ficha agora abre por cima, não há de onde voltar
— e quando ela é aberta em página inteira, o endereço leva de volta à
Documentação Fiscal.

#### A visão se perdia ao sair da tela

*"Quando eu saio de documentação fiscal pra um outro menu e volto, ele volta
sempre pra por lançamento. Só que eu estava em por nota."* A visão passou a ser
guardada junto com o filtro, na mesma gaveta — assim como os recortes da tela de
notas, a busca e o período de emissão.

#### "A busca nunca rodou" com três certificados cadastrados

*"Eu estou vendo aqui a busca nunca rodou. (…) Em configurações eu cadastrei
três certificados já."* O recado era um só, e mandava procurar no lugar errado:
**cadastrar o certificado não dispara busca nenhuma**. Agora são três estados
diferentes, porque pedem coisas diferentes:

- **sem certificado e sem busca** → falta o certificado, e diz onde subir;
- **com certificado e sem busca** → *"há 3 certificado(s) guardado(s), e a busca
  ainda não foi disparada nenhuma vez"*, com o botão ao lado;
- **com busca** → a tabela por CNPJ, e **os CNPJs que têm certificado e nunca
  foram consultados aparecem pelo nome** — com três certificados e um só
  consultado, saber QUAL falta é a diferença entre resolver e adivinhar.

#### As duas perguntas dele, respondidas na tela

*"O que é que acontece quando eu clico em associar? Ele vai pro gravar no que
foi confirmado, é isso?"* — **é isso, com um passo no meio**, e agora está
escrito acima da lista: associar grava a chave naquela SP, tira a categoria de
dentro da própria chave e a nota sai da lista; **não mexe no card ainda** — ela
passa a contar em "Falta gravar no card", e quem leva para lá é o outro botão. A
separação existe para que uma falha do Pipefy não apague a decisão de trinta
notas.

*"O que é que significa devolver à planilha as alterações?"* — a ajuda antiga
dizia *"as alterações feitas na tela"*, que não explica nada para quem não sabe
que existe uma fila. Agora diz: **nada do que se altera nas telas vai direto
para a SPsBD** — fica numa fila e sobe de uma vez, para não escrever na planilha
a cada clique (foi o que a deixou lenta). O botão esvazia a fila agora, em vez
de esperar a atualização do dia. **E não tem nada de fiscal**: vale para
alteração feita em qualquer tela.

**Verificação:** 11 testes novos. E as quatro coisas foram **clicadas num
navegador**, contra banco com dado dentro: o duplo clique abre a ficha sem sair
da tela, o link do card aponta para o Pipefy, a tela por nota abre a ficha do
mesmo jeito, e sair para Configurações e voltar traz de volta a visão por nota.

### Quadragésima terceira leva (13/09) — o porquê de cada par, e o alarme da nota cancelada

#### "Mesmo valor" não dava segurança nenhuma

*"Você bota aqui a nota e bota 'mesmo valor'. Mas gera dúvida: você está
comparando o mesmo valor de quê? Do mesmo fornecedor, do mesmo número de nota
fiscal? Como é que você chegou a essa informação? (…) Era interessante ampliar
essa informação, mesmo que esse seja o critério, pelo menos para dar segurança a
quem está fazendo essa associação."*

Ele está certo, e **"mesmo valor" sozinho é pior que nada**: dá ar de
conferência a uma coincidência. Duas notas do mesmo fornecedor no mesmo mês com
o mesmo valor existem — e é exatamente aí que se associa a errada.

Agora cada SP candidata mostra **quantos critérios conferem, e quais**:

- **CNPJ do credor é o de quem emitiu** — é o critério que a põe na lista, e
  agora está dito em vez de subentendido;
- **Valor igual** (ou **Valor DIFERENTE**, dizendo os dois números — ver "a SP é
  R$ 500,00 e a nota R$ 269,00" é o que impede associar por engano);
- **Nº da nota no card** — bate, ou diz o que o card tem;
- **Data compatível** — a emissão contra o vencimento e o pagamento.

#### Os filtros do par, que ele pediu e eu não tinha feito

*"Nessas que estão aqui já verdinha pra associar (…) tem outra sim, o valor é
diferente. Tem que ter um tratamento aí."* Três recortes novos, e eles se
completam (há teste somando):

- **Tem SP do mesmo valor** — o par que fecha sem pensar;
- **Tem SP do credor, mas de outro valor** — precisa de olho;
- **Nenhuma SP daquele CNPJ** — a despesa pode nem ter sido lançada.

#### ⚠️ O alarme: nota cancelada que JÁ está num lançamento

*"Imagina, o fornecedor emitiu e cancelou a nota. E a gente associou, pagou, e a
nota virou cancelada. A gente tem que ter um local de visualização disso,
facilmente poder tratar isso aí, ligar pro fornecedor e pedir uma nova nota (…)
é algo que tem que dar destaque."*

**Este é o caso mais grave da tela, e ficava INVISÍVEL justamente por estar
"resolvido":** a nota cancelada **não é órfã** — ela está associada —, então
nunca apareceu na lista das que precisam de despesa. Só se descobriria abrindo a
nota certa por acaso.

Agora é a **primeira coisa da tela**, em vermelho, antes dos números, e leva
para a lista delas. E a linha da nota diz tudo o que é preciso para agir: a
etiqueta **não sai em verde** (verde diz "resolvido"), vai o **número da SP**, o
**status de pagamento dela**, e quando a SP está paga sai o carimbo **"paga sem
documento"** — sem o número da SP, agir obrigaria a procurar a chave na outra
tela.

**Verificação:** 6 testes novos com banco de verdade — os três recortes do par
se completando, o alarme separando a cancelada associada da cancelada solta (que
é o esperado, não um achado), os porquês de cada candidata e o caso do valor
diferente dizendo os dois números. E a tela foi aberta num navegador com uma
nota cancelada associada a uma SP paga: o alarme aparece, o recorte funciona e a
linha mostra SP, status e o carimbo.

### Quadragésima quarta leva (13/09) — "como é que a gente sabe se rodou?"

*"Eu clico gravar no Pipefy, aí diz que está rodando no servidor, mas como é que
a gente sabe se rodou, se não rodou, se terminou? (…) Depois que eu fiz isso não
aparece nada na tela, a tela continua do mesmo jeito. É isso mesmo? Não deveria
ter alguma coisa dizendo que gravou, uma confirmação?"*

**O dado SEMPRE existiu.** Cada rodada grava quando terminou, se deu certo e um
recado em português — *"12 card(s) gravado(s), 2 recusado(s)"*. Só que isso
aparecia na tela de **Configurações**, que não é onde o trabalho acontece.
Informação guardada e mostrada no lugar errado é informação que não existe para
quem trabalha.

Agora **cada botão carrega embaixo o resultado da última vez que ele rodou**,
com a hora, em verde ou vermelho. E fica lá: quem sai da tela e volta continua
vendo o que a rodada fez, em vez de ter de lembrar. Quem nunca rodou diz "nunca
rodou" — que é diferente de "rodou e não fez nada".

Os dois blocos de botão (por lançamento e por nota) passaram a ser **o mesmo
pedaço de tela**: eram duas cópias, e o dia em que uma ganhasse o resultado a
outra ficaria para trás.

#### E a nota associada agora diz EM QUAL SP

*"Eu filtrei 'já está no lançamento' (…) diz que está associado mas não diz com
a SP que está associada, o registro que está associado. Isso é ruim, que a gente
fica perdido. Se eu quiser confirmar, visualizar de novo, checar, então não está
funcionando assim."*

A linha passou a trazer o **número da SP** e o **status de pagamento dela**, com
dois cliques abrindo a ficha ali mesmo. Sem o número, conferir obrigava a
procurar a chave na outra tela.

#### A resposta sobre o Pipefy, para ficar registrada

Ele perguntou se a gravação é em lote. **É:** as decisões confirmadas vão em
blocos de **20 cards por chamada** — uma única mutação com 20 operações dentro.
Trinta notas são duas chamadas (20 + 10), não trinta. O resultado de **cada
card** é lido individualmente, então um card recusado não derruba os outros 19;
e um bloco que falha inteiro marca só os 20 dele, que voltam na rodada seguinte
porque a decisão continua gravada aqui.

**Verificação:** 7 testes novos (4 de tela, 2 com banco de verdade, 1 da nota
associada). E a tela foi aberta num navegador com rodadas gravadas de verdade:
os cinco botões mostram o que fizeram, o que falhou sai em vermelho com o
motivo, e a nota associada mostra a SP e o status dela.

### Quadragésima quinta leva (13/09) — o botão que não dizia nada, e o CNPJ digitado errado

#### A regra geral: todo botão tem de dizer que está fazendo

*"Eu estou vendo que é muito comum acontecer isso: os botões que deveriam, após
o clique, mostrar a ação que está sendo executada, ele não mostra. Você fica
cego, sem saber se está acontecendo alguma coisa ou não."*

Ele tem razão, e o caso que mais dói é o do formulário que recarrega a página:
entre o clique e a tela voltar passam **vários segundos** — a equalização de
credor reescreve centenas de SPs, uma a uma, pelo caminho que grava banco, fila,
log e planilha. Nesse intervalo a tela fica **exatamente igual**, e quem clicou
conclui que o botão não funcionou. Aí clica de novo.

**A correção é de uma vez para o módulo inteiro**, e não tela por tela: qualquer
formulário enviado agora trava a largura do botão, troca o texto por "Aguarde…"
com um rodinha e o desliga — e se a rede cair, ele volta ao normal sozinho
depois de um minuto, porque deixar "Aguarde" para sempre seria trocar um engano
por outro. Tratar isso tela por tela é justamente por que o defeito aparecia em
tantos lugares.

**E o recado de volta também mudou:** na tela de credores, *"eu clico em
aplicar, e na tela nada acontece, eu não sei se foi aplicado ou não."* O recado
existia — voltava numa tarja discreta no alto, e a tela pode voltar com a
rolagem no meio da lista, deixando-o acima da dobra. Agora ele vem em destaque e
a tela leva o olho até ele.

#### A nota cancelada, em vermelho — e fora da pilha do "aprovar em lote"

*"Quando tiver a nota cancelada na tela de associação, tem que deixar em
vermelhinho o cancelado, pra a gente não associar a uma nota cancelada sem
perceber."*

A informação estava lá, no meio de uma linha cinza de oito palavras — o que é o
mesmo que não estar. Agora sai em vermelho.

**E foi mais fundo do que a cor:** a marcação em lote existe justamente para
quem NÃO olha linha a linha. Uma cancelada pré-marcada entraria no "Confirmar as
marcadas" sem ninguém ver, que é o contrário do que ele pediu. **Nota cancelada
nunca mais vem proposta**, por mais que combine — ela continua aparecendo, e tem
de aparecer (se aquela é mesmo a nota do lançamento, quem analisa precisa saber
que ela foi cancelada), só não vem decidida.

#### A terceira opinião: quem a Receita diz que é o dono do CNPJ

*"Quando você dá sugestão aqui, esse CNPJ é o quê? Eu quero que você faça a
consulta via API do credor desse CNPJ."*

Entrou, com um botão por fornecedor. O serviço é o **brasilapi.com.br** —
público, sem cadastro e sem chave, alimentado pelos dados abertos da Receita.
Escolhido por não exigir credencial nova (credencial nova é decisão dele) e por
não cobrar.

**Três cuidados, e nenhum é opcional:**

1. **Nunca automático e nunca em massa.** A consulta sai por pedido de uma
   pessoa, um CNPJ por vez. Varrer novecentos fornecedores ao abrir a tela é o
   jeito certo de ser bloqueado por uso excessivo — e aí ela para de funcionar
   inclusive no caso em que importa.
2. **A resposta fica guardada** (migração 011). CNPJ não muda de dono.
3. **Nunca decide sozinha.** A Receita informa; quem escolhe continua sendo ele.
   E **se o serviço sair do ar, a tela continua funcionando** — a consulta é um
   extra.

#### O caso difícil: o CNPJ digitado errado

*"Pode ser que a pessoa digitou errado o CNPJ. Digamos que ela foi digitar o
CNPJ de uma empresa e confundiu: olhou na nota e olhou o CNPJ da BWS, da empresa
que ela trabalha, aí digitou o nome da empresa ao invés do CNPJ ao qual a nota
fazia referência. Como é que a gente resolve essa parada aí?"*

**Nenhuma comparação de nomes resolve isso**, e é o que torna o caso diferente
de tudo o que a tela fazia: o erro não está no nome, está no NÚMERO. Os nomes
podem estar todos certos e escritos igual, e mesmo assim apontando para o CNPJ
errado.

Três sinais, e os dois primeiros são **certeza**, não suspeita:

- **É um CNPJ da própria BWS.** A empresa não é fornecedora de si mesma — é
  exatamente o engano que ele descreveu. Os nossos CNPJs saem de onde o sistema
  já os conhece: o destinatário das notas guardadas e os certificados digitais;
  nada de mais uma lista para ele manter.
- **A Receita não conhece o CNPJ.** Número que não existe foi digitado errado.
- **A razão social não se parece com nenhum dos nomes escritos** — esta é
  suspeita de verdade, porque nome de fantasia legítimo também não se parece.
  Serve para olhar, não para concluir. Nome parecido ou contido não acusa nada:
  exigir igualdade acusaria metade da base, e aviso que aparece sempre é aviso
  que ninguém lê.

**O CNPJ comprovadamente errado sai da pilha do "resolve sozinho"** e vai para a
de decisão. Aquela pilha é aplicada em bloco, sem ninguém olhar: equalizar
bonitinho o nome de um fornecedor que não é aquele seria trabalho jogado fora, e
pior, com ar de resolvido. A *suspeita* não tira dali — barrar por suspeita
encheria a fila de decisão de coisa que não precisa de decisão.

**Verificação:** 16 testes novos. Nenhum fala com a internet — a conversa está
isolada numa função. E a tela foi aberta num navegador com dois casos montados:
o CNPJ da própria BWS lançado como fornecedor (sai o alerta vermelho e ele cai
na pilha de decisão) e um fornecedor com a consulta já feita (a razão social
aparece embaixo das opções).

> **O QUE NÃO FOI PROVADO:** nenhuma consulta de verdade foi feita ao
> brasilapi.com.br — esta máquina não tem saída para ele. O primeiro clique real
> é o teste real. Se o serviço não responder do Render, a tela diz "não consegui
> consultar" e segue funcionando; nada depende dele.

### Pedido na fila, ainda NÃO feito

**Nada do dono esperando código.** O que falta não é programação — é o
certificado digital A1 em si, que agora entra pela tela de Configurações (37ª
leva), e a variável `ANALISESPS_CHAVE_COFRE` no Render, que precisa existir
antes dele.

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


### Quadragésima sexta leva (13/09) — trabalho já feito não volta para a fila, e a decisão passa a ter o dado embaixo

**Publicada na `main` antes desta leva:** a quadragésima quinta (botão que
mostra que está trabalhando, nota cancelada nunca proposta, consulta à
Receita, CNPJ digitado errado). Commit de junção `75ab256`.

**⚠️ PENDENTE DO DONO, e repetido aqui porque continua pendente:** apertar
**"Aplicar atualizações do banco"**. As migrações **010** (índice da busca por
CNPJ) e **011** (o que a Receita respondeu) foram para produção com aquela
junção e **ainda não foram aplicadas**. Sem a 010 a busca por CNPJ volta a
varrer a base; sem a 011 a consulta à Receita não tem onde guardar a resposta.

#### 1. A SP que já tem nota deixou de ser sugerida (o achado dele)

*"Tem registro que está aparecendo aqui que ele já tem nota fiscal, já é um
registro que tem uma nota fiscal associada anteriormente, e inclusive já tem o
número da nota, já está associado lá na planilha de documentação fiscal, ou
seja, está tudo identificado — e ele está colocando aqui como sugestão de uma
nota pra associar. Qual é o sentido disso? (…) Qual foi sua lógica nisso?"*

**Não havia lógica.** `sps_possiveis_das_notas` pegava TODAS as SPs daquele
CNPJ e nunca perguntava se a SP já tinha nota. Duas consequências, e a segunda
é a grave:

1. Trabalho já conferido voltava para a fila disputando as cinco vagas com
   quem está de fato sem documento.
2. A SP conferida ficava **a um clique de receber uma SEGUNDA nota**.

**O corte é pela CHAVE gravada no diário (`sp_fiscal_analise.chave`), e só por
ela.** O número da NF escrito no card **NÃO** serve de corte — e essa
distinção é o cuidado que evita o conserto virar um defeito novo: a SP que tem
o número digitado mas nunca foi associada é a **melhor candidata que existe**,
porque o número confere. Cortar por ele esconderia justamente o par mais fácil
da base. Há teste cravando isso.

**Não somem da tela.** Ficam num bloco à parte, contadas e clicáveis
(`.ja-com-nota`), **sem botão de associar**. Sumir em silêncio seria pedir
confiança cega; ficar ali é dar como conferir que o corte não comeu nada.

E a SP que já aponta para **esta mesma** nota some de vez — não é sugestão nem
"já tem nota de outra": é o que já existe.

**Custo medido, com 59.000 SPs e 4.000 notas:** o LEFT JOIN com o diário levou
as candidatas de 0,19 s para **0,27 s** na página de 200 notas. O número que
importa continua sendo o de onde se veio: **28 segundos**, quando a tela não
abria.

#### 2. A decisão dos credores passou a ter o dado embaixo

*"Eu estou diante de um determinado CNPJ, aí aparecem várias opções. Só que
para algumas eu precisaria, por exemplo, ter um determinado CNPJ que eu
entendo que seja da locadora do Vale. Só que ele marca aqui uma, duas, três,
quatro SPs que é de uma outra locadora que não tem nada a ver, ou seja, aqui
foi claramente um erro. Só que a partir daqui eu não consigo ir a essas SPs
que estão erradas. Só pra poder confirmar se eu posso realmente aplicar ou
não, eu precisaria ver essas SPs e entender onde foi o erro."*

A tela pedia uma **decisão** e escondia o dado que fundamenta a decisão. Ver
"LOCADORA A (4 SPs)" contra "LOCADORA B (37 SPs)" não diz nada; ver as quatro
SPs — número, credor escrito, valor, vencimento, situação do pagamento,
descrição e o link do card — diz se foi engano de digitação, se é outro
fornecedor de verdade ou se o CNPJ é que está trocado.

- `credores.sps_do_nome(documento, grafias)` + rota `POST /credores/sps`.
- **Casa pelas GRAFIAS do grupo, não pelo nome escolhido.** A opção da tela é
  um grupo ("SERVIÇOS" e "SERVICOS" são a mesma opção). Buscar só a grafia
  escolhida faria a tela dizer "3 SPs" e a lista trazer 2 — e uma conta que
  não fecha derruba a confiança na tela inteira.
- **Abre por cima**, como todo o resto do módulo, e **não mexe no rádio**: o
  botão vive dentro do `<label>` da opção, e sem `stopPropagation` clicar em
  "ver as SPs" marcaria aquela opção — a tela decidiria por ele só por ele ter
  pedido para conferir.
- **Teto de 50 linhas, e o teto é DITO na tela.** Lista cortada em silêncio
  faz a conferência concluir o contrário do que os dados dizem.
- É `@exige_consulta`, não `@exige_operador`: isto só lê, e ler o que
  fundamenta uma decisão não pode ser mais difícil do que tomar a decisão.

**Desempenho, e por que a condição redundante fica:** a consulta compara o
documento INTEIRO, e o índice da migração 010 é sobre a **raiz** (8 dígitos).
Sem ajuda, o banco varria as 59 mil SPs a cada clique — **0,11 s aqui**, e o
banco do Render tem um décimo de um núcleo. Filtrando primeiro pela raiz (uma
condição redundante, de propósito) o banco usa o índice: **0,001 s**. As duas
condições juntas dão exatamente o mesmo resultado da exata sozinha.

#### O que foi verificado

- Suíte completa com Postgres de verdade: **4.985 passaram, 129 pulados**
  (10 testes novos).
- As duas telas abertas no Chromium com base semeada. Medido **linha a
  linha**, e não na tela toda — a primeira medição deu falso positivo porque o
  seletor pegava também a SP da nota que já está num lançamento:
  - nota órfã → sugeridas `7001, 7003, 7005, 7004`; **`7002` fora**, no bloco
    "1 SP deste CNPJ já tem nota associada — fora da sugestão";
  - `7003`, que tem o nº da NF no card mas nunca foi associada, **continua
    sugerida** — que é o ponto do parágrafo acima;
  - "ver as SPs" abriu por cima, listou as 3 SPs do nome e **não mexeu na
    escolha marcada**.

#### O que NÃO foi verificado

- **Nada disto rodou contra a base de produção.** O volume foi simulado
  (59.000 SPs, 4.000 notas geradas aqui).
- **A consulta à Receita continua sem um único acerto real** — esta máquina
  não tem saída para o brasilapi.com.br. O primeiro clique em produção é o
  teste real.
- O bloco `.ja-com-nota` não foi visto em celular estreito.

---

### Quadragésima sétima leva (13/09) — o escopo da tela, a nota parcelada, e um defeito que EU criei

**Publicada na `main` no começo desta sessão:** a 45ª leva (commit `75ab256`).
A 46ª (SP já com nota fora da sugestão; "ver as SPs" nos credores) está no
ramo, **não publicada** — o dono perguntou por ela em uso (*"não funcionou.
Você sabe que não deployou ainda?"*) e a resposta é essa: está pronta, falta
juntar.

**⚠️ PENDENTE DO DONO:** apertar **"Aplicar atualizações do banco"**. As
migrações 010 e 011 foram para produção e continuam não aplicadas.

#### 1. O botão "fechar" das janelas — defeito que a 45ª leva criou

*"Eu clico em ver os dados, dá um bug, o link de fechar não aparece, fica em
aguardando."*

**A causa fui eu.** O bloco global "todo botão mostra que está trabalhando",
que nasceu na 45ª leva, trata `submit` como ida ao servidor. Só que
`<form method="dialog">` — o jeito do próprio navegador fechar um `<dialog>` —
dispara `submit` e **não vai a lugar nenhum**. O bloco trocava "fechar" por
"Aguarde…" e desligava o botão; como a janela é uma só e fica na página, da
segunda abertura em diante ela vinha sem o fechar, presa até o destravamento de
um minuto. Não tinha nada a ver com "Nota de Débito", que foi só onde ele
reparou.

Conserto: formulário `method="dialog"` sai do bloco. **Lição:** um
comportamento global aplicado a "todo formulário" precisa saber que existe
formulário que não navega.

#### 2. Os endereços da planilha viraram links

*"Quando clicamos em ver dados, das informações que vêm da planilha vêm alguns
links, torná-los clicáveis."* Anexo (Dropbox) e card (Pipefy). Quem monta o
HTML é o `com_links` que **já existia** — ele escapa o texto (a descrição vem da
planilha, que qualquer um edita) e trata pontuação colada no fim do endereço.
Escrever um segundo transformador no navegador daria dois lugares divergindo.

#### 3. O ESCOPO da Documentação Fiscal — três cortes

Não são "mais um filtro": mudam o tamanho do universo, e por isso ficam
**escritos na barra**, no bloco "O que esta tela nem olha".

- **Antes de 2026 fica fora** — vale pelo vencimento **ou** pelo pagamento (a
  SP vencida em dezembro e paga em janeiro é trabalho de 2026).
  **Escolhi `>= 2026`, não `= 2026`:** com igual a tela esvaziaria sozinha na
  virada do ano, sem ninguém mexer em nada. Se ele quiser só o ano corrente, é
  uma linha.
- **A SP SEM DATA NENHUMA FICA.** Sem data não é "velha", é *sem data*. Sumir
  com ela tiraria da conta um trabalho que ninguém mais veria. Se em produção
  aparecer muita, vira decisão dele.
- **Status Pgt "Cancelado" fora por padrão**, com caixa para trazer de volta.
- **`(TRF)` no tipo de despesa nunca aparece** — transferência entre contas não
  gera nota. Este não tem caixa, porque não foi pedido com volta.

Os cortes vivem em `consultas.condicoes_do_escopo_fiscal`, dentro de
`_condicoes` — o mesmo caminho da lista, do resumo e do painel. Aplicar em dois
dos três traria de volta o defeito de 13/09 (painel dizendo "3 categorizados" e
a linha mostrando "—"). **E valem também para as SPs candidatas da visão por
nota**, senão a tela ofereceria para associar exatamente o que ela esconde.
**Solicitações NÃO herda nada disso**, senão a conta dele deixaria de fechar
com a SPsBD.

#### 4. A nota parcelada — com o caso que ele mandou

    SP 1441193033 · "Parcela 3/3" · Nº NF 1002924 · R$   696,34 · FRIGELAR
    Nota nº 1.002.924 ............................... R$ 2.089,02 · FRIGELAR

696,34 × 3 = 2.089,02. O sistema via diferença de R$ 1.392,68, não dava nenhum
dos 25 pontos do valor, e um par que qualquer pessoa fecha em dois segundos
ficava abaixo do corte de 60 e **nunca era proposto**. Pior: a tela dizia "o
valor é diferente" — verdade no número e mentira no sentido, que é a pior
espécie de erro, porque tem cara de conferência feita.

- `parcela_do_lancamento` lê "3/3" e "3 de 3"; "1/1" não é parcelamento.
- **A divisão tem de fechar no centavo.** Folga aqui casaria notas quaisquer:
  com 12 parcelas, qualquer valor numa faixa de dez reais viraria par. A folga
  de um centavo por parcela existe só para 100,00 ÷ 3.
- Teto de 60 parcelas: acima disso a divisão vira verdadeira por acaso.
- A conta aparece por extenso nas três telas que explicam o par.

**E a nota vai para as demais parcelas.** `parcelas_irmas` casa por CNPJ +
**mesmo nº de nota no card** + mesmo denominador + **ainda sem chave**. O nº da
nota é o laço forte: sem ele, "parcela 2/3 de 696,34" casaria com qualquer
outro parcelamento do mesmo credor — que num fornecedor de aluguel mensal é o
caso comum. Irmã que já aponta para outra nota **não é sobrescrita**.

#### 5. O clique em série, sem a página ir embora

*"Eu clico 'usar este'. Enquanto ele está pensando eu já vou pra outro e clico.
Só que parece que só aceita o primeiro. A página vai lá pra cima."*

A causa era o `location.reload()`: recarregar mata as gravações ainda no ar
(daí "só aceita o primeiro") e joga a rolagem para o topo de uma página de 200
linhas. Agora cada linha se resolve no lugar, várias podem gravar ao mesmo
tempo, e a pergunta de confirmação é **uma só por sessão de tela** — ela existe
para explicar a regra, e repetir a explicação a cada clique é o atrito que ele
pediu para tirar.

**Ficou de fora:** a janela "informar à mão" ainda recarrega. É uma edição
avulsa, e o risco de mexer nela agora não se paga.

#### 6. ⚠️ A acusação errada de "CNPJ da própria BWS" — defeito GRAVE, no ar

*"Na página de nomes está aparecendo um CNPJ errado e dizendo que é da BWS,
sendo que não tem nada a ver o CNPJ."*

A causa estava **escrita com todas as letras** no comentário antigo: *"a BWS é
sempre o destinatário"*. **Não é.** A busca baixa, pelo certificado da empresa,
também as notas que a BWS **emite** — e nessas o destinatário é o **cliente**.
Cada cliente virava "um CNPJ nosso", e qualquer fornecedor com aquele número era
acusado **em vermelho e como CERTEZA**.

Acusar errado é pior do que não acusar: manda conferir o que está certo e ensina
a ignorar o alarme — justamente o que ele não pode ignorar no dia em que o
alarme estiver certo.

- `cnpjs_da_empresa()` passou a devolver `{cnpj: origem}`.
- Das notas, só o destinatário que recebe de **5 emitentes distintos ou mais** —
  a BWS recebe de centenas, um cliente recebe de um só.
- **CERTEZA agora só vem do CERTIFICADO DIGITAL**, que ele cadastrou com a mão e
  a senha. O resto é **suspeita**, com o texto dizendo por quê.

#### 7. O certificado que "aceita e não faz nada"

*"Eu coloco o certificado, boto a senha, ele aceita (…) mas simplesmente nada é
feito, nada é executado, e eu não sei o que está acontecendo."*

Ele estava certo em **duas** coisas ao mesmo tempo:

1. **Guardar o certificado não dispara busca nenhuma** — é preciso apertar
   "Buscar notas na Receita" — e a tela de Configurações nunca disse isso.
2. **A busca que falhava não deixava rastro visível.** A tela lia só o ponteiro,
   e o ponteiro só era escrito quando a busca dava certo. O caso em que ele mais
   precisa saber o que houve era o único que não contava nada — e a tela
   continuava dizendo "a busca nunca rodou".

Agora `sefaz.registrar_falha` grava hora e motivo **sem mexer no NSU** (zerar o
ponteiro faria a próxima rodada reler tudo e bater no limite da Receita, que é
como se perde o acesso por consumo indevido), e a tela de Configurações ganhou
a coluna **"A busca na Receita"** por certificado: "nunca rodou" com o caminho
do botão, "tentou e NÃO conseguiu" com o motivo, ou quantos documentos vieram.

**O que a aceitação do upload JÁ PROVA, e vale ele saber:** o sistema abre o
`.pfx` com a senha na hora de subir. Se aceitou, **a senha está certa e o
arquivo é um A1 com chave privada** — o CNPJ e a validade saem de dentro dele.

#### O que foi verificado

- Suíte completa com Postgres de verdade: **5.017 passaram, 129 pulados**.
- A aplicação sobe (18 blueprints).
- No Chromium, com o caso real dele semeado:
  - escopo → só as 3 parcelas; com "trazer cancelados" → entra a `8002`;
  - "fechar" funciona na 1ª e na 2ª abertura;
  - links do Dropbox e do Pipefy clicáveis na ficha;
  - a conta "parcela 1/3 de R$ 696,34 × 3 = R$ 2.089,02" aparece, e a SP virou
    **Proposta de correção**;
  - associar: **a página não recarregou**, a rolagem ficou em 400px, e a linha
    mostrou "✔ associada à SP 1441193033 · NF-e (Mercadoria) · e mais 2
    parcela(s): 1441193031, 1441193032";
  - Configurações mostra "tentou e NÃO conseguiu — a Receita respondeu 656
    Consumo Indevido" e "nunca rodou" com o caminho do botão.

#### O que NÃO foi verificado

- **Nada rodou contra a base de produção.**
- **A busca na Receita nunca foi exercitada de verdade daqui** — não há
  certificado fora do Render, e esta máquina não fala com a SEFAZ. O que se
  consertou foi o RASTRO da falha, não a causa dela: se a busca lá está
  falhando, agora a tela dirá por quê — e só então dá para consertar.
- A consulta ao brasilapi.com.br continua sem um acerto real daqui.
- O corte dos 5 emitentes é uma escolha minha, não um número medido na base
  dele.

---

### Quadragésima oitava leva (13/09) — o "Usar este" que subia a tela, e o parcelamento dito ANTES do clique

Publicada a 46ª + 47ª na `main` (`94fb023`) antes desta.

#### 1. "Usar este" — eu tinha consertado a tela ERRADA

*"Clico usar este, continua subindo a tela. Clico em dois e acho que ele
somente resolve um."*

Na leva anterior eu tirei o recarregamento dos botões de **associar nota**, na
Documentação Fiscal. **O "Usar este" dele é o da tela de CREDORES** — outro
lugar, outro mecanismo, e lá o defeito continuava inteiro.

Ali cada fornecedor é um `<form method="post">` próprio e cada envio era uma
página inteira indo e voltando. Duas consequências, e ele viu as duas:

- a rolagem voltava ao topo de uma lista longa;
- **o segundo clique ABORTAVA o primeiro**, que ainda estava no ar — daí "só
  resolve um".

A rota passou a responder JSON quando vem o cabeçalho `X-Sem-Recarregar: 1`, e
a tela envia por trás. **Sem JavaScript continua funcionando**: o formulário é
de verdade, o POST é de verdade, e sem o cabeçalho a resposta segue sendo
redirecionamento. Há teste para os dois caminhos.

O formulário ganhou `data-sem-aguarde`, senão o bloco global do "Aguarde…"
brigaria com este pelo mesmo botão.

**Medido no navegador, com 15 fornecedores na tela:** dois cliques em sequência
(60 ms entre eles) → **os dois resolveram**, e a rolagem **mexeu 0 px**.
⚠️ A primeira medição deu "a página subiu" e era **falso**: o Playwright rola
até o elemento antes de clicar. Medir com o clique disparado por JavaScript,
sem rolagem automática, é o que deu o número certo.

#### 2. O parcelamento, dito ANTES do clique

*"Clico em ver os dados de uma sugestão. Claramente é a situação de parcelas
que informei. Não deveria haver uma associação com as outras parcelas pra
vincular logo tudo? Ou avisar que já tá associado com outras?"*

Ele apontou um buraco de **informação**, não de comportamento: gravar nas irmãs
já acontecia desde a 47ª — mas só se descobria **depois** de clicar. Ação que
alcança mais do que se vê tem de ser anunciada antes.

`panorama_das_parcelas` (não escreve nada) mostra **TODAS** as irmãs, inclusive
as que `parcelas_irmas` esconde de propósito — porque aquela é a lista de quem
VAI ser gravado, e a de fora é justamente a que responde "já tá associado com
outras?". Três estados, e cada um com cor:

- **sem nota** (verde) — vai receber esta mesma nota;
- **já com esta nota** (azul);
- **com OUTRA nota** (vermelho) — não será mexida.

E o **rótulo do botão passa a dizer o alcance**: "Usar esta nota nas 2 SPs do
parcelamento". Quando falta o nº da nota no card, a janela diz que sem ele não
dá para achar as irmãs com segurança — calar deixaria parecer que não há
parcelamento.

#### O que foi verificado

- Suíte completa com Postgres de verdade: **5.025 passaram, 129 pulados**.
- A aplicação sobe (18 blueprints).
- No Chromium, com uma parcela plantada apontando para OUTRA nota: o aviso
  saiu "Achei outras 2: 1 ainda sem nota — vão receber esta mesma nota; 1
  aponta para OUTRA nota — não será mexida", com as duas etiquetas coloridas e
  o botão dizendo "nas 2 SPs do parcelamento".

#### O que NÃO foi verificado

- Nada rodou contra a base de produção.
- A busca na Receita continua sem exercício real daqui.

---

### Quadragésima nona leva (13/09) — a Receita sem subir a tela, o certificado que "não é aceito", o que a IA disse, e dois filtros

Publicadas na `main` antes desta: a 48ª (`ee2c103`).

#### 1. A consulta à Receita deixou de recarregar a página

*"Quando consulta o nome na Receita, a tela sobe. O resultado deveria aparecer
flutuante, ou de forma que não mexa na tela."*

O resultado é sobre UM fornecedor no meio de uma lista longa, e ia para um
aviso no ALTO da página depois de recarregar tudo — a resposta chegava longe
da pergunta. Agora é escrita logo acima do próprio botão. **Medido: 0 px de
rolagem**, e "consultar de novo" substitui a linha em vez de empilhar.

Mesma regra do "Usar este": a rota só responde JSON com o cabeçalho
`X-Sem-Recarregar`; **sem JavaScript a tela continua funcionando**.

#### 2. ⚠️ O certificado que "não é aceito" — e a desconfiança dele estava certa

*"Suspeito que o certificado e a senha estejam corretos, mas a mensagem é de
certificado inválido ou senha. Existe algum canto que eu possa tirar essa
prova?"*

**MEDIDO aqui, com um .pfx de verdade e a senha de verdade:**

    senha com espaço no FIM ......... FALHOU
    espaço no COMEÇO ................ FALHOU
    senha de verdade errada ......... FALHOU

As três davam **a mesma mensagem**. Quem copia a senha de um e-mail ou de um
PDF traz o espaço junto — e recebia "senha errada" sem ter errado a senha.

- `_senhas_a_tentar` tenta a senha como veio, sem os espaços das pontas, em
  latin-1 (quando há acento) e vazia (quando não foi digitada). ⚠️ **Não é
  "aceitar qualquer coisa"**: são formas da MESMA senha que o teclado e o
  copiar-e-colar produzem sem a pessoa querer. Nenhuma abre certificado de
  senha diferente, e há teste cravando que a lista não cresce além disso.
- `_diagnostico` separa **arquivo errado** de **senha errada** — antes a
  mensagem dizia as duas ao mesmo tempo, e por isso não dizia nenhuma.
  Reconhece .pem/.crt, PDF, arquivo vazio e senha em branco.
- **"Conferir sem guardar"**, em Configurações: sobe o arquivo, informa a senha
  e a tela diz o que aconteceu. Não grava nada e não mexe no que está em uso.
  Quando abre com a senha aparada, ela diz isso com todas as letras.

**Descartado pelo caminho, e fica registrado para ninguém refazer:** suspeitei
de criptografia antiga (RC2, comum em A1 brasileiro, que o OpenSSL 3 joga no
provedor `legacy`). **Testei e não é**: gerei um `.pfx` com `-legacy` e a
biblioteca abriu normalmente. A causa é a senha, não o algoritmo.

#### 3. O que a IA disse, e onde isso fica

*"Mandei pra IA e então, o que acontece? O que foi que a IA disse? O que foi
sugerido? Ficou gravada essa informação onde?"*

**Ficou gravada desde sempre** — em `sp_fiscal_analise`, com categoria, chave,
confiança e o MOTIVO escrito pela IA. A tela é que não mostrava nada disso.

- A linha ganhou a etiqueta de **quem preencheu** ("a IA leu o anexo", "uma
  pessoa informou aqui", "o sistema conciliou", "já veio do card"), com a
  confiança e o motivo no rótulo de passar o mouse.
- A janela "ver os dados" mostra o motivo por extenso e a confiança.

#### 4. Filtrar por confiança

*"Deveria poder filtrar por confiança."* Quatro faixas, em SQL, sobre a
confiança **gravada**. ⚠️ O nome do grupo diz "do que está gravado" porque há
DUAS confianças na tela: a gravada (no banco, filtrável) e a que o sistema
calcula ao abrir para a linha ainda não decidida (só das 200 da página).
Filtrar pela segunda responderia "nesta página".

Zero conta como **sem confiança gravada**, e não como "baixa": a linha que veio
pronta do card entra com zero, e chamá-la de baixa mandaria conferir o que
ninguém aqui decidiu.

#### 5. Filtrar pela qualidade do par — ⚠️ e o que ficou de fora, medido

*"Deveria poder também filtrar pela nota de associação: 1-4, 2-4, ou pelo
percentual também."*

Construí as cinco faixas, medi as cinco, e **só a de 100% se paga**. Com 59.000
SPs e 4.000 notas, no pior caso:

    confere nos 4 (100%) ....... 0,02 s
    confere em 3 ou mais ....... 0,74 s POR CONSULTA
    confere em exatamente 3 .... 0,70 s POR CONSULTA

E a tela roda a consulta DUAS vezes (a contagem e a página). No banco do
Render, com um décimo de um núcleo, isso é **a tela que não abre** — o defeito
que já custou duas correções nesta mesma tela.

**Por que só o de 100% é rápido:** é uma conjunção de igualdades, resolvida
pelo índice. Os outros perguntam "2 dos 3", e o "ou" faz o planejador desistir
dos índices e juntar a tabela inteira — o EXPLAIN mostrou **262.497 pares**
avaliados. Tentei três caminhos e registro os três para ninguém repetir:

1. separar em EXISTS independentes (é equivalente) → **piorou**, 2,4 s;
2. pôr a condição necessária `(valor OU número)` na frente → 0,74 s;
3. forçar subconsulta escalar para impedir o semi-join → 0,62 s.

**Migração 012**: coluna GERADA `nf_num` (o nº da nota já normalizado) e dois
índices compostos. Ajuda o recorte de 100%; sozinha **não resolveu** os outros,
porque a conta era do tamanho da junção, e não do `regexp`.

**O que destrava as faixas que faltam:** guardar a conta do par no banco, numa
varredura em processo separado — a mesma oferta que faria "o que o sistema está
propondo" virar totalizador de verdade. **Continua sem resposta do dono.**

#### 6. O segundo jeito de "já estar associado" (ele cobrou de novo)

*"Eu já havia comentado isso (…) tá aparecendo registro já associado. A MENOS
QUE A ASSOCIAÇÃO ESTEJA PROVAVELMENTE ERRADA."*

Na 46ª eu cortei só quem tem CHAVE gravada aqui — metade do conserto. O card do
Pipefy também tem "Nº NF": preenchido com número DIFERENTE, aquela SP já está
falada por outra nota.

⚠️ **Número IGUAL continua sendo sugestão**, e a distinção é o coração da
coisa: ali o número CONFIRMA o par. Cortar por "tem número" esconderia o par
mais fácil da base; não cortar por "tem número diferente" enchia a lista de
trabalho já feito. E a ressalva dele está atendida: nada some — vai para o
bloco à parte **com o motivo escrito**, que é onde ele confere se a associação
anterior está errada.

#### O que foi verificado

- Suíte completa com Postgres de verdade: **5.051 passaram, 129 pulados**.
- A aplicação sobe (18 blueprints).
- No Chromium: consulta à Receita com **0 px** de rolagem e resposta ao lado da
  pergunta; "consultar de novo" substitui em vez de empilhar.
- O diagnóstico do certificado distingue os seis casos (espaço no fim, nos dois
  lados, senha exata, cedilha, senha errada, arquivo .pem).

#### O que NÃO foi verificado

- Nada rodou contra a base de produção.
- A busca na Receita continua sem exercício real daqui.
- **A conferência do certificado não foi feita com o certificado DELE** — foi
  com um gerado aqui. O primeiro uso real é o teste real.
- O número de 0,74 s é desta máquina; o do Render é estimado por proporção, não
  medido.

---

### Quinquagésima leva (13/09) — ⚠️ o defeito que mantinha a busca na Receita SEM FUNCIONAR, e o "não marcar algum"

#### 1. ⚠️ O CERTIFICADO — A CULPA ERA NOSSA, e o dono estava certo desde o começo

Ele insistiu três vezes que a senha e o certificado estavam corretos. **Estavam.**
A mensagem que ele mandou de produção fechou o caso:

> *"tentou e NÃO conseguiu — Não consegui abrir o certificado de
> 00079526000109: Certificado ou senha inválida!!!"*

A causa está no construtor da `erpbrasil.assinatura.certificado.Certificado`:

```
elif isinstance(arquivo, bytes):
    self._arquivo = base64.b64decode(arquivo)     # <- assume BASE64
```

Ao receber `bytes`, a biblioteca assume que é o conteúdo **em base64**.
Mandávamos o `.pfx` **cru**. Ela decodificava lixo, o
`load_key_and_certificates` levantava `ValueError`, e ela traduzia isso para
**"Certificado ou senha inválida!!!"**.

**MEDIDO, com um .pfx de verdade e a senha certa:**

    Certificado(bruto, senha) ................. CertificadoSenhaInvalida
    Certificado(base64encode(bruto), senha) ... abriu

**É o pior tipo de defeito que existe:** a mensagem acusava a SENHA, o erro era
de quem chamava, e não havia como desconfiar olhando a tela. Mandou o dono
procurar no lugar errado por dias — trocar certificado, reconferir senha — e
foi por isso que a busca na Receita **nunca trouxe nota nenhuma**.

Duas coisas que a leva anterior fez e que foram o que permitiu achar isto:
o rastro da falha gravado no banco (sem ele, a tela continuaria dizendo "nunca
rodou") e o "conferir sem guardar" (que provou que o arquivo abria pelo nosso
caminho). Sem as duas, este defeito continuaria invisível.

Corrigido com teste cravando o ponto exato — um dia alguém "simplifica" isso de
volta.

#### 2. Deixar SPs de fora da equalização de nome

*"Às vezes não queremos renomear todos os lançamentos. O erro pode ter sido no
CNPJ e não somente o nome. Preciso poder não marcar algum."*

⚠️ **Por que isso é grave e não é refinamento:** quatro SPs com o nome de uma
locadora e o CNPJ de outra. O nome "certo" daquele CNPJ é o das outras trinta —
e reescrever as quatro **apaga a única pista** de que alguém digitou o CNPJ
errado. Depois disso elas ficam idênticas às certas e ninguém mais acha o erro.
O que fica de fora fica **errado de propósito**, à vista, esperando a correção
do número.

- Caixa de marcar por SP dentro do "ver as SPs"; desmarcada vira um campo
  escondido no formulário daquele fornecedor.
- **Fica guardado no formulário, e não numa variável solta**: fechar a janela,
  reabrir ou recarregar não perde a marcação.
- O que ficou de fora **aparece ao lado do botão** ("2 SPs fora: não serão
  renomeadas") e é dito de novo no aviso do resultado. Exclusão que só existe
  dentro de uma janela fechada é exclusão que se esquece.

#### O que foi verificado

- Suíte completa com Postgres de verdade: **5.057 passaram, 129 pulados**.
- A aplicação sobe (18 blueprints).
- No Chromium: desmarcar cria o campo escondido, o aviso aparece ao lado do
  botão, e a marcação **sobrevive a fechar e reabrir** a janela.
- O conserto do certificado foi provado contra a biblioteca de verdade, com um
  `.pfx` gerado aqui.

#### O que NÃO foi verificado

- **A busca na Receita ainda não rodou de verdade.** Esta máquina não fala com
  a SEFAZ e não há certificado aqui. O conserto é certo no ponto do defeito,
  mas só o primeiro clique em produção dirá se havia OUTRO problema atrás dele.
- Nada rodou contra a base de produção.

---

### Quinquagésima primeira leva (13/09) — a tela de VER, que ele pediu desde o começo

*"Desde o começo eu pedi uma tela simples pra poder visualizar similar ao que
eu visualizo na planilha. Uma tela das notas e outra tela dos registros com os
dados que estamos trabalhando. Similar à planilha. Mas até agora não foi
entregue."*

**Ele está certo, e a cobrança é antiga.** O que faltava não era dado — era a
TELA. Todas as telas deste módulo são de **trabalho**: cada uma mostra um
recorte, com painel, proposta e botão de agir. Nenhuma respondia à pergunta
mais simples que existe: *"deixa eu ver os dados"*.

#### O que faz esta ser "a planilha", e não mais uma tela de trabalho

1. **Todas as colunas, na ORDEM DA PLANILHA** (A, B, C…) — e não na ordem de
   uso. Ele lê a SPsBD por posição; a coluna "O" é o Status Pgt, e ele sabe
   disso de cor. São **34 colunas** do lado das SPs e 14 do lado das notas.
2. **A letra da coluna no cabeçalho.** É o detalhe que faz reconhecer.
3. **Nenhuma ação.** Sem propor, sem confirmar, sem marcar. Olhar não é mexer.
4. Uma busca só, e **ordenar clicando no cabeçalho** — como numa planilha.
5. **Cabeçalho e primeira coluna grudados**, que é o "congelar painéis": sem
   isso, 34 colunas ficam impossíveis de ler no meio.

#### Decisões que valem registro

- **Não reusa `consultas.listar`.** Aquela traz um punhado de colunas
  escolhidas e ainda calcula risco, atraso e agendamento por linha. Aqui é o
  contrário: tudo, e sem conta nenhuma por cima.
- **Data e valor ordenam pela versão CONVERTIDA.** Ordenar "10/01/2026" como
  texto põe outubro antes de fevereiro — a tela passaria a mentir numa coisa
  que ele confere de olho. Há teste para os dois.
- **A ordenação sai de uma lista fechada.** O nome da coluna vem do endereço;
  costurá-lo dentro do SQL é o caminho conhecido para mandar comando pela barra
  do navegador. Teste cravando que `ordem=id; DROP TABLE …` cai no padrão.
- **⚠️ NÃO herda o escopo da Documentação Fiscal.** Os cortes de lá (antes de
  2026, cancelado, TRF) não valem aqui: esta tela é "a planilha", e se ela
  escondesse linhas a conta dele deixaria de fechar com a SPsBD — que é
  exatamente o que ele vem conferir.
- **Sentido padrão diferente por aba:** a nota mais recente primeiro (é a que
  acabou de chegar); a SP pelo número, como na planilha.
- **Entra no menu DEPOIS da Agenda.** A ordem até ali é o caminho do dia dele,
  pedida com todas as letras. ⚠️ A tela nasceu furando essa ordem e **foi o
  teste que pegou** — agora o teste também crava que tela nova entra depois.

#### Desempenho, medido com 59.000 SPs e 4.000 notas

    lançamentos, 1ª página ...... 0,15 s
    lançamentos por valor ....... 0,01 s
    lançamentos, busca .......... 0,19 s (7.326 achadas)
    lançamentos, página 50 ...... 0,03 s
    notas, 1ª página ............ 0,00 s

#### O que foi verificado

- Suíte completa com Postgres de verdade: **5.067 passaram, 129 pulados**.
- A aplicação sobe (18 blueprints).
- No Chromium: 34 colunas com as letras (A ID, B Data, C Vencimento…),
  cabeçalho e primeira coluna **grudados**, notas abrindo da mais recente (▾),
  e **nenhum erro de console**.

#### O que NÃO foi verificado

- Nada rodou contra a base de produção.
- A tela não foi vista em celular estreito.
- Não há exportação para CSV nesta tela ainda — as outras têm; esta ficou só
  de ver. Se ele quiser baixar, é acrescentar.

---

### Quinquagésima segunda leva (14/09) — ⚠️ a busca na Receita andou, e parou em dois pontos novos

Publicadas na `main` antes desta: as levas 49, 50 e 51 (`64d8cf0`).

#### 1. ⚠️ OS DOIS DEFEITOS QUE A PRODUÇÃO ACUSOU — e o rastro funcionou

Com o certificado finalmente abrindo (o conserto do base64), a busca foi
adiante. O dono mandou a coluna nova de Configurações, e ela entregou os dois
erros com todas as letras:

    00079526000109  Notas (NF-e)   'TransmissaoSOAP' object does not support
                                   the context manager protocol
    00079526000109  Fretes (CT-e)  403 Client Error: Forbidden for url:
                                   https://www1.cte.fazenda.gov.br/…

**É a prova de que o rastro da falha (leva 49) valeu a pena.** Sem ele, a tela
continuaria dizendo "a busca nunca rodou" e estes dois defeitos seguiriam
invisíveis.

**Os dois são da MESMA FAMÍLIA do base64: usar a biblioteca de um jeito que ela
não suporta, com a mensagem apontando para outro lugar.**

**NF-e.** O código fazia `with TransmissaoSOAP(...)`. A classe **não** é um
gerenciador de contexto — quem é o método `cliente()` dela, que o serviço chama
por dentro. O `with` estourava ANTES de qualquer conversa com a Receita.

**CT-e.** O pedido é montado à mão (a biblioteca não cobre CT-e) e era postado
pela `session` que a `TransmissaoSOAP` guarda — que é uma sessão **comum**. O
certificado só é preso a ela dentro do `cliente()`, que grava chave e
certificado em arquivos temporários. Postando pela sessão crua, a Receita via
um visitante sem identidade e respondia **403**. Agora usa `ArquivoCertificado`,
o mesmo caminho que a biblioteca usa por dentro.

⚠️ **O teste NÃO prova que a busca funciona** — não há certificado nem saída
para a SEFAZ aqui. Ele prova que o código chama a biblioteca do jeito que ela
pede, e crava os dois pontos exatos. Os dois defeitos eram de CHAMADA, não de
rede, e é isso que dá para travar daqui.

#### 2. A planilha saiu do menu e virou visão da Documentação Fiscal

*"Está lá 'ver os dados', está muito solto, não tem vínculo com nada. Está ruim
da forma que está. Aqui tem 'por lançamento', 'por nota' — aí você colocar aqui
dentro. E 'ver os dados' também está foda, tem que ter uma nomenclatura
melhor."*

**Ele está certo nas duas coisas, e as duas são a mesma:** a tela nasceu **sem
contexto**. Ela é da Documentação Fiscal — é ali que ele está quando quer
conferir o dado cru contra o que a tela de trabalho está afirmando. Solta no
menu de cima, virava destino sem volta e sem parentesco.

E o nome não dizia nada: "ver os dados" pode ser qualquer coisa. Agora usa a
palavra que ELE usa o tempo todo — **planilha**.

A barra da Documentação Fiscal passou a ter **quatro visões**, com as duas
famílias separadas por um traço:

    Por lançamento | Por nota  ┊  Planilha das SPs | Planilha das notas
    └─ trabalho ─────────────┘   └─ ver, sem nada para clicar ───────┘

⚠️ **A barra virou UM arquivo só** (`analisesps_fiscal_visoes.html`). Eram
cópias, uma por tela — e foi exatamente por isso que a visão nova nasceu órfã
da primeira vez: quem estivesse na tela de fora não tinha como descobrir que
ela existe. Há teste cravando que nenhuma tela desenha a barra por conta
própria.

- `/analisesps/planilha` continua existindo, redirecionando — ele pode ter
  guardado nos favoritos, e quebrar em silêncio seria pior do que não ter
  mudado.
- Há teste garantindo que o texto "Ver os dados" não sobrou em template nenhum.

#### O que foi verificado

- A aplicação sobe (18 blueprints).
- No Chromium: as quatro visões aparecem nas quatro telas, cada uma com a sua
  acesa, **nenhum erro de console**, e o endereço antigo caindo no lugar certo.

#### O que NÃO foi verificado

- **A busca na Receita continua sem rodar de verdade daqui.** Os dois consertos
  são certos no ponto do defeito, mas o próximo clique em produção pode revelar
  um terceiro ponto atrás deles — foi assim nas duas últimas vezes.
- Nada rodou contra a base de produção.
- As quatro visões não foram vistas em celular estreito: a barra tem agora
  quatro itens e pode quebrar.

---

### Quinquagésima terceira leva (15/09) — o porquê do Omie, o corte de 2026 na planilha e o quadro por categoria

**Publicada na `main` em 15/09/2026 (`b9080c2`), junto com a leva 52.**

#### 1. ⚠️ "Baixa na planilha e não baixa no Omie" — e a recusa de trocar o remendo pela causa

*"Você marcar a planilha só depois do Omie confirmar NÃO é resolver a causa
raiz. A causa raiz é saber POR QUE não está baixando no Omie, porque se eu
estou mandando pra baixar é pra baixar."*

Ele está certo, e a correção é de método: a primeira proposta era **esconder o
sintoma** (só marcar a planilha quando o Omie confirmasse), o que deixaria o
mesmo título sem baixa no Omie — só que agora em silêncio nos dois lados.

O que a tela passou a fazer é **contar o que o Omie respondeu**. Cada
comprovante guarda a conversa passo a passo, com a frase de recusa do próprio
Omie (`faultstring`), e a tela mostra isso num link — "o que o Omie respondeu,
passo a passo". Migração **013** (`conversa_omie TEXT`).

⚠️ **A causa raiz continua EM ABERTO**, e isso é o que uma sessão nova precisa
saber: sem a frase do Omie, qualquer conserto aqui é chute. A suspeita
registrada — não comprovada — é que o `baixabradesco` manda uma **alteração no
título antes da baixa** apoiado na ideia de que "o Omie aceita repetição sem
reclamar", que nunca foi verificada. **Só o texto da conversa decide.**

#### 2. A Planilha das SPs passou a respeitar o corte de 2026

*"Eu fiz um filtro nelas pra exibir só o que é vencimento em 2026 ou pago em
2026. Aplique essa mesma [regra] lá. Porque só me interessa 2026, que é o lucro
real; antes era lucro presumido, não preciso dessa informação."*

O corte já valia na Documentação Fiscal e a planilha nova nascera sem ele — duas
telas do mesmo módulo dizendo números diferentes. Agora o corte é o mesmo, dito
na tela (não escondido) e com um "mostrar tudo" para o caso pontual.

#### 3. O quadro por categoria, com valores e com clique

*"A parte de KPI, pra eu saber quanto tem analisado, quanto não tem, quanto tem
nota, quanto tem contrato, quanto é fundo fixo, quanto está sem informação
nenhuma — isso em valores. E era interessante o KPI direcionar pra uma tela com
as informações."*

Quantidade **e valor** por categoria, barra proporcional, ordenado pelo valor, e
cada linha é um filtro: clicar leva à lista daquela categoria.

⚠️ **Defeito meu, pego pela suíte antes de sair:** o quadro foi montado DENTRO
do `try` da listagem. Uma falha nele apagava a tela inteira. Regra que fica:
**o acessório não pode derrubar o principal** — cada bloco de enfeite tem o seu
próprio `try`, com teste cravando isso.

---

### Quinquagésima quarta leva (15/09) — as DUAS seleções da tela de credores

*"Você propõe qual selecionar pra poder equalizar o nome do fornecedor, só que
da lista às vezes tem grupos de SPs que eu não quero alterar. Ou seja, tem que
ter duas seleções: a do nome, e em quais grupos vamos aplicar."*

**A tela fazia só metade da pergunta.** A bolinha escolhia o nome e, decidido
isso, TODAS as SPs daquele CNPJ eram reescritas. Só que o nome certo para o CNPJ
pode conviver com um grupo de SPs que não pertence àquele fornecedor — é o caso
do **CNPJ digitado errado**, o que esta tela mais erra. Reescrever aquele grupo
**apaga a única pista do erro**: depois disso as SPs erradas ficam idênticas às
certas e ninguém mais as acha.

Já havia a exclusão **SP a SP**, dentro da janela do "ver as SPs" (leva 50). Ela
resolve o mesmo caso no tamanho errado: quatro cliques, e só para quem lembrar
de abrir a janela. O grupo é a unidade que ele enxerga na lista.

Agora cada escrita é uma linha com caixa própria, recuada debaixo do nome:

    ( ) LOCADORA DO VALE LTDA        37 SP(s)
        [x] LOCADORA DO VALE LTDA    31 SP(s)   ver as SPs
        [x] LOCADORA DO VALE          6 SP(s)   ver as SPs
    ( ) TRANSPORTES XYZ               4 SP(s)
        [ ] TRANSPORTES XYZ           4 SP(s)   ver as SPs   ← fica como está

**As duas seleções somam:** dá para desmarcar o grupo inteiro e ainda tirar uma
SP avulsa de um grupo que ficou ligado. O aviso ao lado do botão conta as duas
separadas — somar num número só contaria duas vezes a SP tirada a dedo de um
grupo ligado.

#### Decisões de desenho, com o motivo

- **Dois campos por fornecedor**, e não um: o escondido (`grupo-<documento>`)
  diz quais grupos a tela mostrou, a caixa (`aplicar-<documento>`) diz quais
  ficaram ligados. Caixa desmarcada **não é enviada pelo navegador** — sem a
  lista do que existia, o servidor não teria como distinguir "ele desmarcou" de
  "esta tela é antiga e não manda isso". Tela que não manda grupo nenhum
  continua reescrevendo tudo, como sempre.
- **Nomeado por fornecedor** porque a pilha do "resolve sozinho" manda vários
  no mesmo envio, e dois fornecedores diferentes podem ter a mesma escrita. Uma
  lista única faria a exclusão de um calar a do outro. Há teste cravando isso.
- **Vale nas duas pilhas.** Na do "resolve sozinho" a caixa importa até mais:
  ela é aplicada em bloco, sem ninguém olhar linha a linha.
- **Funciona sem JavaScript.** As caixas são HTML puro; quem lê o que ficou
  marcado é a rota. O JavaScript só conta o que ficou de fora e apaga o grupo
  que já está com o nome escolhido (aquele em que desmarcar não teria efeito
  nenhum — e a pergunta "desmarquei e não aconteceu nada, quebrou?" seria certa).

#### O que foi verificado

- Seis testes novos com Postgres de verdade: grupo desmarcado não é reescrito;
  as duas exclusões somam; o caminho inteiro pela rota; a exclusão não vaza
  entre fornecedores; tela sem grupos continua reescrevendo tudo; a tela desenha
  as duas seleções.
- Suíte inteira verde.

#### O que NÃO foi verificado

- **Não foi exercitado com a base de produção** — aqui não há base real, e o
  dono ainda não apertou "Aplicar atualizações do banco".
- A tela não foi vista em celular estreito: a linha do grupo é recuada e pode
  apertar em tela pequena.

---

### Quinquagésima quinta leva (15/09) — ⚠️ o EVENTO que passava por nota e prendia a busca

**A busca na Receita FUNCIONOU pela primeira vez** — os consertos das levas 50
e 52 pegaram. E o primeiro dia de funcionamento entregou o defeito seguinte,
que só aparece com documento de verdade. O dono mandou a coluna de
Configurações:

    BWSPE   tentou e NÃO conseguiu — invalid input syntax for type date: ""
            LINE 1: ... ('35260505061744000130550010001835971000670305', '', …
    BWSSP   0 documento(s) · faltam 1130 para buscar
    BWS     tentou e NÃO conseguiu — invalid input syntax for type numeric: ""
            LINE 1: ...01365678191', '2026-06-16', '579630', '', 'CT-e', '', …

**A causa:** a Receita entrega, no MESMO lote das notas, os **eventos** ligados
a elas — cancelamento, carta de correção, ciência da operação. Todo evento
carrega o `chNFe`/`chCTe` **da nota a que se refere**, e nenhum tem data de
emissão nem valor. O teste em `ler_documento` era só o tamanho da chave: 44
dígitos, logo é nota. O evento passava, chegava à gravação com `''` nas duas
colunas que TÊM TIPO (`emissao DATE`, `valor NUMERIC`) e o Postgres recusava.

**E o pior não era a linha perdida, era o travamento:** o ponteiro do "até onde
já li" só anda DEPOIS da gravação. Com o lote morrendo no banco, a busca
recomeçava do mesmo NSU a cada rodada, para sempre. Era isso que ele via como
"tentou e NÃO conseguiu" duas vezes seguidas com a mesma mensagem.

#### Os três consertos, em camadas

1. **O evento é reconhecido** (`tpEvento`/`descEvento`/`nSeqEvento`) e não vira
   nota. ⚠️ O teste antigo cobria só o evento SEM chave — era esse o buraco.
2. **O evento de cancelamento vira notícia**, e não lixo: marca a nota que já
   está aqui como Cancelada. É a informação mais importante que esta busca
   traz — despesa paga contra documento que não existe mais. Ele **não cria**
   nota a partir do evento: sem emitente, valor e data seria uma linha fantasma.
3. **Vazio nunca chega a coluna com tipo** (`_linha_de_nota`, na gravação), e o
   lote que ainda assim falhar é regravado **um a um**, com rollback entre as
   tentativas — uma linha ruim não leva as outras quarenta e nove. O que não
   entrou é contado e aparece na tela, e **o ponteiro anda**.

**A regra que fica:** *nada que venha de fora pode travar o ponteiro*. Perder
um documento estranho com recado visível é barato; parar a busca inteira é
caro e silencioso.

#### O que foi verificado

- Oito testes novos (três sem banco, cinco com Postgres de verdade), inclusive
  o caso exato da produção: lote com nota + evento → a nota entra, o evento
  não, e o ponteiro avança.
- Suíte inteira verde.

#### O que NÃO foi verificado

- **Contra a Receita de verdade, não.** Vale o mesmo de sempre: aqui não há
  certificado nem saída para a SEFAZ. O próximo clique em produção é a prova.
- O BWSSP não estava travado — está andando (faltavam 1.130 documentos). Ele
  consome até ~1.000 por rodada, então são poucas rodadas.

---

### Quinquagésima sexta leva (15/09) — a busca FUNCIONOU, e "para onde foram as notas?"

**A prova chegou.** Depois do conserto do evento, o dono mandou a tela de
Configurações:

    BWSPE   112 documento(s)   15/09/2026 às 11:14
    BWSSP     0 documento(s)   15/09/2026 às 11:14 · faltam 1030 para buscar
    BWS      35 documento(s)   15/09/2026 às 11:14

Ou seja: **147 documentos entraram pela busca automática**, pela primeira vez
desde que este caminho foi escrito. E a pergunta seguinte dele é a boa:

> *"Como é que eu sei que eu estou visualizando essas notas que foram baixadas?
> (…) Eu só não sei pra onde é que elas estão indo. E se estão indo pra algum
> canto que é, onde é esse canto que eu não estou enxergando direito."*

**Ele está certo, e o buraco é de desenho:** a nota entra por DUAS portas — o
relatório do FSist e a busca na Receita — e as duas gravavam na mesma tabela
sem deixar dito qual trouxe a linha. As 147 de hoje eram indistinguíveis das
que já estavam ali. A busca podia estar funcionando perfeitamente e ele
continuaria sem ter como saber.

#### O que mudou

- **Migração 014**: coluna `origem` em `notas_fiscais`, com três valores —
  `receita`, `fsist`, `receita+fsist`. ⚠️ A segunda porta **não apaga** a
  primeira: soma. Se apagasse, o relatório do FSist (que roda a cada
  sincronização, depois da busca) zeraria o rastro da Receita em todas as
  notas — e a pergunta voltaria sem resposta na semana seguinte.
- **Duas colunas novas na frente da tabela**, antes da chave: "De onde veio" e
  "Entrou aqui em". A chave tem 44 dígitos; qualquer coluna depois dela só
  aparece rolando a tabela para o lado, e resposta que precisa de rolagem é
  resposta que não se acha.
- **Recorte por origem** na Planilha das notas: todas / só as da busca / só as
  do relatório. "Da Receita" inclui a que veio pelas duas — ela também foi
  trazida pela busca.
- **A data virou dd/mm/aaaa.** Estava saindo no formato do banco
  (2026-06-18), porque a célula do tipo "data" era escrita crua. Pedido dele
  na mesma mensagem.
- **O evento aparece na contagem:** "0 documento(s)" agora vem acompanhado de
  "N evento(s) da Receita (cancelamento, carta de correção) — não são notas".
  Sem isso, uma rodada inteira de eventos se lê como "não veio nada".
- **Recado da Receita deixou de ser pintado de vermelho.** Qualquer recado
  virava alarme — inclusive "Nenhuma nota nova desde a última consulta", que é
  a resposta boa. Alarme que toca no dia normal é alarme que se aprende a
  ignorar.
- **Configurações soma as duas buscas** (notas e fretes) por CNPJ. Mostrava só
  uma delas: o "112" que ele leu era de uma linha só.

⚠️ **A janela entre publicar e apertar o botão** vale aqui também: a coluna
`origem` nasce na migração 014, e o código sobe antes. Tanto a gravação quanto
a tela perguntam se a coluna existe (`tem_coluna`) e seguem sem ela. **Há teste
que derruba a coluna de propósito** e exige que a busca e a tela continuem de
pé.

#### O que fica em aberto

- **Por que a BWSSP traz 0 documentos** com 1.030 na fila. Ela não está
  travada — o ponteiro anda —, mas em duas rodadas andou 100 NSUs e trouxe
  nenhuma nota. Pode ser lote só de eventos, pode ser a Receita pedindo para
  esperar. A partir desta leva a tela mostra a frase dela; é o que decide.

---

### Quinquagésima sétima leva (15/09) — o recorte que devolvia vazio nos dois lados

Minutos depois de aplicar a migração 014, o dono voltou:

> *"Tem algo errado com a atualização do banco agora. Se eu boto todas,
> aparecem as notas aqui, seis mil e tantas. Se eu clico só as da Receita, não
> aparece nada. Se eu clico só as do relatório, não aparece nada."*

**Não estava errado, mas estava inútil — e para quem usa é a mesma coisa.** A
coluna `origem` nasceu vazia para as 6 mil notas que já existiam, porque vazio
é honesto: ninguém registrou por onde elas entraram. Só que **uma tela com dois
recortes que não devolvem nada não se lê como "ainda não sei"; lê-se como
"quebrou"** — e a leitura dele é a que vale.

**A lição, que vale para a próxima coluna nova:** recorte novo sobre dado
antigo nasce vazio, e isso tem de ser tratado NA MESMA LEVA — ou com
preenchimento do passado, ou com o número à vista dizendo quantas são. Publicar
o recorte e deixar a explicação para a conversa é transferir para ele o
trabalho de descobrir que não está quebrado.

#### O que mudou

- **Migração 015 preenche o passado até onde dá para afirmar.** A busca na
  Receita **não preenche o destinatário** (o resumo dela não traz); o relatório
  do FSist preenche. Então destinatário preenchido é **certeza** de relatório.
  ⚠️ **A regra só anda para um lado de propósito**: destinatário vazio fica
  vazio MESMO. No pior caso ela deixa de marcar — não mente. Chutar faria a
  tela responder com confiança uma pergunta que ninguém sabe responder.
- **O número vem junto do recorte**, antes do clique: "todas (6.132) · busca na
  Receita (147) · relatório do FSist (5.961) · entraram antes deste controle
  (24)". A soma dos três fecha com o total — é isso que faz a tela merecer
  confiança.
- **Quarto recorte, para as que não têm origem registrada.** Sem ele, a maior
  parte da base não teria onde aparecer e a conta não fecharia.
- **Recorte vazio explica-se**: "Nenhuma nota com este recorte. As que entraram
  antes de 15/09/2026 estão em 'entraram antes deste controle'."

#### O que NÃO foi verificado

- **Quanto do passado a migração 015 vai marcar na base de verdade** depende de
  o relatório do FSist dele ter trazido a coluna do destinatário. Se não tiver,
  aquelas notas ficam em "entraram antes deste controle" — visíveis e
  contadas, mas sem origem. Elas ganham a marca na próxima passagem de
  qualquer uma das duas portas.
- Nada rodou contra a base de produção.

#### Ficou pendente de perguntar

Ele começou uma frase e trocou de assunto: *"era interessante também uns,
nessa tela aqui na parte superior…"* — provavelmente totalizadores no alto da
Planilha das notas, como o quadro por categoria da tela de lançamentos. **Não
foi feito porque não dá para adivinhar o que ele quer somar.** Perguntar.

---

### Quinquagésima oitava leva (15/09) — os totalizadores das notas, e o que só o dono sabia

Duas coisas, uma dele e uma minha.

#### 1. O que o dono sabe e o banco não

> *"Em relação às notas, só pra explicar: tudo que já tem, que foi importado, é
> tudo da planilha. Só não foi importado em relatório dentro do Análise de
> SPs."*

Isso é informação que **nenhuma consulta produz**. A migração 015 tinha marcado
só o que dava para afirmar pelo dado (destinatário preenchido); o resto ficou
vazio porque o banco não sabia. Agora sabe, porque ele contou.

**Migração 016**, com a data como linha divisória — e ela é defensável: a busca
na Receita **nunca gravou uma nota antes de 15/09/2026** (todas as tentativas
anteriores falharam, e o próprio ponteiro registra isso):

    entrou ANTES de 15/09/2026      →  'fsist'    (o dono afirma)
    entrou HOJE, sem destinatário   →  'receita'  (só a busca grava assim)

O único caso que pode errar é uma nota que o relatório tenha trazido HOJE sem
destinatário — janela de horas, e o erro se conserta sozinho na próxima
passagem do relatório (vira 'receita+fsist').

**Também entrou o índice que faltava** para a pergunta "esta nota tem
lançamento?": o índice existente era sobre a coluna crua e a consulta usa
`regexp_replace`, então o Postgres varria o diário inteiro a cada pergunta. Com
o KPI novo isso passaria a rodar a cada abertura da tela.

#### 2. Os totalizadores do alto

*"Seriam os KPIs aí lá em cima, os totalizadores. Ficaria legal."*

Cinco números, cada um clicável (menos o total): **Notas**, **Sem lançamento**,
**Autorizadas**, **Canceladas**, **Fretes (CT-e)** — quantidade e valor.

- **"Sem lançamento" é a pergunta de dinheiro desta tela**: documento emitido
  contra a empresa que nenhuma SP declarou. Canceladas ficam fora da conta,
  pela mesma regra da visão "notas sem lançamento" — as duas têm de concordar.
- **O quadro conta sobre o MESMO recorte da lista** (busca e origem), por um
  filtro montado num lugar só (`recorte_das_notas`). Se cada lado montasse o
  seu, bastaria um ganhar condição nova para o quadro dizer "12 canceladas" e a
  lista mostrar outra coisa.
- O que o quadro MEDE (situação, sem lançamento) não entra na conta dele,
  senão cada número mediria a si mesmo.

⚠️ **Defeito meu, pego na primeira olhada no navegador:** criei um cartão de
KPI do zero — e o módulo **já tem** o componente (`.kpis/.kpi/.kpi-rotulo/
.kpi-valor`, usado em seis telas). O cartão novo nasceu torto na hora, porque
já existia um `a.kpi { display: block }` no arquivo. Componente repetido não é
só código a mais: é um jeito de a mesma tela ficar diferente de si mesma no
próximo ajuste. Agora usa o componente e o mesmo modificador (`fiscal-kpis`)
que tira o azul de link dos números.

#### O que foi verificado

- Suíte inteira verde; seis testes novos com Postgres de verdade, inclusive o
  que exige que a cancelada NÃO entre em "sem lançamento" e o que derruba o
  quadro de propósito para garantir que a tela continua de pé.
- No Chromium, com 40 notas semeadas: os cinco números batem com a base
  (40 / 34 / 35 / 5 / 8), cada clique recorta a lista, e não há erro de
  console.

#### O que NÃO foi verificado

- O custo do "sem lançamento" na base de verdade (6 mil notas × 59 mil SPs).
  O índice novo é exatamente o que essa consulta pede, mas a medição só dá
  para fazer lá.

---

### Quinquagésima nona leva (15/09) — "diz 112 documentos, mas só tem nove notas"

> *"Em Configurações diz 112 documentos na BWSPE e 36 na BWS Construções.
> Quando eu vou na planilha das notas, busca na Receita, só tem nove. E é tudo
> coisa de frete. (…) Tá estranho, como se tivesse alguma coisa equivocada."*

**Os dois números estão certos e medem coisas diferentes** — e a tela não
dizia isso, que é o defeito de verdade:

- **"documentos"** é o que a Receita ENTREGOU. Ela reentrega o histórico
  inteiro a cada varredura, do NSU zero em diante.
- **"notas da busca"** é o que virou linha NOVA aqui. A maior parte do que a
  Receita manda já estava na base pelo relatório do FSist: a nota é
  confirmada, não criada — e continua marcada como do relatório, que foi quem
  a trouxe primeiro.

#### O que mudou

- **A busca passa a dizer os três números que faltavam**, no recado que
  aparece em Doc. Fiscal › Por nota (coluna Situação): quantos documentos
  recebeu, **quantos eram nota nova aqui**, quantos já estavam, de que tipo
  (NF-e / CT-e) e **de que período de emissão**. É com o período que dá para
  responder "a Receita já me entregou as notas do dia 9 ao 14?" sem adivinhar.
- **"Documentos" virou "Documentos recebidos"** nas duas telas, com um
  parágrafo explicando a diferença.
- **O que a leitura não reconhece é contado**, em vez de sumir. Se a Receita
  mandar um formato novo, ele aparece como "⚠️ N documento(s) que não consegui
  ler" em vez de virar silêncio.

#### ⚠️ E um defeito de verdade, achado no caminho

**A segunda porta apagava o que a primeira sabia.** As duas entregam campos
diferentes da mesma nota: o resumo da Receita não traz destinatário nem as
NF-e de dentro do CT-e; o relatório do FSist traz. A gravação escrevia
`EXCLUDED.<campo>` puro — ou seja, **vazio por cima do preenchido** — toda vez
que a busca reentregava uma nota que o relatório já tinha trazido.

Isso derrubava justamente o campo usado pelas migrações 015 e 016 para saber
de onde a nota veio. Agora **campo que chega vazio não sobrescreve**; só o
`status` manda sempre, porque precisa poder virar "Cancelada".

Junto veio o cuidado que isso exige: a condição de "mudou alguma coisa"
compara contra **o valor que de fato será gravado**, e não contra o que
chegou. Sem isso, uma nota cujo valor a Receita não manda entraria em "mudou"
a cada rodada e seria regravada para sempre — o caminho exato das 14,3 milhões
de gravações inúteis de 10/09.

#### O que continua em aberto

- **As NF-e emitidas entre 9 e 14/09 não estão na base.** O relatório do FSist
  dele vai até 8/09, e a busca trouxe nesse período só CT-e. Não dá para saber
  daqui se a Receita ainda não as distribuiu ou se a leitura as descartou — a
  próxima rodada responde, porque o recado agora diz tipo e período do que
  veio.

---

### Sexagésima leva (15/09) — ⚠️ a tela do Bradesco mostrava 47 linhas EM BRANCO

O dono colou o resultado da conferência e ele fala por si: *"47 operação(ões)"*
seguidas de quarenta e sete linhas sem **nenhuma** célula preenchida. O mesmo
nos oito Pix.

**A CAUSA:** a linha da conferência é um dicionário com chaves **em
português** — `"Valor (Bradesco)"`, `"Credor (SP)"`, `"SP"` — e o template
pedia chaves **técnicas** — `valor`, `credor`, `id`. Nenhuma batia, e no Jinja
uma chave que não existe vira vazio em silêncio. O contador vinha do
`len()` da lista, e por isso continuava certo: a tela parecia funcionando.

**⚠️ POR QUE A SUÍTE NÃO PEGOU — e esta é a parte que interessa para a próxima
vez.** O teste da tela dublava `cruzar_tudo` e devolvia um dicionário com as
chaves **que o template queria**. Ou seja: ele provava que o template desenha o
que recebe, e **não** que recebe o que o código produz. O dublê escondia
exatamente o defeito que existia.

    # o que o teste mandava            # o que a produção mandava
    {"valor": "6.750,00",              {"Valor (Bradesco)": "6.750,00",
     "credor": "ACME"}                  "Credor (SP)": "ACME"}

**O conserto, em três partes:**

1. **As colunas saíram do template e foram morar em `bradesco.py`**
   (`COLUNAS_BOLETO`, `COLUNAS_PIX`), ao lado da função que monta a linha.
   Nome de campo que aparece em dois arquivos vira dois nomes diferentes no dia
   em que um dos dois mudar.
2. **Teste que percorre cada coluna** e exige que a chave exista na linha de
   verdade.
3. **Teste de ponta a ponta, sem dublê nenhum**: cola o texto do Bradesco,
   cruza de verdade e confere que a célula aparece preenchida — e confere pelo
   que **não** está no texto colado (o credor, a validação), porque o texto
   volta dentro da caixa de digitação e procurar pelo valor no HTML daria certo
   mesmo com a tabela vazia. A primeira versão deste teste passou sem provar
   nada justamente por isso.

**A tela ganhou colunas que já existiam no dado e não apareciam:** validação,
vencimento, status de pagamento e o nº da SP como o banco o leu (separado do
nº da SP que a conferência encontrou) — o que permite ver o caso em que os
dois diferem.

---

### Sexagésima primeira leva (16/09) — ⚠️ o QUARTO defeito da NF-e, e a tela que não dizia nada

#### 1. `invalid literal for int() with base 10: 'PE'`

Da tela dele, nas **três** empresas, em toda NF-e. A biblioteca faz
`self.uf = int(uf)` no construtor: ela quer o **código do IBGE** (26), não a
sigla. O código já existia neste módulo — era usado só na montagem do pedido de
CT-e — e faltava neste caminho.

**É o quarto defeito da mesma família**, e vale anotar o padrão: usar a
biblioteca de um jeito que ela não aceita, com a mensagem apontando para outro
lugar. Os quatro, em ordem: certificado entregue sem base64 → `with` numa classe
que não é gerenciador de contexto → CT-e postado sem o certificado → UF como
sigla. **Cada um só apareceu depois que o anterior foi corrigido**, porque o
primeiro erro escondia o seguinte.

⚠️ **E a conclusão que isso obriga:** a NF-e **nunca funcionou**. Os "112
documentos" e os "36" que a tela mostrava eram **CT-e** — o que explica, sem
mistério nenhum, por que as nove notas trazidas pela busca eram todas de frete.

#### 2. A falha estava à vista e não se chamava falha

O caminho que trata erro na consulta gravava a mensagem como **recado comum**.
A tela então mostrava "ainda há lote para buscar" com o erro do Python
pendurado ao lado, como se fosse informação. Agora esse caminho usa
`registrar_falha`, e a linha aparece como **"tentou e NÃO conseguiu"**.

Junto: **"em dia" passou a ser sobre a fila da Receita**, e não sobre haver
recado. A tela dizia "Falta buscar: —" e, na célula ao lado, "ainda há lote
para buscar". Duas células da mesma linha se contradizendo fazem quem lê
desconfiar da tela inteira — com razão.

#### 3. ⚠️ A tela que dizia "Disparado" e nunca mais dizia nada

> *"Quando clicamos em buscar não vemos em canto nenhum se a busca está de
> fato acontecendo, apenas uma mensagem dizendo que está sendo buscado. É ruim
> isso, ainda mais que não tá funcionando ainda de fato."*

**A CAUSA, e ela é sutil:** a tela só se recarregava depois de ter **visto** a
tarefa rodando, e perguntava de quatro em quatro segundos. Uma rodada **curta**
— e a busca estava falhando rápido, justamente por causa do defeito da UF —
começa e termina **entre duas perguntas**. A tela nunca via nada, nunca
recarregava, e ficava para sempre com "Disparado" na cara dele, mostrando o
resultado da rodada **anterior** (a de 13/09, com o certificado ainda
quebrado). Ou seja: **quanto mais rápido falhava, menos a tela contava**.

Agora há **dois** jeitos de saber que acabou, e basta um: ter visto rodando, ou
o carimbo da última execução concluída ter mudado desde o clique. E pergunta de
segundo em segundo nos primeiros quinze segundos. Se em um minuto não vir nada,
**diz isso** em vez de girar para sempre.

#### 4. O botão que ele mandou tirar

> *"Pra que diabo serve o botão 'Ler a aba do FSist na planilha'? Não tem
> sentido isso. Vou importar o relatório no sistema."*

Saiu do meio do trabalho fiscal. Era o caminho de antes de existir o formulário
de subir o arquivo; manter os dois lado a lado obriga quem usa a escolher entre
duas portas para a mesma coisa, e a certa depende de alguém ter colado o
relatório numa aba antes. A varredura das planilhas de apoio **continua** em
Configurações e na sincronização automática — saiu o atalho, não a função.

#### O que NÃO foi verificado

- **A NF-e contra a Receita de verdade.** O conserto é certo no ponto do erro
  (a biblioteca faz `int(uf)`, e agora recebe `26`), mas o histórico desta
  busca diz que **cada conserto revelou o próximo**. Só o clique em produção
  responde.

---

### Sexagésima segunda leva (16/09) — a NF-e sai da biblioteca

Quinto defeito seguido no mesmo caminho, e o último veio de dentro da própria
`erpbrasil`:

    name 'distDFeInt' is not defined

**A causa, e ela é do tipo que não dá para consertar de fora:** a biblioteca
importa os onze módulos de XML dentro de um `with suppress(ImportError)`. Se
qualquer um deles falhar no ambiente, **os nomes simplesmente não existem** — e
o erro não aparece na hora da importação, aparece lá na frente, na hora de
usar, com uma mensagem que fala de outra coisa. Daqui não dá nem para saber
qual dos onze falha no Render.

**A decisão:** a NF-e passou a ser montada à mão, como o CT-e — que funciona em
produção há dias e foi quem trouxe os 112 documentos. O envelope é o mesmo, com
outro namespace e outro endereço; dez linhas que dá para ler inteiras. O pedido
de distribuição **não é assinado**: quem autentica é o certificado da conexão.

**O que continua na biblioteca é o que importa:** abrir o certificado A1. Essa
é a parte difícil e perigosa (chave privada, formatos, senha), e ela faz isso
há anos para muita gente.

**A série inteira, para não se repetir** — cinco defeitos, cada um escondendo
o próximo, todos no mesmo caminho e nenhum visível daqui:

    1. certificado entregue sem base64        → "certificado ou senha inválida"
    2. `with` numa classe que não é gerenciador → "does not support the
                                                  context manager protocol"
    3. CT-e postado sem o certificado         → 403 Forbidden
    4. UF como sigla, não como código         → invalid literal for int(): 'PE'
    5. import engolido dentro da biblioteca   → name 'distDFeInt' is not defined

A lição: **dependência que só falha no ambiente do cliente custa uma ida e
volta por defeito.** Quando o pedaço que ela faz é pequeno e legível — um
envelope XML —, escrever à mão sai mais barato do que depurar às cegas. Quando
é grande e perigoso — abrir a chave privada —, não sai.

#### O que NÃO foi verificado

- **A NF-e contra a Receita de verdade, de novo.** Aqui não há certificado nem
  saída para a SEFAZ. O que os testes provam é que o envelope vai com o CNPJ, o
  NSU, a UF como número, o namespace da NF-e e o certificado preso à conexão —
  os cinco pontos onde já deu errado.

---

### ✔ A BUSCA NA RECEITA FUNCIONOU (15/09, 23:33) — a prova, para não se perder

A linha que o dono mandou da tela, e ela encerra cinco dias de conserto em
série:

    00079526000109  Notas (NF-e)  501 documento(s) recebido(s) da Receita ·
                                  15 nota(s) nova(s) aqui ·
                                  486 já estava(m) na base · 501 NF-e ·
                                  emissão de 15/06/2026 a 15/09/2026 ·
                                  202 evento(s), que não são notas

**O que isso prova, ponto a ponto:**

- O envelope montado à mão **é aceito** pela Receita (o caminho da biblioteca
  nunca chegou a falar com ela).
- O certificado na conexão **autentica** — sem ele seria 403.
- A leitura separa **nota de evento**: 501 notas e 202 eventos vieram no mesmo
  lote, e os eventos não viraram linha (era o defeito que travava o ponteiro).
- A gravação distingue **nova de já conhecida**: 15 contra 486.
- E responde a dúvida dele de 15/09: **as NF-e de setembro existiam** — a
  emissão vai até o dia 15. Elas não apareciam porque a NF-e nunca tinha
  funcionado; o que a busca trazia era só CT-e.

**As outras duas empresas levaram `cStat 656` ("consumo indevido")** na mesma
rodada: a Receita bloqueia por cerca de uma hora quem consulta demais em pouco
tempo, e elas foram consultadas várias vezes seguidas durante a depuração. Não
é defeito, e a próxima rodada resolve sozinha.

⚠️ **O que isso deixa como risco, e ainda NÃO foi tratado:** nada impede clicar
"Buscar notas na Receita" cinco vezes seguidas e levar o bloqueio de novo — o
sistema consulta mesmo quando o ponteiro já está no fim da fila. Uma trava
simples (não perguntar de novo, para um CNPJ já em dia, antes de passar uma
hora) evitaria isso. **Fica proposto, não feito.**

---

### Sexagésima terceira leva (16/09) — ⚠️ a coluna que ainda não existe derrubou a BAIXA

Relato do dono, com o lote inteiro recusado:

    column "conversa_omie" of relation "comprovantes_item" does not exist

**É a regra deste repositório, quebrada por mim.** A coluna nasce na migração
013; o código sobe para o Render **antes** de alguém apertar "Aplicar
atualizações do banco". Nessa janela, todo comprovante arrastado **falhava por
inteiro** — não é que ficasse sem a conversa do Omie: a baixa não acontecia.

Pior: **a LEITURA tinha a proteção e a GRAVAÇÃO não.** Eu protegi o lado que só
mostraria menos informação e deixei desprotegido o lado que perde trabalho. A
proteção vale para os dois lados, e o lado da escrita é o que importa mais.

Agora a gravação pergunta se a coluna existe (`tem_coluna`, o mesmo caminho do
`origem` nas notas) e grava sem ela quando não existe. **Há teste que derruba a
coluna de propósito** e exige que o comprovante ainda baixe.

#### E o lote que ficava "ainda processando" para sempre

Na mesma tela, dois lotes de 07:47 e um de 09:37 do dia anterior diziam *"Ainda
processando — as linhas vão aparecendo"*. Não estavam processando: um estava na
fila sem ninguém ter começado, os outros morreram no meio.

- A tela passa a dizer **há quanto tempo** e, passados 15 minutos, **que está
  parado** — com o motivo provável, que é diferente para cada caso (ninguém
  começou × o serviço reiniciou no meio).
- E ganhou o **botão que a mensagem prometia**: "Retomar a fila agora". Ele é
  um formulário comum, não `fetch`, de propósito — é o botão de destravar, e
  tem de funcionar mesmo se o JavaScript não carregar.

⚠️ **O que continua pendente e é do dono:** apertar "Aplicar atualizações do
banco". Sem isso a baixa funciona, mas a conversa com o Omie **não fica
guardada** — e é ela que responde por que o Omie recusa, que é a investigação
em aberto desde 14/09.

---

### Sexagésima quarta leva (16/09) — ⚠️ a baixa no Omie FUNCIONOU, e o botão que não retomava

#### ✔ A prova

O dono, depois de publicada a correção do nome da credencial: **"deu certo"**.
A baixa no Omie voltou a acontecer pela tela do Análise de SPs. A causa era o
nome da variável (`OMIE_KEY`/`OMIE_SECRET` no servidor, e este robô procurando
só `OMIE_BWS_APP_KEY`) — ver `baixabradesco/HISTORICO.md`, 16/09.

#### O botão "Retomar a fila agora" não retomava

*"Clico nele e nada acontece. Como zerar ele ou fazer com que ele retome
mesmo?"*

**E não acontecia mesmo.** Retomar só pegava lote em **ESPERANDO**, e o dele
estava em **RODANDO** — tinha começado e morrido no meio (o serviço do Render
reinicia de tempos em tempos). Um lote em RODANDO ficava assim **para sempre**:
nenhum processo o retomava, nenhum botão o alcançava, e a tela dizia que estava
parado sem oferecer saída de verdade.

Agora, antes de drenar a fila, o sistema **destrava o que ficou pelo caminho** —
lotes em RODANDO parados há mais de 15 minutos:

- **arquivo ainda no disco** → volta para ESPERANDO e é reprocessado (o que já
  baixou no Omie é reconhecido como duplicado e não baixa duas vezes);
- **arquivo sumiu** (o contêiner reiniciou e levou o disco) → vira FALHOU com o
  motivo escrito e o pedido de arrastar o PDF de novo. **Deixar em RODANDO
  seria mentir que ainda está trabalhando** — e era isso que acontecia.

⚠️ **Lote recém-começado NÃO é destravado.** Destravar o que está trabalhando
agora seria processá-lo duas vezes ao mesmo tempo. Há teste cravando os três
casos.

**E o botão passou a dizer o que fez.** Quando outra tarefa já está rodando, o
disparo é recusado — e a tela não contava nada, o que era metade do "nada
acontece". Agora responde: *"Não consegui começar agora: já está rodando."*

---

### Sexagésima quinta leva (16/09) — "já associada" tinha dois significados, e a tela só conhecia um

> *"Tava associando as notas e estranhei a quantidade. Quando abri algumas,
> muitas eram notas que já haviam sido associadas na planilha. Não era pra
> precisar fazer de novo."*

**Ele está certo, e o defeito é conceitual.** Uma nota pode já ter dono de duas
formas:

1. **pelo diário deste módulo** (`sp_fiscal_analise.chave`) — a associação
   feita AQUI, guardando a chave de acesso inteira;
2. **pelo card** (`sps.nf` → coluna gerada `nf_num`) — o **número da nota** que
   a equipe escreveu na coluna "Nº NF" da SPsBD, muito antes desta tela
   existir. **É a maior parte do trabalho já feito.**

A tela só olhava a primeira. Resultado: mandava refazer o que já estava feito —
exatamente o que ela existe para evitar. E pior do que o trabalho repetido é o
risco: associar de novo uma nota que já tem dono cria a chance de apontá-la
para a SP errada.

⚠️ **Número sozinho não basta**, e é por isso que a regra tem duas partes:
"nota 1430" existe em dezenas de fornecedores. A conferência exige o **mesmo
emitente** (raiz do CNPJ, 8 dígitos) **e** o mesmo número, sem zeros à esquerda
e sem pontuação — o mesmo par que a conciliação já usa como sinal forte. Com
raiz diferente, a nota continua órfã, que é o certo.

**O par (raiz, nf_num) é exatamente o índice composto da migração 012**, então a
conta nova não custa varredura.

#### E as TRÊS perguntas viraram uma

"Esta nota tem lançamento?" era perguntada em três lugares — a lista, o quadro
do alto e a visão das órfãs — e cada um perguntava do seu jeito. Três respostas
diferentes na mesma tela é o caminho mais curto para ninguém acreditar em
nenhuma. Agora as três chamam a mesma função, e há teste exigindo que os três
números batam.

⚠️ **A coluna `nf_num` nasce na migração 012.** Sem ela, vale só o diário — o
comportamento antigo — e a tela continua de pé. Há teste que derruba a coluna.

---

### Sexagésima sexta leva (17/09) — ⚠️ a ciência da operação: o módulo passou a ESCREVER na Receita

> *"Tem como visualizar fácil a partir da tela de associação, clicar e ver a
> nota fiscal? (…) E se esse PDF não pudesse ser gerado de imediato, que
> ficasse um processamento após a baixa das notas, baixando, gerando esses PDFs
> e salvando no Google Drive. Que tal fazer assim?"*

E, no dia seguinte, a autorização com todas as letras: **"Pode baixar as notas
dando essa ciência."**

#### ⚠️ Leia isto antes de mexer: este é o ÚNICO ponto do módulo que não é leitura

Tudo que o Análise de SPs fazia com a Receita era **perguntar**. A ciência da
operação (evento 210210) é **declarar**: um XML assinado com o certificado A1
da BWS, que entra no histórico daquela nota na Receita **para sempre** e não
tem como ser desfeito. Quem for mexer aqui precisa saber que erro nesta parte
não se conserta com um `UPDATE`.

Por isso o desenho todo é conservador, e cada trava abaixo existe por um
motivo:

- **É modo próprio, com botão próprio** ("Dar ciência e baixar as notas"), e o
  texto de ajuda do botão diz, sem enfeite, que ele escreve no sistema fiscal.
  Não pega carona na busca de notas — quem quer só olhar continua só olhando.
- **Nunca é automático.** A busca na Receita roda sozinha com a tela aberta; a
  ciência, não. Declarar em nome da empresa sem ninguém ter pedido seria o
  avesso do que o dono autorizou.
- **40 notas por rodada** (`ANALISESPS_CIENCIAS_POR_RODADA`). A Receita bloqueia
  por consumo indevido — já aconteceu com dois CNPJs em 15/09 — e ciência é
  escrita, que pesa mais que leitura.
- **Cada tentativa fica registrada** (`nota_evento`): quando foi, o que a
  Receita respondeu, o código, o motivo nas palavras dela e o protocolo. Sem
  isso ninguém responde "por que a BWS deu ciência nesta nota?" seis meses
  depois.
- **Não repete.** A chave primária é (chave, tipo): nota que já tem evento não
  entra na fila de novo. A Receita recusa a segunda ciência (código 573), e
  insistir é o caminho do bloqueio.
- **Só o que faz sentido manifestar:** modelo 55 (NF-e), emitida há menos de
  90 dias (fora disso a Receita não aceita mais), não cancelada, sem evento
  anterior, sem XML já guardado, e com destinatário de 14 dígitos — a ciência
  é do destinatário, e sem saber por qual CNPJ assinar não há o que enviar.

#### ⚠️ Ciência hoje, documento na PRÓXIMA rodada — e a tela diz isso

Esta é a parte que mais confunde, e não é defeito nosso: a distribuição entrega
o **resumo** da nota (chave, emitente, valor, situação). O **XML completo** só é
liberado ao destinatário **depois** da ciência — e aparece num lote seguinte da
distribuição, não na hora.

Quem apertar o botão e for procurar o arquivo no mesmo minuto não vai achar. Por
isso o recado do fim da tarefa diz, com todas as letras, que o XML de cada nota
chega na próxima busca na Receita. Sem essa frase, todo mundo conclui que
falhou.

#### É XML, não PDF — e a diferença importa

O dono pediu PDF. **Está guardando XML**, e a troca é consciente:

- **o que tem valor fiscal é o XML.** O DANFE é a representação imprimível
  dele; quem guarda documento, guarda o XML.
- **gerar DANFE exigiria biblioteca nova**, e não foi introduzida (regra da
  casa: dependência nova se avisa antes). A tabela `nota_arquivo` já tem a
  coluna `tipo` (`'xml'` | `'pdf'`) justamente para o dia em que houver PDF —
  os dois convivem sem migração nova.

**Enquanto isso, o XML abre.** O navegador mostra o conteúdo; quem quiser DANFE
bonito joga o arquivo em qualquer visualizador de NF-e da internet.

#### Onde o arquivo mora

No **Drive**, o mesmo caminho que já guarda comprovante e BeeVale — no banco
fica só o endereço. Um XML tem de 10 a 50 KB; seis mil notas seriam centenas de
megabytes num Postgres de 0,25 GB de RAM que já morreu de memória uma vez
(`CONTEXTO.md` §9).

`DRIVE_FOLDER_NOTAS` separa as notas dos comprovantes, para quem quiser; sem
ela vale a pasta geral. **Continua valendo a armadilha da cota**: tem de ser
Drive Compartilhado, senão o Google recusa (ver `drive.py`).

#### A tela de associação mostra a chave, e o número vira link

Na tela das notas, cada uma agora mostra a **chave de acesso** e o número vira
link, com três estados possíveis:

- **arquivo guardado** → clica e abre o XML no Drive;
- **ciência dada, documento a caminho** → diz isso, com a data da ciência;
- **nada ainda** → fica só o número, como era.

#### Como saber que a ciência funcionou

A Receita **não avisa** que aceitou: ela simplesmente passa a entregar o XML.
Por isso o recado da **busca de notas** agora conta também quantos documentos
inteiros foram guardados no Drive naquela rodada. É esse número, na busca
seguinte à ciência, que responde "valeu a pena?" — e o registro em
`nota_evento` responde "o que a Receita disse em cada nota".

#### Dois defeitos que a suíte pegou antes de publicar, e valem registro

1. **`disparo` não existia dentro de `executar_trabalho()`.** A etapa nova
   passava adiante o nome de quem disparou usando uma variável que só existe na
   função de fora — o que teria estourado com `NameError` **na primeira vez que
   o botão fosse apertado**, e só no processo separado, onde o erro é mais
   difícil de ver. Quem pegou foi o `test_nomes_indefinidos`, que lê o código
   procurando exatamente isso. É a segunda vez que esse teste paga o próprio
   custo.
2. **`lxml` importado direto sem estar declarado.** Já vinha instalado (é
   dependência do `erpbrasil`), então funcionava aqui e funcionaria no ar — até
   o dia em que o `erpbrasil` mudasse de dependência e derrubasse o módulo por
   um motivo sem relação nenhuma. Declarado.

#### Os testes da tela renderizam de verdade — a lição do Bradesco

Os três estados do número da nota são conferidos sobre o **HTML que sai**, com
o dado entrando pelo mesmo caminho da tela — não sobre o texto do template.

É a lição de 16/09, e vale repetir porque foi cara: lá o teste dublava a função
que monta as linhas e afirmava sobre as chaves que **o próprio teste** tinha
inventado. A tabela chegava em branco na produção e a suíte continuava verde.
Teste que inventa o dado de entrada só prova que o teste concorda consigo
mesmo.

#### O que NÃO foi verificado

- **A ciência contra a Receita de verdade.** Nesta máquina não existe o
  certificado A1 da BWS, e ele não vai ser trazido para cá — ver a decisão de
  15/09. O envelope, a assinatura, a leitura da resposta e todas as travas
  estão cobertos por teste; o que só a produção responde é se a Receita aceita.
  **A primeira vez tem de ser olhada**, e o registro em `nota_evento` é
  justamente o que permite olhar.
- **A subida do XML no Drive** também não foi exercitada de verdade aqui — ela
  usa o mesmo caminho do BeeVale, que já funciona no ar.

⚠️ **Pendente do dono: apertar "Aplicar atualizações do banco".** As migrações
013 a 017 estão à espera. Sem a 017 as duas tabelas não existem — e aí a
ciência não tem onde registrar nem onde guardar o endereço do arquivo.

---

### Sexagésima sétima leva (17/09) — a nota fiscal na tela, e o PDF que sai do navegador

> *"Não tem problema abrir o XML em tela. Abre num modal? E desse modal poderia
> exportar em PDF? Que tal?"*

A proposta dele resolveu o problema que eu tinha deixado em aberto na leva
anterior — e resolveu melhor do que a minha ideia.

#### O PDF sai do NAVEGADOR, e é por isso que não entrou biblioteca nenhuma

Eu tinha dito que o PDF exigiria dependência nova. Exigiria, **se o servidor
fosse gerar o PDF**. Não é: a folha de impressão é uma página HTML limpa, e
quem transforma em PDF é a caixa de impressão do navegador, onde "Salvar como
PDF" é um destino como qualquer impressora.

Três vantagens, e a terceira não é pequena:

- **nenhuma dependência nova** — a regra da casa, e este serviço já morreu de
  falta de memória uma vez;
- **sai melhor**: quem imprime escolhe margem, tamanho do papel, e se quer
  papel ou arquivo;
- **funciona igual no computador e no celular**, sem código separado.

#### ⚠️ O QUE SAI NÃO É UM DANFE OFICIAL, e o papel diz isso no alto

O DANFE tem forma definida pela Receita e vale como documento auxiliar de
circulação de mercadoria. O que sai daqui é uma **leitura do XML**: os mesmos
dados, organizados para conferir, arquivar e imprimir. **Não serve para
acompanhar carga na estrada.**

O aviso fica na primeira linha, não no rodapé, e sai impresso junto — porque
quem recebe um papel com cara de nota fiscal supõe que ele vale como uma. O que
tem valor fiscal continua sendo o XML, guardado no Drive.

#### O que mudou na tela de associação

O número da nota **abria o XML cru no Drive** — quem clicava recebia uma tela de
etiquetas, que não é "ver a nota fiscal". Agora abre o **modal**, com a nota
desenhada: emitente, destinatário, chave em blocos de quatro, mercadorias com
quantidade e valor, totais, transporte e informações complementares.

Reusa o mesmo modal que a ficha da SP já usava desde a conversão — não foi
criado nada novo. Continua sendo link de verdade: ctrl+clique abre a página
inteira em nova aba.

#### Três cuidados que o leitor do XML tem, e o motivo de cada um

1. **O total da nota é o `vNF`, não a soma dos itens.** Na nota de exemplo os
   itens somam 3.250 e a nota vale 3.400 — a diferença é o frete. Um papel que
   dissesse 3.250 seria o defeito mais caro possível aqui: passa despercebido
   justamente por parecer razoável.
2. **O que a nota não informou fica vazio, não vira zero.** "Não informou
   seguro" e "o seguro é zero" são coisas diferentes; escrever R$ 0,00 onde não
   houve informação é inventar dado. A linha some.
3. **O resumo não é desenhado como se fosse a nota.** O `resNFe` tem oito
   campos e nenhuma mercadoria — desenhá-lo daria uma folha quase vazia com cara
   de nota fiscal. A recusa é explícita, e diz o que fazer: o documento chega na
   próxima busca, depois da ciência.

#### Conferido num navegador de verdade

Não só por teste: a página, o modal e o PDF foram abertos no Chromium, com uma
NF-e de oito itens.

- **O PDF saiu com 2 páginas**, com o aviso, a nota inteira e **sem a barra de
  botões** — um botão "Imprimir" impresso dentro do próprio PDF seria
  constrangedor, e é o tipo de coisa que só aparece depois de imprimir.
- **O modal abriu, carregou a nota e o botão de PDF apontou para a nota certa.**
  Zero erro de JavaScript no console.
- **No celular (390px) a página rolava para o lado** — a tabela de nove colunas
  empurrava tudo, e o aviso e os totais saíam do campo de visão junto. Medido:
  583px de conteúdo para 390px de tela. Agora a rolagem está presa na tabela, o
  selo do número vai para baixo do emitente e a página mede 390 contra 390. Há
  teste exigindo o envelope de rolagem — e outro exigindo que ele **não** valha
  no papel, onde cortaria as últimas colunas do PDF sem ninguém perceber.

#### Detalhe pequeno que valeu o conserto

No cabeçalho do modal, "Salvar em PDF" era um `<a>` e ficava como texto solto ao
lado do "Fechar", que é botão. Dois controles lado a lado com aparências
diferentes fazem parecer que têm pesos diferentes — e o de PDF é justamente o
que a pessoa veio fazer ali. Ganhou contorno.

#### O que ficou de fora

- **O DANFE oficial**, com código de barras e faixas na posição que a Receita
  exige. Exigiria biblioteca nova, e não foi introduzida. A tabela
  `nota_arquivo` já prevê o tipo `pdf` para o dia em que houver.
- **A leitura de CT-e** (frete). O leitor entende NF-e; um CT-e cai na recusa
  explicada. As notas de frete continuam aparecendo na lista, só não abrem
  desenhadas.

---

### Sexagésima oitava leva (17/09) — o filtro funcionava; o que faltava era VER

> *"Se eu botar por categoria, quando eu clico em 'sem informação', não era
> para filtrar eles na listagem abaixo? Porque aí eu já ia trabalhando neles."*

#### ⚠️ Antes de mexer, foi conferido — e o filtro JÁ funcionava

Vale registrar o caminho, porque quase virou um conserto do que não estava
quebrado. Com Postgres de verdade e navegador de verdade, clicar em "(sem
informação)" **derrubava a lista de 6 SPs para 3**, e pelo totalizador
"Categoria vazia" também. O quadro e a lista contam pela mesma expressão SQL,
então não têm como divergir.

**A primeira versão do teste "provou" um defeito que não existia**, e o motivo
merece ficar escrito: a categoria **não é coluna da SP** — mora em
`analisesps.sp_fiscal`, que vem da planilha de apoio. Pôr `doc_fiscal` no
registro da SP não grava em lugar nenhum. As seis SPs ficaram todas sem
categoria e o filtro, corretamente, devolveu as seis. Parecia defeito; era
semeadura errada.

**A lição, que vale para a próxima:** teste que reproduz uma queixa precisa
provar primeiro que o cenário foi montado — aqui, que o quadro enxerga **duas**
categorias. Sem isso, "o filtro não filtrou" pode ser só "não havia o que
filtrar".

#### O que estava errado de verdade: a distância

Entre o topo da página e a lista há os totalizadores, o quadro por categoria, o
parágrafo da reconferência e o bloco "Trazer, ler e gravar" — **quase duas
telas**. O clique recarregava a página **no topo**, onde tudo parecia igual ao
que era antes. Quem clicava concluía que não tinha acontecido nada, e não
rolava para conferir.

Agora os atalhos que **mudam a lista** — o quadro por categoria e os
totalizadores do alto — apontam para o bloco da lista, que ganhou nome
(`id="lancamentos"`). O navegador cai direto nela, já filtrada.

Medido num navegador a 1440×800: antes a página ficava em **0px**; agora para
em **802px**, com a primeira linha da lista à vista. É literalmente o *"aí eu já
ia trabalhando neles"* do pedido.

#### Três testes, e cada um trava uma metade

1. **o recorte alcança a lista** — as SPs com categoria não aparecem;
2. **o quadro e a lista contam a mesma coisa** — se divergissem, o quadro diria
   "3" e a lista traria outra quantidade, e não haveria como saber qual dos
   dois está certo;
3. **o atalho leva até a lista** — que é a correção desta leva.

---

### Sexagésima nona leva (17/09) — reenviar um comprovante, e o aviso que não saía

Duas queixas do dono, no mesmo pedido, e as duas se resolvem no mesmo lugar.

#### 1. "Poder reenviar um comprovante a partir da tela"

> *"Às vezes os comprovantes não baixam por algum motivo. (…) Permitir o
> reenvio de algo que a gente não baixou. Opa, esqueci algum detalhe — o título
> não está no [Omie]."*

É o caso de todo dia: o comprovante não baixa porque **o título ainda não
existe no Omie**, ou faltou um dado no card. A pessoa conserta **lá** e quer
tentar de novo. Até agora o único caminho era achar o PDF e arrastar outra vez.

Cada lote ganhou **"Processar de novo"**. Aparece só onde há o que
reprocessar — lote terminado (pronto ou falhado) ou parado; num lote que está
trabalhando agora o botão convidaria a atropelar o próprio processamento.

⚠️ **Os itens antigos são apagados ao reprocessar, e isso não é detalhe.** Eles
são gravados com `INSERT` simples, **sem chave única**. Reprocessar sem limpar
mostraria **cada página duas vezes** na tela — e quem olhasse concluiria que o
comprovante foi baixado em dobro. O que já baixou no Omie continua baixado: é
reconhecido como duplicado e não baixa de novo, a mesma promessa que o botão
"Retomar a fila" já fazia.

⚠️ **Sem o PDF não há o que reprocessar.** O contêiner do Render reinicia e leva
o disco junto. Nesse caso o lote é **encerrado** dizendo isso, em vez de voltar
para uma fila onde falharia de novo a cada rodada.

#### 2. ⚠️ O aviso de "lote parado" que não saía — e a causa

> *"Uma coisa que está aparecendo lá e não sai é um retomar fila. Não sei por
> que está com aquela pendência e está assim."*

**Achado, e é um defeito de verdade.** O aviso da tela acende para lote parado
em **ESPERANDO ou RODANDO** (`parece_parado`), mas o destravamento
(`destravar_parados`) só alcançava **RODANDO**.

Resultado: um lote que ficou em **ESPERANDO sem o PDF no disco** — o contêiner
reiniciou entre o envio e o processamento — era escolhido pela fila a cada
rodada, falhava ao abrir o arquivo, e **o aviso acendia de novo na rodada
seguinte, para sempre**. O botão fazia exatamente o que devia; era a lista que
nunca esvaziava. Apertar não adiantava, e não havia como adivinhar por quê.

Agora as duas situações entram no destravamento: com arquivo, volta para a
fila; sem arquivo, é encerrado com o motivo escrito. **O que não pode é ficar
preso.**

⚠️ **E a trava do outro lado:** lote recém-solto **não** é destravado. Os 15
minutos protegem quem acabou de chegar — encerrar um lote antes de alguém
sequer tentar processá-lo seria trocar um defeito por outro pior. Há teste
cravando isso.

#### Conferido num navegador

Com um lote terminado trazendo exatamente o caso dele — *"Título não encontrado
no Omie. Inclua o título primeiro."* — e um lote preso há quatro horas. Os dois
botões aparecem, o clique devolve o lote à fila, e o recado diz o que
aconteceu de verdade: se a fila começou agora, ou se ficou esperando outra
tarefa terminar. Zero erro de JavaScript.

#### Uma armadilha de teste que vale registrar

O teste da tela falhou por um motivo que não tinha nada a ver com o botão:
**sem nenhuma SP na base, TODA tela do módulo devolve "a base ainda não foi
carregada"** e não chega a desenhar os lotes. Teste de tela deste módulo
precisa semear ao menos uma SP, senão falha por motivo errado — e faz perder
tempo procurando no lugar errado.

---

### Septuagésima leva (18/09) — o cabeçalho órfão: a regra existia, mas só num caminho

> *"Em algum momento eu pedi que, quando eu removesse cancelados ou pagos de um
> lote e ele ficasse vazio, o cabeçalho limpasse. Mas até agora não funcionou."*

**Ele tem razão, e o pedido dele é de 11/09.** O que foi feito naquele dia
funcionava — só que **por um caminho só**.

#### Há TRÊS jeitos de tirar SP do lote, e um deles ficou de fora

| Caminho | Limpava o cabeçalho órfão? |
|---|---|
| "Remover pagos" / "Remover cancelados" | ✔ desde 11/09 |
| "Remover duplicados" | ✔ desde 11/09 |
| **"Remover" da barra do alto** (marcar as linhas) | ✘ **nunca** |

Os dois primeiros passavam por uma função comum (`_limpar`), escrita justamente
para que as limpezas tratassem o título órfão do mesmo jeito. O terceiro
montava o texto à mão, linha por linha, e guardava **todo** título — esvaziado
ou não.

Como o dono trabalha marcando as linhas, era sempre esse o caminho que ele
usava. Do lado dele, o pedido simplesmente nunca funcionou.

#### ⚠️ O que fez o defeito durar quase uma semana: um comentário errado

Dentro do `remover_ids` estava escrito:

> *"Os títulos dos grupos ficam, mesmo que o grupo esvazie — apagar o título
> junto faria a remessa perder a divisão que alguém montou. **Mesma decisão do
> `remover_por_status` ao lado**."*

**A decisão do `remover_por_status` é a oposta**: ele tira o título junto. A
frase estava errada, e errada de um jeito específico — ela oferecia uma
justificativa coerente e uma referência cruzada falsa. Quem abrisse o arquivo
para investigar encontrava uma explicação pronta e parava ali.

**A lição, e ela é geral:** comentário errado é pior do que comentário nenhum.
Sem comentário, quem investiga vai ler o código do lado e descobre em um
minuto. Com um comentário errado, a investigação termina no lugar errado com a
sensação de ter sido concluída. Ao escrever "mesma decisão de X", conferir X.

#### E havia um TESTE cravando o comportamento errado

`test_remover_do_lote_tira_de_grupos_diferentes` afirmava, com comentário
explicando: *"os títulos ficam mesmo quando o grupo esvazia"*. Ou seja: a suíte
protegia o defeito. Corrigido junto, e o teste agora exige as duas metades — o
grupo que ainda tem SP mantém o título, o que esvaziou perde.

#### A regra, agora num lugar só e conferida nos três caminhos

`remover_ids` passou a usar o mesmo `_limpar` dos outros dois. Três testes
novos percorrem **os três caminhos de uma vez**: se um divergir de novo, é ali
que aparece. E a trava do outro lado continua valendo, nos três: **grupo que já
estava vazio antes continua** — alguém escreveu aquele título de propósito,
para encher depois.

#### Conferido na tela

Pelo caminho que ele usa (marcar as linhas e remover): o grupo que esvaziou
some com o título, o que ainda tem SP fica, e tirando a última SP o lote fica
realmente vazio — a tela diz "o lote está vazio" em vez de mostrar um título
sem nada embaixo.

---

### Septuagésima primeira leva (18/09) — "não pediu para atualizar o banco"

> *"Não foi preciso atualizar o banco, não pediu."*

Ele abriu Configurações depois da publicação e a tela não ofereceu nada para
aplicar. Duas explicações possíveis, e as duas merecem registro.

#### A explicação provável: a tela só conhece o código JÁ PUBLICADO

A lista de atualizações pendentes é a comparação entre **os arquivos `.sql` que
existem no código rodando** e a tabela de controle no banco. Se a tela é aberta
**antes de o Render terminar de publicar**, o código antigo está no ar — e ele
**nem sabe que as migrações 013 a 017 existem**. A tela então diz, com toda a
sinceridade, que não há nada pendente.

É uma armadilha de tempo, não um defeito: "tudo em dia" pode significar "tudo
em dia" ou "ainda não sei o que existe", e as duas frases são idênticas na tela.

#### ⚠️ O defeito de verdade que isso revelou: a ciência errava em SILÊNCIO

Se ele tivesse apertado "Dar ciência e baixar as notas" sem a migração 017
aplicada, a resposta seria:

> *"0 nota(s) com ciência dada (de 0 olhadas)."*

**Essa frase se lê como "não havia nada a fazer"** — e o que aconteceu foi
outra coisa: a consulta falhou porque as tabelas não existem, a falha foi
engolida, e a lista voltou vazia. Ele veria zero, concluiria que não há nota
esperando ciência, e **nunca descobriria que faltava um passo**.

É exatamente o defeito que o `CLAUDE.md` nomeia: *número errado com cara de
certo é pior que resposta nenhuma*. A diferença entre apertar o botão que
resolve e passar uma semana procurando defeito onde não há.

Agora a rotina **confere as tabelas antes de tudo** e, faltando, responde o que
falta, onde resolver — e avisa da armadilha de tempo acima, com todas as
letras: *"se a tela disser que está tudo em dia, a publicação ainda não
terminou"*.

Na dúvida (banco fora do ar, pergunta não respondida) ela assume que **não**
falta nada: assim o caminho normal segue e quem decide é a Receita, não um
palpite nosso sobre o estado do banco.

#### Como conferir que a 017 está mesmo aplicada

Na tela de Configurações, na lista de atualizações **aplicadas**, tem de
aparecer `017_nota_ciencia_e_arquivo.sql`. Estar ausente da lista de
*pendentes* não basta — pode ser a versão velha ainda no ar.

---

### Septuagésima segunda leva (18/09) — o PDF saía com outro filtro, e ninguém tinha como desconfiar

> *"Eu coloco aplicar para poder baixar o PDF, mas não baixa com as informações
> que estão aparecendo na tela. Está aparecendo outras informações."*

#### A causa, e ela é de uma linha

Os links de exportar eram montados assim: `url_for(rota, **args)`.

`args` é o `request.args` do Flask — um **MultiDict**, que guarda vários
valores por chave. **Desempacotar com `**` pega só o PRIMEIRO valor de cada
uma.**

Medido:

    marcado na tela  →  ['OBRA-1', 'OBRA-2', 'OBRA-3']
    no link do PDF   →  {'centro_custo': 'OBRA-1'}

A tela filtrava por três obras; o arquivo saía com **uma**. Com um valor só por
filtro — o caso mais comum — funcionava perfeitamente, e foi por isso que
passou tanto tempo sem aparecer.

#### ⚠️ O que torna este defeito pior do que parece: o silêncio

O arquivo **baixa normalmente**. Sem erro, sem aviso, com cara de relatório
certo. Quem recebe um PDF de prestação de contas não tem como desconfiar que
faltam duas obras — a não ser somando na mão contra a tela, que é justamente o
trabalho que o relatório existe para evitar.

É a mesma família do que o `CLAUDE.md` nomeia: **número errado com cara de
certo é pior que resposta nenhuma**.

#### Eram QUATRO lugares, não um

O mesmo `**args` estava em quatro links, e todos saíam errados do mesmo jeito:

| Onde | O que saía capado |
|---|---|
| Relatório | **Baixar PDF** |
| Relatório | **Exportar CSV** |
| Solicitações / Lote / Doc. Fiscal | **Exportar CSV** (a barra de ações) |
| Auditoria | **Exportar esta checagem** |

Só a tela de Documentação Fiscal escapava, porque ela já usava
`args.to_dict(flat=false)` — escrito quando o "28 vira 2" foi corrigido, em
13/09. A forma certa existia no repositório e não tinha sido levada aos outros.

#### O que NÃO estava errado

O **"Quebrar por"** da tela não chega ao PDF, e isso é de propósito: o PDF traz
**todas** as quebras (obra, projeto, tipo de despesa, conta). Ele é mais
completo que a tela, não diferente.

#### Os testes

Quatro casos, e eles foram conferidos **desfazendo o conserto**: com `**args` de
volta, dois falham com a mensagem exata do problema (*"o PDF sairia sem
OBRA-2"*). Os outros dois travam o caso simples (um valor só) e o caso sem
filtro nenhum, para o conserto não quebrar o que funcionava.

---

### Septuagésima terceira leva (18/09) — o analítico no PDF do relatório

> *"Queria que no relatório em PDF saísse mais abaixo o analítico. Está bom do
> jeito que você está colocando, mas está faltando a parte analítica: o
> lançamento, credor e a descrição com detalhe do que é. Pode reduzir a fonte
> para poder caber as coisas."*

O PDF tinha os totais, as quebras por obra/projeto/tipo/conta, os maiores
credores e o atraso — tudo **resumo**. Faltava o que está por trás: cada
lançamento, um por linha.

Agora, **ao final do relatório**, uma tabela com **SP · Data · Credor · Obra ·
Tipo · Descrição · Valor**, em fonte 6,5 e com a descrição quebrando em até
três linhas — foi ele quem autorizou reduzir a fonte. É o mesmo caminho do PDF
do lote, que já fazia isso desde 11/09.

#### Por último, e não no começo

Quem abre o relatório quer primeiro o resumo: os totais e as quebras respondem
*"quanto"* e *"onde"*. O analítico responde *"quais"*, e é para onde se vai
quando um número do topo surpreende. Pondo-o antes, seriam dezenas de páginas
de linhas antes do primeiro total. Há teste exigindo essa ordem.

#### ⚠️ A parte que mais importa: o detalhe FECHA com o total

O analítico usa **exatamente** o mesmo recorte dos totais — as mesmas funções
de filtro e de período que a contagem e as quebras usam.

Isto não é zelo. Detalhe e resumo saem **na mesma folha**: se filtrassem por
critérios diferentes, a soma das linhas não bateria com o número do topo, e
quem conferisse não teria como saber qual dos dois está certo. O relatório
inteiro perderia a credibilidade por causa justamente da parte que deveria
prová-lo.

Dois testes com banco de verdade cravam isso: a soma do analítico bate com o
total **no centavo**, e filtrando por obra o detalhe encolhe junto.

#### ⚠️ O teto, e por que ele AVISA

São **2.000 linhas**, da maior despesa para a menor. A base tem 59 mil SPs; um
relatório sem filtro viraria um PDF de centenas de páginas que ninguém abre e
que come a memória do serviço ao ser montado.

**Quando o teto corta, a folha diz isso com todas as letras** — e explica a
consequência: *"a soma destas linhas fica abaixo do total do topo, que continua
certo"*. Analítico truncado em silêncio seria pior do que analítico nenhum:
quem somasse as linhas não encontraria o total e concluiria que **a conta está
errada**, quando o certo é o total.

Quando não corta, a folha afirma o contrário — *"a soma desta lista fecha com o
total do topo"* — o que poupa a conferência de quem recebe.

A ordem por valor existe por causa do teto: cortando em 2.000, o que fica de
fora é o miúdo, não a despesa que interessa.

#### O que ficou de fora

- **O CSV do relatório não ganhou o analítico.** Ele já exporta os blocos de
  resumo, e quem quer lançamento a lançamento em planilha tem o "Exportar CSV"
  das Solicitações, que sai com o filtro inteiro (corrigido nesta mesma data).
  Acrescentar um sexto bloco lá tornaria o arquivo difícil de abrir no Excel.
- **A coluna "Data" muda de significado conforme o relatório**: em "pagas" é a
  data do pagamento; nos outros, o vencimento. É a mesma data que o período
  recorta — mostrar vencimento num relatório de pagas faria a coluna não
  explicar por que aquela linha entrou.

#### Conferido gerando o PDF de verdade

Três páginas, 45 lançamentos com descrição longa e credor comprido: a tabela
cabe na largura da folha (as sete colunas somam os 190 mm exatos), o cabeçalho
se repete na virada de página, a descrição quebra em vez de ser cortada, e o
aviso do rodapé aparece.

---
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
