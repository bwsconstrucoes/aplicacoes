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

- o endereço de produção, para entrar no lugar de `SEU-ENDERECO` no PDF de
  orientação e na mensagem de WhatsApp (os dois estão prontos);
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
