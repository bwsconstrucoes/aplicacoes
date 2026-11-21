# HISTÓRICO — ERP · Suprimentos

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

- **procurar o passado** → `HISTORICO.md`, buscando por "suprimento", "cotação", "fornecedor", "insumo", "pedido de compra", "nota"
- **escrever o novo** → aqui

**Leitura desta área, além do `HISTORICO.md`:** `SUPRIMENTOS.md`,
`HOMOLOGACAO_SUPRIMENTOS.md`, `SUPRIMENTOS_ditado.md`, `ENTRADAS.md` e
**`CADEIA_DE_COMPRAS.md`** (o caminho inteiro, passo a passo, com o que está
pronto e o que falta — comece por ele).

---

## Onde o trabalho está

Insumos, fornecedores, cotações (o mapa de preços), pedidos de compra,
notas de entrada e o cruzamento nota × pedido.

⚠️ **CONSTRUÍDO E NUNCA OPERADO** — ver `SUPRIMENTOS.md`. É a área com
mais código sem uso real do repositório, e isso muda o cuidado: o que
está escrito não foi conferido por ninguém usando.

---

## ⚑ PENDENTE AGORA — leia isto antes de qualquer coisa

**Ramo `claude/admiring-brahmagupta-ac2tfg` — NÃO PUBLICADO em 20/11/2026.**
Traz a **migração 120** (apertar "Aplicar atualizações do banco" junto com a
publicação) e mexe em arquivos compartilhados: `routes.py`,
`core/auth/permissoes.py`, `core/auth/secoes.py`, `db/models/cadastros.py`,
`core/manutencao/limpeza.py`.

**Traz também a migração 121** (`pedido_titulos`) — as duas, 120 e 121, no
mesmo aperto do botão. E, desde a 3ª rodada, mexe em arquivos de OUTRAS áreas:
`templates/erp_lancar.html` e a rota `api_criar_titulo` (Financeiro),
`core/portal.py` (portal), `static/erp.css`.

**São TRÊS migrações no mesmo aperto do botão: 120, 121 e 122.**

**Aprovado por ele e AINDA NÃO FEITO**, em ordem sugerida:

1. **Aviso de preço fora da curva** no mapa (o relatório "preço contra a compra
   anterior" já existe; falta o aviso NA HORA de digitar). Cuidado: o preço do
   mapa entra no banco de preços como COTADO — a referência é o último
   COMPRADO de ANTES desta cotação.
2. **Melhor fornecedor por categoria** e **correção pela inflação (INCC)**.
3. **Ler a caixa de e-mail sozinho** — ele: *"depois a gente vai colocar a IA
   para fazer a leitura da caixa de e-mail"*. É "depois", dito por ele.
4. ~~Gasto da obra contra o previsto~~ — **ESCLARECIDO em 21/11 (noite):** o
   "previsto" que ele quer é a PROJEÇÃO das compras em andamento, não um
   orçamento de obra. Já existe (Projeção de pagamentos) e ganhou o 4º grau.
5. **Avisar o coordenador por Telegram** (chegou incompleto, entrega atrasada,
   compra por fora esperando autorização). Hoje tudo aparece em tela.
6. **O prazo típico por tipo de insumo** no Painel — o relatório de ciclo já
   mede; falta o Painel usar no lugar da régua fixa de 7 dias.
7. ~~Lançar junto também pela tela do ERP~~ — **FEITO em 21/11 (noite):** as
   telas do ERP não recebem mais sozinhas; mandam para o procedimento único.
8. `exportarMapa` em `erp_suprimentos_cotacoes.html` ficou sem uso — pode sair.

**Respondido por ele em 21/11 (noite):** o histórico de preços da planilha
**NÃO foi importado ainda**, e **ninguém usa o módulo de verdade ainda**. A
saída "a nota não veio" **FICA** ("vamos manter, é um recurso"). Compra por
fora: **começa só avisando**; travar é depois, decisão dele.

**Consequência do histórico não importado:** a estimativa "solicitado, sem
cotação" da Projeção e o relatório de preço contra a compra anterior só
enxergam o que for comprado PELO ERP. Importar o histórico de preços da
planilha é o que dá valor a essas duas telas desde o primeiro dia — ainda não
foi pedido; oferecer.

**Decidido por ele (não perguntar de novo):** parceiro e consulta PODEM ver
Painel, Acompanhar e Projeção; pedido e cotação vão em PDF com corpo de e-mail
padrão; WhatsApp para quem tem WhatsApp como canal; recebimento e lançamento
são UM procedimento, no portal, que serve no celular e no computador; a
assinatura fica com a pessoa, desenhada na tela ou escaneada.

---

## Decisões desta área

### ⚑ 21/11/2026 (4ª rodada) — NUNCA SÓ RECEBER, A SOLICITAÇÃO TARDIA, E OS ACERTOS NOS PAPÉIS

Ele leu os PDFs e os e-mails de amostra e voltou com sete pontos. Todos feitos.

**1. Solicitação de cotação (PDF).** Saiu "QUEM ESTÁ PEDINDO" (virou "EMPRESA");
saiu a frase "os preços podem ser informados nas colunas em branco" **e as
colunas em branco junto** — ele: *"É um PDF que dá para lançar o valor? Porque
se não der, não adianta."* Saiu o apelido interno da cotação do cabeçalho, do
e-mail e do assunto (ele viu "Utilitários 1" e não soube o que era). Entrou a
**hora** da emissão (fuso de Fortaleza).

**2. Pedido de compra (PDF).** Entrou o **CNO da obra** — ao lado do nome da
obra em cada bloco de entrega e como instrução própria em "Para a nota fiscal".
Vem de `obras.cno`. Obra sem CNO no cadastro: o pedido sai sem, calado.

**3. E-mail com formatação.** Ele: *"quando o texto fica simples demais dá a
impressão de que não é nem uma coisa de verdade."* `envio.corpo_em_html`: faixa
com o nome da empresa, quadro com número/valor/prazo, texto, rodapé com razão
social e CNPJ. Sem imagem, de propósito (leitor de e-mail bloqueia). O texto
puro vai junto como alternativa. `core/comum/email.py` ganhou `corpo_html`
(arquivo compartilhado — só acrescenta um parâmetro opcional).

**4. "Nunca pode ser feito só o recebimento."** Três mudanças:
- As telas do ERP (Receber material, e o recebimento dentro de Pedidos) **não
  registram mais a chegada sozinhas**: mostram o que falta e mandam para o
  procedimento único (`/erp/portal/material?pedido=N`).
- A rota de recebimento sozinho **recusa quem pode lançar**. Fica só para quem
  recebe e não lança — e aí grava o evento `RECEBIDO_SEM_LANCAMENTO`.
- **A nota não entra solta**: ao lançar um título de fornecedor que tem
  material recebido e sem conta, o servidor responde 409 e a tela pergunta "de
  qual pedido é esta nota?". Ou escolhe, ou declara que não é de nenhum
  (evento `NOTA_DECLARADA_SEM_PEDIDO`). Era a pergunta dele: *"como é que a
  gente vai garantir que esse pedido seja vinculado depois?"*
- A saída "a nota não veio" continua, com motivo escrito — decisão dele.

**5. A solicitação tardia** (`fora_do_fluxo.regularizar`). Ele: *"é exigido o
lançamento da solicitação, mesmo que tardia (...) até para a gente ter o
registro do que está sendo comprado, o quantitativo de material que está sendo
utilizado em obra."* No Painel, cada compra por fora ganhou "Registrar o que
foi comprado": a pessoa informa material, quantidade e preço, e nascem na mesma
transação a solicitação, o pedido (já autorizado), o recebimento integral e o
vínculo com o lançamento. Não cria conta — ela já existe.
- **A linha do Painel tem DUAS pendências independentes** — a ciência (de quem
  tem alçada) e o registro do material — e só sai quando as duas fecham.
  ESCOLHA MINHA, dita a ele: uma não espera a outra, para o pagamento não ficar
  refém de digitação. Se ele quiser que a ciência só possa ser dada depois do
  registro, é uma linha em `autorizar`.
- Soma dos itens diferente do valor da nota **não bloqueia** (frete, desconto);
  a diferença fica no evento.
- Colunas novas (`pedido_id`, `regularizado_por`, `regularizado_em`) entraram
  **dentro da migração 122**, que ainda não foi aplicada em produção.
- O material tem de estar no cadastro de insumos. Se não está, a pessoa pede o
  cadastro ao suprimento — não há cadastro de insumo por aqui.

**6. A previsão financeira.** Ele descreveu o que queria e era a Projeção de
pagamentos, que já existia. Entrou o **4º grau**: "solicitado, sem cotação",
estimado pelo último preço conhecido do insumo, na data em que a obra precisa.
Item sem preço anterior continua de fora, contado no rodapé.

**Não verificado:** e-mail formatado aberto num Outlook/Gmail de verdade (vi só
no navegador); o envio real por SMTP com as duas versões.

### ⚑ 21/11/2026 (3ª rodada) — COMPRA POR FORA: PRIMEIRO SÓ AVISA, DEPOIS TRAVA

Decisão dele: *"O que não seguir o processo poderemos por enquanto avisar, sem
travar. Depois a gente trava, quando tiver tudo rodando já."*

A regra do material lançado sem pedido de compra passou a ter **três posições**,
escolhidas no Painel de suprimentos por quem pode configurar:

| Posição | O que acontece no lançamento |
|---|---|
| não acompanhar (como nasce) | nada |
| **só avisar** — a que ele escolheu para começar | o lançamento segue normal no financeiro, sem pedir motivo; fica listado no Painel até alguém dar "Ciente" |
| travar até autorizar | pede o motivo e bloqueia até alguém com alçada autorizar (como era) |

- A chave (`suprimentos_exigir_pedido_desde`) guarda `MODO:data`. Valor antigo,
  só com a data, é lido como TRAVAR.
- **Continua desligada até alguém clicar** "só avisar aqui" no Painel — e isso
  exige a migração 122 aplicada. Lembrar o dono na publicação.
- Passar para TRAVAR é decisão dele, "quando tiver tudo rodando". Não ligar por
  conta própria.
- Na mesma conversa ele perguntou se precisava ESCOLHER entre os relatórios:
  não. Os oito existem juntos (quatro na aba Relatórios, quatro dentro do mapa).

### ⚑ 21/11/2026 (2ª rodada) — RECEBER E LANÇAR JUNTOS, E-MAIL PADRÃO COM PDF, E MAIS CINCO

Ele respondeu à entrega anterior ponto a ponto. O que cada resposta virou:

**1. "Fazer isso tudo no mesmo procedimento, receber e lançar notas e boletos."**
E a razão, que é a regra: *"inclusive eu travo isso atualmente, a pessoa recebe
e lança ao mesmo tempo, porque senão as pessoas podem ignorar, podem fazer só o
lançamento financeiro, esquecer de fazer o recebimento e vice-versa."* E sobre o
preenchimento: *"se a IA detectar, preenche tudo. Se não detectar, a pessoa tem
que concluir o básico: número de nota, fornecedor, CNPJ, vencimento, código de
barras, Pix."*

⚠️ **Eu tinha feito o contrário:** a obra recebia no portal e era mandada para
a tela de Lançar do ERP. Ele: *"não, eu quero que seja lançada a partir do
portal (…) uma tela enxuta. Mas nada impede de estar utilizando o portal no
computador também."*

Como ficou (`lancamento.receber_e_lancar`, rota `/receber-e-lancar`):
- UMA transação — recebimento, título e vínculo nascem juntos ou nenhum;
- credor, rateio por obra e conta do plano VÊM DO PEDIDO e nem aparecem na
  tela; o que a tela pede é o que só a nota e o boleto sabem;
- o valor vem sugerido pelo que foi marcado como recebido (preço do pedido ×
  quantidade, mais frete e menos desconto quando a entrega fecha o pedido);
- a nota e o boleto são lidos pela IA assim que juntados (a MESMA rota de
  leitura da tela de Lançar) e os campos lidos ficam contornados de verde;
- travas: número da nota, valor, vencimento, linha digitável no boleto,
  parcelas somando o valor, e a nota anexada;
- foto do material: botão próprio, abre a câmera, não é lida pela IA.

⚑ **A saída "a nota não veio" — decisão MINHA, e ele pode mandar tirar.** Ele
disse que trava. Mantive UMA saída: marcar "a nota não veio com o material",
com motivo obrigatório, registra só o recebimento e deixa o pedido em "Falta
lançar", à vista do coordenador e do financeiro. Sem ela, no dia em que o
caminhão chega sem o boleto, a obra não registraria nem o recebimento — e o
sistema perderia as duas informações em vez de uma.

⚑ **O título de PIX/TED sem conta conferida nasce BLOQUEADO** (crítica C2 do
financeiro, que já existia) — a tela avisa antes. É o comportamento certo.

⚑ **`receber_e_lancar` repete a ORQUESTRAÇÃO de `api_criar_titulo`** (checar
duplicidade → derivar o tipo → `criar_titulo` → vincular). As REGRAS são as
mesmas funções; a ordem é que está escrita duas vezes. Se a tela de Lançar
ganhar um passo novo na criação, este lugar precisa ganhar também.

**2. O e-mail.** *"O e-mail você colocou em TXT, mas ele tem que ser em PDF
mesmo. Tem que ter um corpo de e-mail padrão, avisando o fornecedor. E no
anexo, o relatório completo."* Cotação e pedido saem com um aviso curto e
sempre igual, e o documento em PDF anexo (`cotacao_pdf.py` é novo: os itens por
endereço, com colunas em branco de preço e prazo). O REGISTRO do envio guarda
o conteúdo por extenso (`corpo_registrado`, parâmetro novo em
`core/comum/email.py`) — "segue em anexo" não prova o que foi mandado. Se o
PDF falhar, sai o texto completo no corpo, como era.

**3. "Pedido sem cotação, isso é essencial."** Botão "Comprar direto (sem
cotação)" na tela de Solicitações. Exige fornecedor, preço de cada item,
condição, data e o MOTIVO de não cotar; mostra o último preço conhecido de
cada item; vai para a mesma fila de autorização, marcado "sem mapa".

**4. "Tem que ter a tela de pedido de material também no portal."** Aba
"Pedir" em Material da obra: título, obra, data, prioridade, materiais por
busca no catálogo, especificação. Mesma rota da tela de Solicitações. Não
achou o material → pede o cadastro dele ao suprimento.

**5. A assinatura.** *"Fica mais fácil de desenhar com o dedo."* Página
`/erp/assinatura` (quadro de desenhar + mandar imagem escaneada), guardada
como anexo DA PESSOA (`core/comum/assinatura.py`) — não é coluna em
`usuarios`, de propósito. Sai no PDF do pedido e da cotação de quem manda.
⚠️ "Zerar movimento › anexos" apagaria as assinaturas junto.

**6. "Os relatórios todos, sugeridos todos."** Aba **Relatórios**
(`relatorios.py`): economia da cotação, preço contra a compra anterior,
desempenho do fornecedor, tempo do ciclo. Uma tela desenha qualquer um.
Ficou de fora "gasto contra o previsto" — não há orçamento para comparar.
Ele também notou que os quatro do mapa eram "tal qual o que eu mandei": são
mesmo; o que acrescentei foi o que olha várias compras de uma vez.

**7. A compra fora do fluxo.** Ele esclareceu: *"alguém autorizar esse
lançamento para o financeiro de um material comprado que não seguiu o fluxo do
suprimento."* Feito (`fora_do_fluxo.py`, migração 122, crítica **S1** em
`core/titulos/analise.py`): lançamento de material (tipo T1) sem pedido exige
o motivo, nasce BLOQUEADO, aparece no Painel, e só anda quando alguém com
`autorizar_compra_fora_do_fluxo` — e que NÃO seja quem lançou — autoriza.
**NASCE DESLIGADA.** A chave fica no Painel (quem tem `configurar`). Ligar
antes de as compras passarem pelo ERP reteria todo lançamento de material.

**Conferido:** 83 testes em `tests/test_suprimentos_ciclo_banco.py`; no
navegador, em tamanho de celular: receber e lançar pelo portal (com a trava
dizendo tudo o que falta de uma vez), pedir material, desenhar a assinatura;
no computador: compra direta, os quatro relatórios, a caixa do Painel.
**Não conferido:** a leitura da nota pela IA de verdade (sem chave aqui — a
tela foi testada com a leitura falhando, e ela pede os campos à mão); boleto
com linha digitável real; envio real de e-mail e de WhatsApp.

### ⚑ 21/11/2026 — O PORTAL EM LISTA, OS DOCUMENTOS NO RECEBIMENTO, E A COMPRA POR FORA

Três coisas que ele viu na imagem do portal:

**1. Lista que abre.** *"Tem três pedidos a serem recebidos — o ideal é a lista
e você clicar e abrir, porque pode ser um pedido grande com muitos insumos."*
Eu tinha entregue os três pedidos abertos, um embaixo do outro. Agora cada
pedido é uma linha (fornecedor, número, quantos itens faltam, previsão ou
atraso) e abre ao tocar — um por vez; o que foi digitado fica guardado ao
trocar de pedido.

**2. Anexar nota, boleto e mais.** *"Eu não vi nenhum campo de anexar nota
fiscal, boleto. A gente tem que prever já isso. E da mesma forma que no DP, a
gente precisa poder anexar vários documentos."* Os arquivos ficam presos AO
RECEBIMENTO (`anexos`, entidade `recebimento`), com tipo (nota, boleto, foto,
outro — palpite pelo nome, corrigível). Sobem depois do recebimento gravado;
falha em anexar não desfaz o recebimento. **A tela de Lançar os traz
sozinha** quando abre por aquele recebimento: ninguém escolhe o mesmo arquivo
duas vezes. Rotas próprias, com o recorte pela obra do pedido — a rota
genérica de anexos não conhece "recebimento" e não foi mexida.
⚠️ Só o PORTAL anexa. A tela Suprimentos › Receber material (ERP) ainda não
tem o campo.

**3. A compra que não passou pelo suprimento.** *"A gente tem que tentar
inibir a possibilidade de uma compra sem ter seguido o processo do suprimento.
Imagina um engenheiro da obra que foi lá e comprou um material (…) quando
fosse anexar uma nota e identificar que não identificou um pedido — por que
estão lançando isso? Aí tem que pensar na forma de dar ciência e alguém
autorizar."*

**Feita a metade que IDENTIFICA**, e ela não bloqueia nada: na tela de Lançar,
escolhido o credor, o sistema procura os pedidos de compra dele com conta a
lançar e pergunta "é deste pedido?" (`lancamento.pedidos_do_fornecedor_a_lancar`).
Um clique liga o lançamento ao pedido, mesmo depois de a nota já ter sido lida.

**NÃO FEITA a metade que dá ciência e exige autorização — de propósito, e a
razão é o dia de hoje:** nenhuma compra da empresa passa pelo ERP ainda. Ligar
essa trava agora marcaria 100% dos lançamentos de material como "compra por
fora" e pararia o financeiro. Proposta entregue a ele em 21/11, esperando três
respostas: (a) quem autoriza; (b) trava o pagamento ou só avisa; (c) a partir
de quando, e para quais obras. Desenho proposto: lançamento de MATERIAL sem
pedido exige o motivo escrito, nasce marcado "compra fora do processo", avisa
o coordenador de suprimentos e — se ele quiser a trava — fica bloqueado até a
ciência de quem autoriza, pela crítica do motor de análise do Financeiro
(`core/titulos/analise.py`), que é onde título bloqueado já mora. Com chave de
ligar em Configurações e data de início.

**Lição desta rodada (minha):** reescrevi a tela do portal recortando o
arquivo por marcadores de texto, um marcador casou no lugar errado e o código
ficou fora de ordem — a suíte passou, porque a página do portal não está no
teste de sintaxe das telas. Só o navegador pegou. Tela reescrita se grava
INTEIRA, e se abre no navegador antes de dizer que está pronta.

### ⚑ 20/11/2026 (3ª rodada) — O FINANCEIRO NASCE DO RECEBIMENTO, O PORTAL RECEBE, O MAPA VIRA PLANILHA

Ele leu a primeira entrega e corrigiu o rumo em sete pontos. Os três que mudam
o desenho:

**1. "EU QUERO GERAR O FINANCEIRO A PARTIR DO RECEBIMENTO."** Eu havia escrito
como "ponto fraco" que o pagamento só era visto se a nota fosse vinculada ao
pedido na tela de Notas. Resposta dele: *"na verdade, eu quero gerar o
financeiro a partir do recebimento, como eu já falei previamente, que
atualmente é assim que funciona. A pessoa vai receber o pedido e lança nota,
lança boleto, lança tudo e gera a obrigação financeira. Ou confirma uma
previsão que já existe."*

Como ficou (`core/suprimentos/lancamento.py`, migração 121):

- depois de receber, a obra clica em "Lançar a nota e o boleto" e cai na tela
  de **Lançar** com `?pedido_compra=N&recebimento=M`. A tela abre com credor,
  valor DO QUE CHEGOU (não do pedido inteiro), rateio por obra com a conta do
  plano de cada insumo, e as parcelas da previsão;
- o título nasce em `titulos.service.criar_titulo`, pelo caminho de sempre —
  **este módulo não cria título**. Depois de criado, `vincular` grava
  `pedido_titulos` e marca as previsões cobertas (`titulo_id`, que existia e
  ninguém preenchia). É a "previsão que já existe" sendo confirmada;
- o painel e a projeção passaram a enxergar esse título direto. Lançado assim,
  conta também como "tem nota" — a nota é jogada na própria tela de Lançar.

⚠️ **Decisão minha, dita a ele:** não fiz uma tela de lançamento própria do
suprimento. Duas telas de lançar divergiriam nas críticas fiscais e de
duplicidade, e `SUPRIMENTOS.md` §8.2 já dizia para não contornar o financeiro
por dentro. O custo: a obra sai do portal (celular) e cai na tela de Lançar do
ERP, que é de computador. Se isso incomodar, o caminho é uma tela enxuta de
lançar que chame as MESMAS rotas — não uma regra nova.

**2. "O RECEBIMENTO VAI SER PRINCIPALMENTE PELO PORTAL."** *"O pessoal
administrativo de obra tem uma visão mais limitada do sistema. Semelhante ao
que a gente faz com o DP."* A área "Material da obra" do portal abriu
(`/erp/portal/material`, `erp_portal_material.html`), com três abas: para
receber, em andamento (o que ainda está em cotação ou autorização — só
acompanhar) e falta a nota. Usa as MESMAS rotas de Suprimentos › Receber
material: duas portas, uma regra. A frase da área no portal NÃO promete "pedir
material", porque isso ainda não existe lá.

**3. O MAPA.** Três queixas e um pedido:

- *"o modal se torna pequeno (…) tem que ter um modal que expanda na tela"* —
  botão **Tela cheia** no mapa; os diálogos da tela (relatórios, fechar
  pedido) passaram a ocupar quase a tela inteira (`dialog.largo`);
- *"é uma planilha que a gente consegue visualizar bastante informação"* — o
  mapa foi refeito no formato do relatório que ele mandou (9228): condições de
  cada fornecedor em LINHAS, unitário e subtotal lado a lado, Nº SS e número
  do item, colunas da esquerda presas na rolagem, e no rodapé melhor
  fornecedor e melhor compra, sem e com frete;
- *"eu jogo o arquivo, aí o sistema faz a leitura e tenta preencher o mapa"* —
  já existia "ler proposta" por coluna; agora há uma área no mapa onde se joga
  o arquivo (vários de uma vez) e o sistema **descobre de quem é** pelo CNPJ
  (matriz e filial contam) ou pelo nome. Não reconheceu, PERGUNTA — não chuta;
- *"quero ver como fica o mapa para a gestão"* — o "Mapa em PDF" era a
  exportação genérica (só preço unitário). Virou `mapa_pdf.py`, no servidor:
  o mapa no formato da planilha + os quatro resumos + linha de aprovação.

**Defeito achado e consertado:** confirmar as condições lidas de uma proposta
recarregava o mapa e APAGAVA os preços que a IA tinha acabado de preencher e
ainda não estavam salvos (`recarregarMantendoDigitado`).

**Os outros quatro pontos:**

- **Pedido em PDF** (`pedido_pdf.py`): anexo ao e-mail; o texto continua no
  corpo. Um bloco por local de entrega com subtotal, parcelas com data e
  valor, instruções da nota. Falha ao gerar o PDF não segura o envio.
- **Cobrança por WhatsApp** (`cobranca.telefones_de_whatsapp`): só para
  fornecedor com WHATSAPP em `canais_cotacao`; número do contato escolhido no
  disparo, senão o do fornecedor. O e-mail continua saindo junto.
  ⚠️ Usa `notificar(canais=("whatsapp",))` SEM `finalidade` — não passa pela
  política da tela Mensagens. Se a Mensageria quiser governar isso, é criar a
  finalidade lá. ⚠️ E a cotação ainda exige a conta de e-mail da empresa
  configurada mesmo para quem só tem WhatsApp.
- **Lentidão**: `pedido.listar` caiu de 2 leituras da tabela inteira POR
  PEDIDO para 3 consultas no total; `montar_mapa`, envio, recebimento e
  proposta ganharam `.where`. O `if` em Python ficou ao lado, de propósito: a
  sessão dublada dos testes ignora `WHERE`. Sobram dezenas de leituras de
  tabela inteira em cadastro/planejamento/cobrança/desempenho — são de tabela
  pequena ou de tela pouco aberta; não foram tocadas.
- **"As propostas que chegaram" — ele perguntou se era o e-mail.** Não é:
  ninguém lê a caixa. A "fila única" que propus perdeu o sentido com o
  arquivo jogado no mapa, e saiu da lista.

**Conferido:** 55 testes em `tests/test_suprimentos_ciclo_banco.py`; o caminho
portal → receber → Lançar pré-preenchido percorrido num navegador em tamanho
de celular; o mapa em tela cheia e os dois PDFs abertos e lidos.
**Não conferido:** leitura de proposta de verdade pela IA (não há chave aqui —
o reconhecimento do fornecedor foi testado com a leitura simulada); envio real
de e-mail e de WhatsApp.

### ⚑ 20/11/2026 (2ª rodada) — O PAINEL DO COORDENADOR E O REGISTRO COMPLETO DOS PEDIDOS

Depois de ver a primeira entrega, ele pediu o que tinha "no método antigo":

> *"Uma tela onde eu listo todos os registros dos pedidos fechados (…) o
> status, se já foi entregue, se já tem lançamento financeiro, se já tem nota
> fiscal, se já foi pago, previsão de entrega — tudo isso pesquisável.
> Lembrando que a gente sempre está usando o sidebar de forma completa (…)
> busca por fornecedor, por tipo de insumo, por obra. Tem que ter um painel do
> coordenador, para ele bater o olho e compreender a coisa (…) o que é
> prioritário, o que não é, o que está atrasando, o que atrasou, o que deveria
> ter chegado e não chegou."*

**REGRA QUE FICA, e vale para toda tela de lista do ERP:** a barra lateral é
COMPLETA — um filtro para cada pergunta que a pessoa faz, com a contagem do
que sobraria em cada opção. Eu tinha entregue "Acompanhar compras" com dois
filtros e ele notou na hora.

**Não nasceu tela nova de pedidos: "Acompanhar compras" VIROU o registro.**
Ganhou filtros de entrega, nota fiscal, lançamento financeiro, pagamento, obra,
tipo de insumo, fornecedor e período da previsão; a busca acha o pedido por
qualquer material comprado; a linha aberta mostra todos os itens e leva ao
pedido e à cotação; e "incluir fechados" + "incluir recusados e cancelados"
fazem dela o registro de tudo.

**Tela nova, e é a primeira aba: Suprimentos › Painel**
(`acompanhamento.planejamento`). A linha é o ITEM que a obra pediu — é ele que
tem a data em que a obra precisa, e existe antes de haver cotação. Compara
três datas (precisa até × previsto pelo fornecedor × hoje) e classifica:
já deveria ter chegado · fornecedor estourou a data dele · vai chegar depois
do que a obra precisa · em risco · no prazo · sem data. **A lista vem
ordenada pelo pior prazo e, dentro dele, pela prioridade.** Os quadros de
baixo são as compras fechadas e levam ao registro já filtrado.

⚠️ **"Baseado em tempo de entrega" — ele perguntou se temos, e NÃO TEMOS.** A
tabela de prazos por insumo da planilha não foi migrada (decisão dele de
04/09). "Em risco" usa régua fixa: faltam 7 dias ou menos e não há pedido
autorizado (`FOLGA_DIAS`). O caminho certo, quando houver uso: medir o prazo
real de cada categoria pelos próprios pedidos (autorização → recebimento).

**Defeito achado de passagem e consertado:** o contador "Prazo estourado" da
tela de Solicitações contava material já recebido ou cancelado — olhava só a
data. Número vermelho que nunca zera ensina a ignorar o vermelho.

**Sobreposição que fica, dita:** a tela de Solicitações continua existindo e
é onde se TRABALHA o item (corrigir, selecionar para cotar); o Painel é onde
se ENXERGA a fila inteira até a chegada. As duas listam itens.

### ⚑ 20/11/2026 — A CADEIA REVISADA DE PONTA A PONTA: três quebras, duas visões

**O pedido dele**, depois de mandar o modelo do mapa (nº 9228) e o do pedido
(ID 19457) que a equipe usa hoje:

> *"O coordenador de suprimentos precisa monitorar e acompanhar o que ele
> compra. (…) precisa fechar todo o ciclo do pedido à entrega e poder
> acompanhar o pagamento. É necessária visão geral e cruzada das informações.
> Por outro lado o financeiro não pode ficar no escuro em relação ao
> planejamento do compras."*

E os dois problemas que ele frisou, porque existem hoje: **(1)** o ciclo só
fecha com a entrega INTEGRAL, e o que faltar tem de virar pendência na mão do
coordenador; **(2)** *"muitas vezes chega material e a obra atrasa o lançamento
das notas e boleto. E o financeiro fica cego."*

**O QUE A REVISÃO ACHOU — três quebras, as três do mesmo formato (regra escrita
e testada, sem porta na tela):**

| Quebra | Desde quando | O que era |
|---|---|---|
| **O mapa não tinha botão de fechar pedido** | 05/09/2026 | `fechar_do_mapa` e a rota existiam; nenhuma tela chamava. Dava para cotar, comparar e mandar e-mail — não dava para COMPRAR. O roteiro de homologação mandava fechar no mapa |
| **A obra não tinha onde receber** | 05/09/2026 | o recebimento morava dentro da tela de Pedidos (`ver_pedidos_compra`: só comprador e diretor). E o POST aceitava `ver_suprimentos`, sem recorte de obra: consulta e parceiro de fora registravam recebimento de qualquer obra |
| **O financeiro não via a previsão** | 05/09/2026 | `previsoes_pagamento` nasce na autorização e só aparecia DENTRO do pedido. Nenhuma tela do sistema a somava. `previsao.titulo_id` nunca é preenchido por ninguém |

⚠️ **LIÇÃO, e ela já está no `CLAUDE.md` pela etapa do DP:** "CONSTRUÍDO E
NUNCA OPERADO" escondia isto. A suíte prova a regra; **só percorrer a tela
prova a porta**. Ao revisar uma área, procurar no template quem CHAMA cada rota.

**O QUE FOI FEITO:**

1. **Fechar pedido no mapa** (`erp_suprimentos_cotacoes.html`,
   `pedido.preparar_fechamento`). Um diálogo: de quem, o quê (vêm marcados os
   de menor preço; o que já está em outro pedido aparece travado com o
   número), condição, data, frete, desconto, e onde desce. **Condição de
   pagamento e data de entrega são obrigatórias NA TELA** — sem elas a projeção
   do financeiro não tem como ser feita.
2. **Local de entrega** (migração 120, `pedido_entregas`, `entrega.py`). Regra
   dele: *"a princípio a entrega é na obra associada ao pedido"*; a exceção
   (sede, outro endereço) *"pode ser definida no fechamento"*. É **por obra
   dentro do pedido**. Decisões minhas: o endereço é gravado POR EXTENSO (o
   pedido é documento e não muda se o cadastro da sede mudar); a "sede" é a
   empresa da obra; dá para trocar depois enquanto NADA chegou; tabela própria
   e não coluna em `pedido_itens`, porque a tela de Notas do Financeiro lê essa
   tabela e quebraria até a migração ser aplicada.
3. **Receber material** (`erp_suprimentos_receber.html`,
   `recebimento.a_receber`). A porta da obra: só as obras dela, **sem preço**,
   campo já preenchido com o que falta. Ação nova `receber_material` (obra +
   quem compra). No topo, o lembrete "recebido e nota não lançada".
4. **Acompanhar compras** (`acompanhamento.py`). Uma linha por pedido: chegou?
   tem nota? foi pago? e **o próximo passo que falta, com de quem depende**.
   Nada é digitado ali — cruza recebimento × nota vinculada × título da nota.
   Ciclo FECHADO = entregue inteiro + nota + pago.
5. **Projeção de pagamentos** (`projecao.py`). O que ainda vai virar conta a
   pagar, por mês e por obra, em três graus (comprado / a autorizar / em
   cotação). **Desconta o que já tem título**, para não contar a dívida duas
   vezes. A parcela com data vencida e sem título sai em vermelho.
6. **O texto do pedido ao fornecedor** ganhou o que o modelo dele tinha e o
   nosso não: CNPJ para faturar, CNPJ do fornecedor, CEP, link do mapa
   (quando a obra tem coordenadas), nº da solicitação por item e as três
   instruções da nota fiscal.

**CUSTO, medido** (regra de 17/11): o painel e a projeção fazem **7 a 9
consultas para a tela inteira**, com dez ou dez mil pedidos. ⚠️ **O resto do
suprimento NÃO é assim, e não foi mexido:** `pedido.listar` lê a tabela
`pedido_itens` INTEIRA duas vezes POR PEDIDO da lista (`_itens` e `total`
filtram em Python), e `montar_mapa` lê todos os preços de todas as cotações. A
numeração antiga da empresa passa de 19 mil pedidos; com esse volume a tela de
Pedidos faria dezenas de milhares de leituras de tabela inteira por abertura.
A causa é o dublê de testes ignorar `WHERE` — o código foi escrito para passar
nele. **Relatado ao dono em 20/11; conserto esperando o "pode".**

**O QUE FICOU DE FORA, dito:** o pedido em PDF; pedido direto sem cotação;
anexo da nota no recebimento; aviso por Telegram; a ligação automática
previsão → título (hoje o caminho é pedido → nota vinculada → título com a
nota; título lançado SEM vincular a nota não é enxergado pelo painel).

**Conferido:** 23 testes novos com banco (`tests/test_suprimentos_ciclo_banco.py`),
as quatro telas percorridas no navegador com dois usuários (admin e
administrativo de obra), fechamento com entrega na sede feito pela tela.
Mapa completo em `CADEIA_DE_COMPRAS.md`.

---

## Incidentes

(vazio)
