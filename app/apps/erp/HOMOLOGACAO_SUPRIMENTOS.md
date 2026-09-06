# Suprimentos — o que testar, na ordem

Roteiro para a primeira vez que o módulo for usado. O sistema já confere
sozinho, a cada envio ao GitHub, **quem entra em cada tela** — isso não precisa
ser testado à mão. O que sobrou aqui é o que só uma pessoa percebe: se a tela
faz sentido, se a IA acerta com documento de verdade e se o número bate.

> **Antes de tudo:** apertar **"Aplicar atualizações do banco"** em
> Configurações, como ADMIN. Sem isso as telas de Suprimentos mostram "o banco
> está desatualizado" — e, como esta entrega acrescenta colunas ao cadastro de
> fornecedores, telas antigas também vão reclamar até o botão ser apertado.

---

## 0. Simular sem digitar nada (10 minutos)

**Suprimentos › Cadastros › Importações › Dados de exemplo** → *Trazer dados
de exemplo*.

0.1. *Tem de acontecer:* entram seis categorias, treze insumos, cinco
   fornecedores, duas condições de pagamento e quatro solicitações fictícias.
   Se não houver obra cadastrada, as solicitações não entram e o aviso diz
   isso — obra é da empresa, o exemplo não inventa.
0.2. Percorra os blocos 2, 3 e 4 abaixo **com esses dados**, antes de trazer
   os seus. É a forma de ver o fluxo inteiro sem depender de nada.
0.3. Ao terminar, *Remover os dados de exemplo* (digite REMOVER).
   - *Tem de acontecer:* sai exatamente o que entrou. Se você já tiver
     lançado algo de verdade em cima de um insumo de exemplo, a remoção é
     **recusada inteira** e diz o motivo — nada sai pela metade.

---

## 1. Trazer o que já existe (15 minutos)

**Suprimentos › Cadastros.**

0. **Antes da carga**, em *Categorias, unidades e pagamento*, cadastre as
   categorias de insumo que a sua planilha usa. A importação **não cria
   categoria** de propósito: fornecedor sem categoria não recebe cotação
   nenhuma, e categoria inventada na carga é como a base começa a apodrecer.
1. Em *Importações*, na planilha *Cadastro de Insumos*, aba **Cadastrar**:
   Arquivo → Fazer download → CSV. Escolha "Insumos" e clique em **Ver
   prévia**.
   - *Tem de acontecer:* o relatório diz quantos entrariam e quantos ficariam
     sem conta do plano financeiro. **Nada é gravado.**
   - *Confira:* o número de insumos bate com o que você espera (~115).
2. Clique em **Trazer para o ERP**. Depois clique em **Ver prévia** de novo com
   o mesmo arquivo.
   - *Tem de acontecer:* na segunda vez, "criados: 0". Rodar duas vezes não
     pode duplicar.
3. Repita com *Registro de Fornecedores*, aba **Registro** (~111 fornecedores).
   - *Confira:* se aparecerem "categorias que não existem no ERP", elas
     precisam ser cadastradas antes — o sistema não as inventa de propósito.

**O que pode dar errado e é normal:** cabeçalho com nome diferente do
esperado. O importador aceita variações, mas se recusar tudo, me diga qual
coluna é.

---

## 2. Um pedido de material do começo ao fim (30 minutos)

**Suprimentos › Solicitações** → *Novo pedido de material*.

4. Título: "teste — armadura da fundação". Prioridade: alta. Acrescente dois
   itens, **de obras diferentes**.
   - *Tem de acontecer:* ao escolher o insumo, a unidade vem preenchida do
     cadastro.
5. Clique em **Colar lista da planilha** e cole três ou quatro linhas de uma
   planilha sua de verdade (material, quantidade, unidade).
   - *Tem de acontecer:* a IA monta as linhas. O que ela **não** reconhecer
     aparece com fundo destacado e o texto original ao lado — ela não escolhe
     um insumo parecido por conta própria.
   - *Este é o teste que mais importa:* veja se ela acerta com a **sua**
     planilha, não com um exemplo.
6. Registre o pedido e confira que ele aparece na lista, com a obra de cada
   item e a situação "Solicitação".

---

## 2.5 A tela de gestão de cadastro (10 minutos)

**Suprimentos › Cadastros › Insumos.**

- Filtre pela esquerda por categoria e por unidade; confira que os números ao
  lado de cada opção batem com o que aparece na tabela.
- Clique em **Sem conta do plano** (o número em vermelho no topo).
  - *Tem de acontecer:* a tabela mostra só os insumos sem conta. São esses que
    não viram previsão de pagamento — vale corrigir agora.
- Clique na célula da unidade de um insumo e troque.
  - *Tem de acontecer:* a lista abre com busca; ao escolher, salva sozinho e a
    categoria e a conta **continuam as mesmas**.
- Clique na célula da conta do plano.
  - *Tem de acontecer:* só aparecem contas de despesa e material, agrupadas.
    Nenhuma conta de receita. Digite "cimento" e veja a lista encolher.

**Suprimentos › Cadastros › Fornecedores.**

- Olhe o número **Sem categoria**. Cada um desses é um fornecedor que não vai
  receber a próxima cotação. Abra um e marque o que ele vende.

---

## 3. Cotar e comparar (30 minutos)

**O caminho normal — a partir das Solicitações.** Em *Suprimentos ›
Solicitações*, filtre pela esquerda o que precisa comprar (por exemplo:
"Ainda não cotados" + a obra), marque os itens na primeira coluna e clique em
**Gerar cotação com os selecionados**.

- *Tem de acontecer:* o modal já vem com os fornecedores que vendem aquelas
  categorias marcados, e com o selo "vende isto". Você pode marcar outros.
- *Tem de acontecer:* ao confirmar, a tela vai direto para o mapa da cotação
  recém-criada.
- *Se nenhum fornecedor vier sugerido:* é o cadastro que está incompleto —
  volte em Cadastros › Fornecedores e marque o que cada um vende.

**O caminho alternativo** — *Suprimentos › Cotações* → *Nova cotação*, marque
os itens.

7. Acrescente dois fornecedores, um com frete e outro sem.
8. Digite os preços e clique em **Salvar preços**.
   - *Tem de acontecer:* a célula do menor preço de cada linha fica destacada;
     o rodapé mostra o total **já com frete e desconto**.
   - *Confira a conta:* o "melhor no total" pode não ser quem tem o menor preço
     unitário — é justamente esse o ponto.
9. Clique em **ler proposta** no cabeçalho de um fornecedor e cole o texto de
   uma proposta de verdade (ou o corpo de um e-mail recebido).
   - *Tem de acontecer:* os campos se preenchem com contorno verde (casou
     exato) ou âmbar (confira). O que não casou aparece escrito no aviso.
   - *Confira:* se ela colocou algum preço na linha errada, **me avise** — é o
     erro mais caro possível aqui.
10. Clique em **Puxar preços anteriores** (só faz efeito depois que houver
    histórico).
    - *Tem de acontecer:* a célula fica marcada como "herdado".

---

## 4. Fechar, autorizar e receber (30 minutos)

11. No mapa, feche o pedido com um fornecedor. **Suprimentos › Pedidos**.
    - *Tem de acontecer:* o pedido nasce "aguardando autorização". O comprador
      não compra sozinho.
12. Abra o pedido.
    - *Confira:* o mapa aparece embaixo, com as alternativas que existiam.
    - Clique em **Ver o pedido como o fornecedor vai receber**: os itens têm de
      estar separados por **endereço de entrega**.
13. Marque um item como recusado e clique em **Autorizar**.
    - *Tem de acontecer:* o item recusado volta para a fila de solicitações, e
      a previsão de pagamento nasce só sobre o que ficou — com as parcelas da
      condição de pagamento escolhida.
    - *Confira as datas e os valores das parcelas.* É este número que vai virar
      dinheiro saindo.
14. Registre um recebimento **parcial** (metade da quantidade).
    - *Tem de acontecer:* o item fica em "Pendência" com o saldo certo.
15. Registre o resto.
    - *Tem de acontecer:* o item fecha em "Recebido".

---

## 5. O banco de preços (10 minutos)

**Suprimentos › Banco de preços.**

16. Escolha o insumo que você acabou de comprar.
    - *Tem de acontecer:* aparecem o preço cotado e o **comprado**, com o
      resumo (último, menor, maior, média).
    - *Confira:* o preço que você autorizou está lá como "comprado".

---

## O que NÃO dá para testar ainda

- **Disparar a cotação por e-mail.** Não existe: falta decidir por qual conta
  da empresa o e-mail sai. Por enquanto, copie o texto da tela e envie você.
- **A previsão virando título no financeiro.** O caminho passa pelas regras
  fiscais do ERP e ainda não foi ligado.
- **Aviso automático** de atraso ou de nota não lançada: aparece na tela do
  pedido, mas ninguém recebe mensagem.

---

## Para recomeçar do zero

Testar suja o sistema, e desfazer um a um seria pior que o teste. Em
**Configurações › Zerar movimento do ERP** há o botão que limpa — e ele cobre
o ERP inteiro, não só Suprimentos: financeiro, extratos e conciliação,
despesas com colaborador, medições, locações, anexos, consumo de IA e a trilha
de auditoria. Você marca as áreas.

- **Cadastro nunca sai por aqui.** Obras, fornecedores, plano de contas,
  colaboradores, contas bancárias, operadores, insumos, unidades e condições de
  pagamento ficam sempre. A carga das planilhas não se perde.
- **Três etapas:** marcar as áreas, ver a contagem do que sairia, e só então
  digitar `ZERAR`.
- **Ele recusa em vez de apagar em cascata.** Se você marcar só o financeiro e
  houver um pedido de compra apontando para um título, ele para e diz "falta
  marcar suprimentos" — nunca leva junto uma área que você não escolheu.
- Fica registrado quem apertou, quando e quantas linhas saíram de cada tabela.

Rode o roteiro à vontade: zerar e recomeçar custa três cliques.

## Se algo quebrar

Anote **em qual passo** e o que apareceu na tela. Erro com o texto "o banco
está desatualizado" quer dizer que o botão de Configurações não foi apertado —
é o primeiro a conferir.
