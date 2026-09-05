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

## 1. Trazer o que já existe (15 minutos)

**Suprimentos › Cadastros.**

1. Na planilha *Cadastro de Insumos*, aba **Cadastrar**: Arquivo → Fazer
   download → CSV. Escolha "Insumos" e clique em **Ver prévia**.
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

## 3. Cotar e comparar (30 minutos)

**Suprimentos › Cotações** → *Nova cotação*, marque os itens.

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

## Se algo quebrar

Anote **em qual passo** e o que apareceu na tela. Erro com o texto "o banco
está desatualizado" quer dizer que o botão de Configurações não foi apertado —
é o primeiro a conferir.
