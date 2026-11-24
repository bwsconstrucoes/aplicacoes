# HISTÓRICO — ERP · Obras e Arquivo

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

- **procurar o passado** → `HISTORICO.md`, buscando por "obra", "contrato", "diário", "arquivo", "documento", "agenda", "acompanhamento"
- **escrever o novo** → aqui

**Leitura desta área, além do `HISTORICO.md`:** `GESTAO_DOCUMENTOS.md`,
`ACOMPANHAMENTO.md`, `CRONOGRAMA.md`.

---

## Onde o trabalho está

Cadastro de obra (contrato, matrícula CNO, ART, OS), diário de obra,
o Arquivo (catálogo de documentos, validade, sigilo), agenda e
acompanhamento de processos.

---

## ⚑ PENDENTE AGORA — leia isto antes de qualquer coisa

**Falta publicar (19/11/2026):** o filtro de fase no diário (abaixo). Está no
ramo `claude/oi-vjvrn8`, testado, **sem migração**, esperando o "pode" dele.

**Dito a ele e ainda não decidido:**

- **Ele vai enxugar as fases.** Palavras dele, em 19/11: *"depois eu vou dar
  até uma enxugada nessas fases."* Quando isso vier, mexe em `FASES_OBRA` e em
  `FASES_ENCERRADAS`, em `core/cadastros/obras.py`, e na lista repetida dentro
  do `erp_diario.html` (`FASES_FORA`) — os três juntos.
- **A mesma régua vale para outras telas?** O filtro de fase hoje é só do
  diário. A caixinha de obra do **pedido ao DP** (`portal.py`,
  `obras_para_escolher`) e a tela de **Obras** continuam mostrando tudo. Para
  o pedido ao DP isso parece certo (desligar alguém de obra que acabou é caso
  real), mas é decisão dele.
- **Ineficiência relatada, não consertada:** o painel "onde está faltando
  diário" faz **uma consulta ao banco por obra**, mais uma — com 80 obras são
  81 idas. O quadro do mês, ao lado, já resolve a mesma conta em uma consulta
  só. Esconder as concluídas cortou o número, mas a forma continua errada.

O que estava pendente antes da divisão dos chats está em `HISTORICO.md`, na
seção "PENDENTE AGORA".

⚠️ **Perguntar ao dono** se o que está escrito lá já aconteceu — o estado do
mundo muda entre uma sessão e outra, e ele é a única fonte disso.

---

## Decisões desta área

### 19/11/2026 — O diário esconde obra em fase de conclusão (e não proíbe)

**O defeito, e ele é dos que passam despercebidos por meses:** o diário dizia,
na própria tela, que obra concluída não aparecia. **Era mentira pela metade.**
O filtro era só `status <> 'ENCERRADA'` e **a fase era ignorada** — e ninguém
encerra o cadastro de uma obra no mesmo dia em que ela acaba na prática. Então
obra marcada "Concluída", "Recebimento definitivo" ou "Distratada" continuava
no quadro do mês, acumulando dias vermelhos que ninguém ia preencher.

**O que ele pediu, com as palavras dele:**

> *"As fases iniciais da obra, que são as fases que tem sentido você lançar
> diário (…) criação/cadastro, aguardando a ordem de serviço, em execução, ou
> paralisada. Daí em diante, as demais fases são todas fases associadas à
> conclusão da obra. (…) Não tem sentido eu fazer diário de uma obra que está
> concluída, (…) concluída com dívida, (…) aguardando recebimento provisório
> ou recebimento definitivo, (…) acervo técnico, ou que foi destratado."*

**E a forma, que é a parte importante:**

> *"Eu não digo nem que proíba a confecção do diário, mas que pelo menos a
> exibição não fique na tela inicial. (…) A tela inicial exibe só as obras
> ativas. As obras concluídas ficam ocultas. Aí eu altero o filtro e visualizo
> todas, se eu quiser. Porque senão fica aquela lista imensa de obras que
> deveriam estar concluídas."*

**O que foi feito:** a tela do diário ganhou o filtro **"Quais obras"**, com
duas opções — *Só as obras em andamento* (o padrão) e *Todas, inclusive as
concluídas*. Ele vale ao mesmo tempo para o quadro do mês, para a caixinha de
escolher obra, para a busca por digitação e para o painel de pendências.

**ESCONDE, NÃO PROÍBE, e isso é decisão registrada:** lançar diário em obra
concluída continua permitido. Quem tem diário atrasado de obra que já acabou
troca o filtro e chega nela. Há teste cobrando isso, para ninguém "melhorar"
a regra depois e virar trava sem ele pedir.

**Onde a régua mora:** `FASES_ENCERRADAS`, em `core/cadastros/obras.py` — a
mesma lista que o assistente já usava. ⚠️ Ela está **repetida** dentro do
`erp_diario.html` (`FASES_FORA`), porque a lista de obras que a tela já tem em
mãos traz a fase de cada uma, e buscar a régua no servidor seria uma ida de
rede só para isso. Os dois nomes são parecidos de propósito: **fase nova muda
nos dois lugares.** Há um teste que trava a composição da lista, então
acrescentar uma fase ao sistema obriga alguém a decidir de que lado ela fica.

**O que ficou de fora, de propósito:** a tela de Obras, o Arquivo, a Agenda,
os Contratos e a caixinha de obra do pedido ao DP continuam mostrando tudo.
São listas em que obra concluída ainda faz sentido.

**Não mexeu em arquivo compartilhado além do `routes.py`** (duas rotas do
diário).

---

## Incidentes

(vazio)
