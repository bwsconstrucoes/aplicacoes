# HISTÓRICO — AtualizaSPBotão

Memória desta área entre chats. Ler antes de mexer; atualizar antes de
encerrar. O que o módulo faz está no `README.md`.

## Pendente AGORA

1. **Publicado em 13/11/2026** (desligamento do Log, botão e SP nova; medição por etapa; recusa de clique repetido).
2. **Depois de publicar, ler no Render as linhas `[atualizaspbotao]`** do
   próximo botão lento: elas dizem a etapa que segura e se o clique se repetiu.
3. **Saber o que o dono quer mudar no botão.** Já se sabe que o "do Render" é
   este mesmo módulo (ver "Como o botão é disparado"); falta o objetivo do
   trabalho. Nada foi alterado no código.

## Como o botão é disparado (fonte: relato do dono, 24/09/2026)

**Pipefy → Make → Render.** A pessoa aperta o botão no card da SP no Pipefy;
o Pipefy dispara um webhook (aviso automático) para um cenário do **Make**; o
Make chama `/api/atualizaspbotao/executar` neste serviço do Render. O objetivo,
nas palavras do dono: levar as informações do card do Pipefy para o **Omie** e
para a planilha **Registros de SP** (abas `SPsBD` e `Log`).

Consequências:

- O "atualizaspbotao do Render" que o dono pediu para trazer **é este
  módulo** — não existe outro serviço.
- A senha da rota e as chaves do Omie ficam guardadas **no cenário do Make**.
- As etiquetas do Pipefy que este módulo calcula são, ao que tudo indica,
  aplicadas no card pelo próprio Make (este código não chama o Pipefy) — **não
  confirmado**.
- O dono **não conhece** o Apps Script de que o código foi traduzido. Ele deve
  ter sido o caminho usado antes de 25/04/2026; se o Make ainda aponta para ele
  em algum ramo do cenário, existem dois caminhos gravando a mesma SP — **não
  confirmado**.

## Linha do tempo (dos commits — fonte: `git log`)

| Data | O que mudou |
|---|---|
| 25/04/2026 | Tradução do Apps Script para Python (`99e769b`). No mesmo dia: pedido ao Omie corrigido para ficar igual ao cenário do Make.com; resposta passou a trazer a página HTML |
| 27/04/2026 | `Log` passou a usar `append_rows` (estourava o limite de linhas da aba); centro de custo `[]` deixou de virar linha no Log |
| 06/05/2026 | Centros de custo do Log passaram a ser tirados do rateio múltiplo quando vêm vazios |
| 13/05/2026 | Conserto automático "alterar → incluir" quando o Omie diz que o título não existe (erro Client-103); números com até 10 casas decimais |
| 22/06/2026 | Carimbo de data/hora de Fortaleza na coluna V da SPsBD |
| 24/09/2026 | README e este HISTÓRICO criados (nenhuma mudança de código) |
| 13/11/2026 | Escrita na aba `Log` desligada (pedido do dono); medição por etapa e recusa de clique repetido na mesma SP |

## 24/09/2026 — primeira sessão desta área

**Pedido do dono:** trazer para cá um "atualizaspbotao" que roda no Render.

**O que foi apurado:**

- O módulo **já existe neste repositório** desde 25/04/2026 e já está no ar
  em `/api/atualizaspbotao/executar`, no mesmo serviço do Render que roda o
  resto (`srv-d167o5qli9vc73cvq83g`, `CONTEXTO.md`).
- **Não há ramo** com esse nome aqui, e **não há repositório** com esse nome
  entre os que a conta do GitHub enxerga. A conta vê três: `aplicacoes`, `erp`
  e `app-bws`. Os dois últimos foram abertos e **não** contêm este código
  (`app-bws` é o encurtador de links de abril/2025; `erp` é o ERP antigo).
- O arquivo `main_atualizado.py` é o `main.py` que veio junto da tradução em
  abril — prova de que o código foi escrito fora e colado aqui. Ele não é
  usado por nada.
- A origem, pelos comentários, é um **Apps Script** (arquivos `.gs`) e um
  cenário do **Make.com**. É possível que o "do Render" seja, na verdade, um
  desses dois — ou este mesmo módulo.

**Pontos de atenção achados na leitura** (não corrigidos — só registrados,
porque não foram pedidos e mexem em produção):

1. **Senha vazia passa.** Se `ATUALIZASPBOTAO_SECRET` não estiver configurada
   no Render, uma chamada com `"secret": ""` é aceita. Com a variável
   configurada, não há problema. Conferir no Render que ela existe.
2. **A chave do Omie viaja em cada chamada** (`omieAppKey`/`omieAppSecret` no
   corpo). Quem chama a rota guarda a chave; ela não está no Render. Se a chave
   do Omie for trocada, é lá (no chamador) que se atualiza.
3. **Hora da aba `SPsDDA` em UTC.** A linha nova do DDA usa a hora do servidor
   sem fuso — no Render, 3 horas à frente de Fortaleza. A coluna V da SPsBD já
   foi corrigida para Fortaleza em junho; esta ficou para trás.
4. **Leitura da coluna A inteira.** Cada chamada lê a coluna A da SPsBD
   (~52 mil linhas) duas vezes e a do Log uma vez. É leve (uma coluna só),
   mas é o lugar a olhar se o botão ficar lento.
5. **Trava da planilha-calculadora vale por processo.** Funciona porque o
   Render roda 1 worker; com mais de um, dois botões simultâneos poderiam
   disputar a mesma linha.

## 13/11/2026 — aba Log desligada

**Pedido do dono:** "Desative a atualização da aba Log."

**O que mudou:** `sheets.py` ganhou `CONFIG['LOG_ATIVO'] = False`. Desligado,
o botão não abre a aba Log, não lê a coluna A dela e não grava nada; a SPsBD
segue igual. A página de resposta passa a dizer só "Banco de dados
atualizado". Para religar: `True` e publicar.

**Economia por clique** (contada num teste com planilha simulada, não medida
em produção): 3 a 4 chamadas ao Google a menos — abrir a aba, ler a coluna A
inteira do Log e 1 ou 2 gravações. Num clique típico com rateio, são ~16 a 17
chamadas ao Google no total, então ~20% delas. Em tempo, a estimativa é de 1
a 2 s a menos num clique de ~6 a 10 s; o maior peso continua sendo a espera
fixa de 2 s da planilha de cálculo e as chamadas ao Omie.

**Efeito colateral avisado ao dono:**

- Primeiro desligamento: só o botão. SP nova ainda entrava pelo
  `processarnovasp`.
- **No mesmo dia o dono pediu para desligar também a SP nova** ("não precisa
  mais"): `processarnovasp/sheets.py` ganhou `LOG_ATIVO = False`. `inserir_log`
  volta "pulado" sem abrir a planilha; na rota de transferência, o cálculo de
  rateio — que só servia ao Log — também deixou de rodar (uma passagem a menos
  na planilha de cálculo). Nas rotas padrão e pagamento futuro o rateio
  continua, porque alimenta o Omie e a checagem de duplicidade do boleto.
- **Resultado: nenhuma automação do Render escreve mais na aba Log.** Ela fica
  congelada no estado de 13/11/2026, a menos que outro escritor fora deste
  repositório (scripts do Google dentro das Análises, cenário do Make) siga
  gravando — não verificado.
- O `sync_logs` copia a aba Log para as planilhas "Análises de Pagamentos"
  (`docs/sync_logs.md`). Se ele estiver ligado, essas cópias também congelam.
  Se elas ainda são usadas, ficou como pergunta ao dono.

## 13/11/2026 — botão lento e disparando muito

**O que o dono mostrou** (log do Render, 18:39–18:40): três chamadas do botão
abertas ao mesmo tempo (78 s, 72 s e uma nova), uma terminando em **155 s**, e
as 4 linhas do serviço ocupadas — 3 delas pelo botão. Pergunta dele: "não sei
pq o atualizasp tá disparando tanto".

**O que o log não diz:** se as três chamadas eram da mesma SP (gente clicando
de novo porque a página demora) ou de SPs diferentes, nem em que etapa foram
os 155 s. O código não registrava nenhuma das duas coisas.

**O que entrou (ramo, não publicado):**

1. **Medição por etapa** — uma linha `[atualizaspbotao] SP N: terminou em X s —
   abrir planilha …, boleto …, planilha de cálculo …, SPsBD …, Omie …` por
   chamada; cada chamada ao Omie registra seu tempo e status; espera de mais
   de 1 s na fila da planilha de cálculo também é registrada (essa fila
   serializa todos os botões — um por vez).
2. **Clique repetido na mesma SP é recusado** enquanto a primeira chamada não
   termina: resposta 409 com a página "A SP N já está sendo atualizada.
   Aguarde…", sem tocar em planilha nem Omie. SPs diferentes seguem em
   paralelo. **Risco não verificado:** como o cenário do Make trata a resposta
   409 — o botão já devolvia 400/500 em erro, então o Make já recebe respostas
   de erro hoje; se o cenário estiver configurado para desligar após erros,
   o clique repetido conta como um.

**Suspeitas a confirmar com a medição:** Omie lento (cada chamada pode esperar
até 30 s, e o fluxo faz até 3 — cliente, inclusão, conserto automático) e a
fila da planilha de cálculo (espera fixa de 2 s por botão, um por vez).

**`sync_logs`:** o dono informou que o agendamento do cron-job.org **já
estava desligado**. A aba Log congelada não tem efeito nas "Análises de
Pagamentos" além do que já existia.

## O que precisa ser confirmado com o dono

Respondido em 24/09/2026: quem chama a rota (Pipefy → Make → Render) e que o
"do Render" é este módulo. O dono não conhece o Apps Script de origem.

Em aberto:

1. **O que se quer mudar no botão?** (o motivo de abrir esta área)
2. **No cenário do Make, o endereço chamado é só o do Render?** Basta abrir o
   módulo HTTP do cenário e ver se o endereço tem `onrender.com` (ou o domínio
   do serviço). Se aparecer `script.google.com`, o Apps Script antigo ainda roda.
3. **Quem aplica as etiquetas no card do Pipefy** — o Make, com o que este
   módulo devolve?
4. **Corrigir os pontos 1 (senha vazia) e 3 (hora da SPsDDA)** da lista de
   atenção acima?
