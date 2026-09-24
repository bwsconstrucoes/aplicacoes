# HISTÓRICO — AtualizaSPBotão

Memória desta área entre chats. Ler antes de mexer; atualizar antes de
encerrar. O que o módulo faz está no `README.md`.

## Pendente AGORA

1. **Saber o que o dono quer mudar no botão.** Já se sabe que o "do Render" é
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
