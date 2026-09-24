# HISTÓRICO — AtualizaSPBotão

Memória desta área entre chats. Ler antes de mexer; atualizar antes de
encerrar. O que o módulo faz está no `README.md`.

## Pendente AGORA

1. **Descobrir o que é o "atualizaspbotao do Render" que o dono quer trazer
   para cá** (pedido de 24/09/2026). Ver "O que precisa ser confirmado com o
   dono" abaixo — nada foi alterado no código enquanto isso não se resolve.

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

1. **O que exatamente é o "atualizaspbotao do Render"?** Um serviço separado
   no painel do Render (com nome e endereço próprios)? Se sim: qual o
   repositório do GitHub dele, e em qual conta — a atual não enxerga nenhum com
   esse nome. Ou é o Apps Script / o cenário do Make que ainda roda em paralelo?
2. **O que chama esta rota hoje?** Botão do Pipefy direto, Make, Apps Script?
   Isso define se desligar o antigo quebra alguma coisa.
3. **O Apps Script original ainda roda?** Se sim, há dois caminhos gravando a
   mesma SP — convém saber antes de mexer em qualquer um.
