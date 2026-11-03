# Exclusão no Omie de títulos de cards cancelados no Pipefy

Programa de PC (fora do sistema web — não roda no Render, não mexe em nenhuma outra área).

> **Pedido original: CANCELAR. O programa EXCLUI.** A API do Omie não tem comando de cancelamento para conta a
> pagar (ver seção ⚠️ abaixo). Em 03/11/2026 o dono decidiu: *"pois vamos excluir"*. A pasta manteve o nome.

## O que faz

1. Lê a planilha com os números dos cards cancelados no Pipefy (**primeira coluna**, um por linha;
   cabeçalho e linhas com texto são ignorados; número repetido conta uma vez).
2. Baixa do Omie a **lista inteira de contas a pagar** (páginas de 500) e procura nela o código de
   integração `Int` + número do card (o mesmo que o `processarnovasp` grava ao criar o título).
   Não consulta card por card — ver "Por que baixar a lista" abaixo.
3. Separa:
   - **não existe no Omie** → nada a fazer (foi excluído);
   - **já estava cancelado** → nada a fazer;
   - **pago / com baixa** (inclusive parcial) → não cancela, fica para revisão;
   - **situação desconhecida** → não cancela, fica para revisão;
   - **em aberto e sem pagamento** (A VENCER, ATRASADO, VENCE HOJE) → candidato a excluir.
4. Mostra o resumo e **pede para digitar SIM** antes de excluir. Enter sem SIM = só relatório.
5. Exclui (`ExcluirContaPagar`) só os em aberto e sem pagamento. Já cancelado, pago ou com baixa parcial
   não é tocado. Os 5 primeiros são conferidos com uma consulta; os demais valem pela confirmação do Omie —
   conferir exclusão é uma consulta que volta como erro, e erro em série bloqueia a chave.
6. Grava `relatorios/relatorio_<data>.xlsx` (aba Resumo + aba Detalhe, com cores).

## Como usar no PC

1. Ter o Python instalado (python.org, marcar "Add python.exe to PATH").
2. Copiar esta pasta para o PC e dar dois cliques em `executar.bat`.
   Na primeira vez ele cria o `.env` e abre no Bloco de Notas: preencher `OMIE_KEY` e `OMIE_SECRET`.
3. Na pasta `entrada`, **colar os números num arquivo `.txt`** (um por linha; a coluna copiada do Excel
   serve) ou pôr a planilha `.xlsx`/`.csv`. Todos os arquivos da pasta são lidos juntos.
   (Ou arrastar um arquivo em cima do `executar.bat` — aí ele não é movido.)
4. Rodar o `executar.bat`.

## O que foi resolvido não volta

Ao fim de uma rodada de verdade (não simulação), os arquivos lidos vão para `entrada/processados/`, e os cards
que **falharam** ou **não chegaram a ser processados** (Omie pediu espera longa demais, rede caiu, recusa,
execução interrompida) viram `entrada/pendentes_<data>.txt`. Rodar de novo consulta **só esses**.
Os demais estão resolvidos: cancelado, já cancelado, não existe — ou pago/revisar, que dependem de olho humano
e estão no relatório.

Para só consultar sem nem ser perguntado: `SIMULAR=sim` no `.env`.

## Pasta no Dropbox / OneDrive

Pode. O `executar.bat` instala o Python do programa **fora** da pasta, em
`%LOCALAPPDATA%\BWS\cancela_titulos_omie\venv` — dentro do Dropbox a sincronização trava os arquivos
no meio da instalação (aconteceu em 03/11/2026: `WinError 32 … sendo usado por outro processo`).
Se uma instalação não terminar, a próxima execução apaga e refaz do zero.

## Por que baixar a lista (e não consultar card por card)

Em 03/11/2026, na primeira rodada real (4.755 cards), a versão que consultava um por um levou bloqueio de 30
minutos depois de 14 consultas: card sem título no Omie volta como **erro** ("não cadastrado"), e erro em
série o Omie pune com *"API bloqueada por consumo indevido. Tente novamente em 1799 segundos"*. A chave
bloqueada é a mesma do sistema da empresa (painel, BaixaBradesco, criação de títulos).

Baixando a lista, toda chamada dá certo e card ausente é só um código que não aparece nela. Na primeira
rodada o Omie informou **1.205 páginas** — a baixa é longa (a tela mostra quanto falta), mas é feita uma
vez: a lista fica em `%LOCALAPPDATA%\BWS\cancela_titulos_omie\contas_pagar_omie.json` (fora do Dropbox,
que travou o arquivo em 03/11/2026) e vale por `VALIDADE_LISTA_HORAS` (12 h). É regravada a cada 25 páginas
e ao parar; interrompida, a baixa continua de onde parou. Se a lista tiver mais de
1 h na hora de excluir, cada título é reconsultado antes (título que existe responde sem erro).

## Cuidado com o Omie (limite de consultas)

- Pausa fixa entre chamadas (`PAUSA_ENTRE_CHAMADAS`, padrão 1,5 s).
- Quando o Omie manda esperar ("tente novamente em N segundos", HTTP 425/429, "consumo indevido"),
  o programa espera **o tempo pedido + 5 s**, com contagem na tela, e repete a mesma chamada.
  "Consumo redundante" espera 65 s. Pedido de espera acima de `TETO_ESPERA_SEGUNDOS` (1 h) não é
  esperado: o título fica como "não processado".
- Erro definitivo (não cadastrado, período contábil fechado, "não é permitido") **não** é repetido.
- Três recusas iguais seguidas na exclusão, ou o Omie não reconhecer o comando, param as
  exclusões restantes (sem martelar o Omie).
- Se a resposta da exclusão se perder, ele consulta antes de dar como falha.

## ⚠️ O Omie NÃO tem comando de API para cancelar conta a pagar (confirmado em 03/11/2026)

Na primeira rodada real o Omie respondeu `Method "CancelarContaPagar" not exists`. As bibliotecas geradas da
definição oficial da API listam, para `financas/contapagar`, só: Incluir, Alterar, Excluir, Consultar, Listar,
Upsert, LancarPagamento e CancelarPagamento (este desfaz uma BAIXA, não cancela o título). Cancelar o título
existe só na tela do Omie. Contas a RECEBER têm `CancelarContaReceber`; contas a pagar, não.

Resultado da rodada: 4.755 cards → 64 em aberto (não cancelados), 36 já cancelados, 4.646 inexistentes,
9 pagos/para revisar. Os 64 estão no relatório e em `entrada/pendentes_*.txt`. **Decisão do dono, no mesmo dia: excluir**
(`ExcluirContaPagar`), mesmo sabendo que a exclusão dá trabalho no painel.

Testes: `tests/test_cancela_titulos_omie.py` (Omie dublado).
