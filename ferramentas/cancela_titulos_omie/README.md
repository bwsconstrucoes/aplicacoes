# Cancelamento no Omie de títulos de cards cancelados no Pipefy

Programa de PC (fora do sistema web — não roda no Render, não mexe em nenhuma outra área).

## O que faz

1. Lê a planilha com os números dos cards cancelados no Pipefy (**primeira coluna**, um por linha;
   cabeçalho e linhas com texto são ignorados; número repetido conta uma vez).
2. Consulta no Omie a **conta a pagar** de código de integração `Int` + número do card
   (o mesmo que o `processarnovasp` grava ao criar o título).
3. Separa:
   - **não existe no Omie** → nada a fazer (foi excluído);
   - **já estava cancelado** → nada a fazer;
   - **pago / com baixa** (inclusive parcial) → não cancela, fica para revisão;
   - **situação desconhecida** → não cancela, fica para revisão;
   - **em aberto e sem pagamento** (A VENCER, ATRASADO, VENCE HOJE) → candidato a cancelar.
4. Mostra o resumo e **pede para digitar SIM** antes de cancelar. Enter sem SIM = só relatório.
5. Cancela (nunca exclui) e consulta de novo para conferir.
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

## Cuidado com o Omie (limite de consultas)

- Pausa fixa entre chamadas (`PAUSA_ENTRE_CHAMADAS`, padrão 1,5 s).
- Quando o Omie manda esperar ("tente novamente em N segundos", HTTP 425/429, "consumo indevido"),
  o programa espera **o tempo pedido + 5 s**, com contagem na tela, e repete a mesma chamada.
  "Consumo redundante" espera 65 s. Pedido de espera acima de `TETO_ESPERA_SEGUNDOS` (1 h) não é
  esperado: o título fica como "não processado".
- Erro definitivo (não cadastrado, período contábil fechado, "não é permitido") **não** é repetido.
- Três recusas iguais seguidas no cancelamento, ou o Omie não reconhecer o comando, param os
  cancelamentos restantes (sem martelar o Omie).
- Se a resposta do cancelamento se perder, ele consulta antes de dar como falha.

## Ponto não confirmado

O nome do comando de cancelamento (`CancelarContaPagar`, com `codigo_lancamento_omie`) não pôde ser
conferido na documentação do Omie a partir daqui (o site da API não abre do ambiente de desenvolvimento).
Se o Omie disser que o método não existe, o programa para no primeiro e o relatório mostra a frase dele;
o nome se troca no `.env` (`OMIE_METODO_CANCELAR`), sem mexer no código. **Recomendado: a primeira rodada
real com uma planilha de 1 ou 2 cards.**

Testes: `tests/test_cancela_titulos_omie.py` (Omie dublado).
