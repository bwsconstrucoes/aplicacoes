# ValidaSP — decisões, estado e o que falta

Para uma sessão nova pegar o trabalho sem refazer a leitura. O `README.md` ao
lado explica **como** funciona; este explica **por quê** e **onde parou**.

## Pendente AGORA

1. **Juntar o ramo na `main`** (só com o "pode" do dono) e, **no mesmo
   momento**, apertar **"Aplicar atualizações do banco"** no ERP › Configurações
   — cria a tabela da fila (migração 084). Sem o botão a fila funciona em disco,
   mas uma publicação apaga o que estiver nela.
2. **Conferir com o dono se algo dependia do campo "Validação da SP" no
   Pipefy** (uma automação que muda o card de fase, um relatório, outro cenário
   do Make). O validasp parou de marcar esse campo; se alguma coisa lia ele, vai
   parar de andar.
3. **As SPs que já ficaram sem validação antes desta mudança não voltam
   sozinhas.** A fila só guarda o que chega a partir da publicação. As antigas
   estão (com sorte) na aba `FalhaProcessar` como "Validar" — ou em lugar
   nenhum, se o serviço reiniciou no meio. Precisam de uma conferência na mão
   ou de uma reentrada pela fila (não existe rota para isso ainda).

## 17/10/2026 — a validação não se perde mais; o Pipefy saiu

**Pedido do dono:** *"eu não quero mais fazer nenhuma alteração no card do
Pipefy. Quero apenas marcar na planilha SPsBD o sim. (…) tem aparecido SPs que
não estão validadas (…) se a pessoa sabe que validou, precisamos garantir que a
validação aconteça."*

**O que estava errado** (lido no código, não visto em log):

- O "Sim" era gravado por uma tarefa solta na memória do serviço: 90 s de
  espera, uma tentativa, mais 90 s, outra. Se o Render reiniciasse nesse meio
  (publicação, renovação do `--max-requests`, queda), a tarefa sumia **sem
  deixar rastro em lugar nenhum** — e a página já tinha dito "Base de dados em
  atualização".
- Falhando as duas, ela escrevia na `FalhaProcessar` — que mora no **mesmo
  Google** que acabara de falhar. Google fora = as duas escritas caem juntas.
- A gravação nunca era conferida: `update_cell` sem erro era tratado como feito.

**O que mudou:**

- A validação entra numa **fila no banco** (Postgres do ERP, schema `validasp`)
  **antes** de qualquer mensagem sair. O banco não depende do Google.
- Um **vigia** dentro do serviço insiste até gravar, e só dá por feito depois de
  **ler a célula de volta**. Reinício não perde nada: a fila está no banco e o
  vigia liga de novo na subida.
- Banco fora: a fila vai para um arquivo do serviço e passa para o banco quando
  ele voltar. É a segunda linha de defesa (uma publicação apaga o disco).
- Desiste só depois de 7 dias sem achar a SP na SPsBD — aí escreve na
  `FalhaProcessar`, como antes.
- **Saiu:** a marcação no Pipefy e a mensagem de "erro no Pipefy" que ia por
  WhatsApp quando ela falhava. O resto (link de anuência, mensagens ao
  responsável, requerente e anuente) ficou igual.

**Por que no banco e não numa aba da planilha, como a fila da BaixaBradesco:**
a `BaixaBradescoFila` é uma aba do Google — quando o Google cai, a fila cai
junto. Aqui o problema É o Google falhar, então a fila tem de morar fora dele.
O padrão seguido é o das filas do ponto e da mensageria (tabela no banco do ERP,
migração pelo botão, código que responde "ainda não existe" sem estourar).

**Por que um vigia dentro do serviço e não o cron-job.org** (que a BaixaBradesco
usa): o cron é um sistema de terceiro que o dono teria de configurar; o vigia
não precisa de nada. A rota `/fila/processar` existe para quem quiser acionar
de fora mesmo assim.

**Testado:** 14 testes novos (10 sem banco, 4 com Postgres de verdade, migração
084 aplicada pelo mesmo caminho da produção) e a suíte inteira verde. **Não
testado:** contra a planilha real e o Make real — os testes usam uma planilha
dublada.

## O que ficou de fora

- Reentrada em massa das SPs antigas que ficaram sem validação (ver Pendente 3).
- Aviso ativo (Telegram/WhatsApp) quando uma SP passa muito tempo pendente ou é
  desistida. Hoje só o log e a rota `/fila` dizem.
- Os envios de Z-API próprios do validasp continuam fora do notificador
  (decisão já registrada em `mensageria/HISTORICO.md`).
