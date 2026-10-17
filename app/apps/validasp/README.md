# ValidaSP — `/api/validasp`

Roda quando alguém **valida uma Solicitação de Pagamento (SP)**. Quem chama é o
Make (a partir do Pipefy). Não tem tela: recebe o pedido, faz o trabalho e
devolve uma página pronta ("✅ Solicitação de Pagamento Validada") que o Make
mostra para quem validou.

Decisões e estado: `HISTORICO.md`, ao lado.

## O que acontece numa validação

1. **A validação entra na fila** (`fila.py`) — antes de qualquer outra coisa.
   Fica no banco (`validasp.pendencias`, migração 084 do ERP); se o banco não
   responder, num arquivo do próprio serviço. Só se os dois falharem a página
   diz "⚠️ A validação NÃO pôde ser registrada".
2. **Link de anuência**, só se a SP tiver anuente: monta o endereço do fluxo do
   Make com o bloco `anuencia` e encurta pelo `/encurtador/novo` (vale 8 dias).
3. **WhatsApp pelo Z-API** (com espelho no Telegram para quem está cadastrado):
   ao responsável, ao requerente (se houver telefone) e ao anuente, com o link.
   Os textos vêm prontos do Make; aqui só se troca `__link_anuencia__`.
4. **A resposta sai na hora.** O "Sim" na planilha é trabalho do vigia.

**Não mexe mais no card do Pipefy** desde 17/10/2026 (pedido do dono). Os campos
`msg_erro_pipefy` que o Make ainda manda são ignorados.

## O vigia da fila

Uma linha de execução dentro do serviço, ligada na subida e a cada validação,
que passa pela fila de 30 em 30 s:

- pega o que está na hora (a primeira tentativa espera 90 s, para dar tempo de
  a linha da SP nascer na SPsBD);
- lê a coluna A da SPsBD **uma vez por passada**, acha a **última** linha da SP,
  grava `Sim` na coluna **AH (Validação)** e **lê a célula de volta**. Só a
  leitura confirmando tira o item da fila;
- falhou (Google fora, cota, SP ainda não está na SPsBD, gravação que não
  pegou): tenta de novo em 1,5 → 3 → 5 → 10 → 30 → 60 min e daí de hora em hora;
- **7 dias** sem conseguir: desiste, guarda o motivo e escreve a SP na aba
  `FalhaProcessar` como "Validar" (o caminho antigo).
- o que estava em disco passa para o banco assim que o banco responder.

## Rotas

| Método | Rota | O que faz |
|---|---|---|
| POST | `/executar` | A validação (payload do Make, com `secret` e `id`) |
| POST | `/fila` | Resumo da fila: pendentes, a mais antiga, desistidas recentes |
| POST | `/fila/processar` | Anda com a fila agora, sem esperar o vigia |

Todas exigem o `secret` (`VALIDASP_SECRET`) no corpo — ou, nas duas da fila,
no cabeçalho `X-ValidaSP-Secret`.

## Variáveis

| Variável | Para quê |
|---|---|
| `VALIDASP_SECRET` | senha do serviço |
| `GOOGLE_CREDENTIALS_BASE64` | planilha "Registros de SP" (`1lrP1HOvwqyXiVdP2kuTgG7sJjl2QXl0WT4lwkd392DA`) |
| `DATABASE_URL` | a fila (mesmo banco do ERP) |
| `NOTIFICAR_WHATSAPP` | `0` desliga o WhatsApp aqui também |
| `VALIDASP_SEM_VIGIA` | qualquer valor: não liga o vigia (só para diagnóstico) |
| `VALIDASP_FILA_DIR` | pasta da fila em disco (padrão `/tmp/validasp_fila`) |

As credenciais do Z-API vêm no próprio payload do Make.

## Testes

`tests/test_validasp_fila.py` (sem banco) e `tests/test_validasp_fila_banco.py`
(com banco de verdade, `@pytest.mark.banco`).
