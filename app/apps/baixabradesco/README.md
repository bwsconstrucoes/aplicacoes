# BaixaBradesco — o robô que dá baixa nos comprovantes bancários

Este arquivo explica **o que a aplicação faz e como ela é**. O `HISTORICO.md` ao
lado explica **por que ela é assim** e o que já deu errado. Leia os dois antes de
mexer.

---

## Em uma frase

Chega o PDF do comprovante de um pagamento; o robô descobre a qual Solicitação
de Pagamento (SP) aquele comprovante pertence, dá a baixa do título no Omie,
marca a SP como paga na planilha, move o card no Pipefy, guarda o comprovante no
Dropbox e, se pedirem, avisa por WhatsApp.

Antes disso tudo era um cenário do Make.com montado peça por peça. A aplicação
substituiu esse cenário.

## Quem chama

| Quem | O que chama | Quando |
|---|---|---|
| Cenário do **Make.com** | `POST /api/baixabradesco/executar` | a cada lote de comprovantes que chega |
| **cron-job.org** | `POST /api/baixabradesco/processar-fila-tardia` | de 5 em 5 minutos, para retomar o que ficou parado |
| Quem estiver investigando | `POST /api/baixabradesco/diagnostico` | quando um comprovante não casou e ninguém sabe por quê |
| Quem estiver investigando | `POST /api/baixabradesco/reprocessar-fila` | para tentar de novo o que falhou depois do casamento |
| Monitor | `GET /api/baixabradesco/health` | sinal de vida |

Ninguém abre tela aqui: **não existe interface**. A aplicação só responde a
chamadas de máquina.

### Senha de entrada

Todas as rotas conferem a senha `BAIXABRADESCO_SECRET`, que vem no corpo do
pedido (campo `secret`) ou no cabeçalho `X-BaixaBradesco-Secret`. ⚠️ **Se a
variável estiver vazia no Render, a porta fica aberta**: o código libera a
passagem quando não há senha configurada.

### O freio de mão: `modo_teste`

O padrão é **`modo_teste: true`** — o robô lê tudo, decide tudo e devolve o
relatório do que faria, **sem escrever em lugar nenhum**. Para valer, o Make
precisa mandar `modo_teste: false` explicitamente. Além disso dá para ligar e
desligar cada etapa: `executar_omie`, `atualizar_spsbd`, `atualizar_pipefy`,
`enviar_whatsapp`, `salvar_comprovante`, e escolher a pasta do Dropbox em
`pasta_dropbox`.

## De onde vêm os dados

**O comprovante** chega dentro do próprio pedido, de uma de duas formas: o PDF
codificado em texto (`base64`) ou um endereço para baixar (`url`). Baixa por
endereço tem teto de 50 MB — acima disso o pedido é recusado em vez de a
instância morrer de falta de memória.

Um PDF pode ter várias páginas, e **cada página é tratada como um comprovante
separado**.

**As bases de consulta** são três planilhas do Google, lidas uma vez por lote:

| Planilha / aba | Para que serve |
|---|---|
| **SPsBD** (aba `SPsBD`, ~52 mil linhas) | a fonte da verdade das SPs: valor, credor, conta, status, código de barras |
| **SPsAgendar** (mesma planilha) | as SPs que ainda estão na fila de agendamento |
| **BaseBancos** (planilha própria) | de qual conta bancária saiu o dinheiro e qual é o código dessa conta no Omie e no Pipefy |
| **LogBaixaBradesco** (aba na SPsBD) | o registro do que já foi processado, para não pagar duas vezes |
| **BaixaBradescoFila** (aba na SPsBD) | a fila de falhas para tentar de novo |

Da SPsBD o robô carrega só duas fatias, e não a planilha inteira:

- as SPs **a pagar e agendadas** (coluna O = `Pagar`, coluna AB entre
  `agendar`, `agendado` e `falhaagendar`) — é onde ele procura o comprovante;
- as SPs **já marcadas como pagas mas sem data de pagamento e com comprovante
  guardado** — são as que a planilha já resolveu e o Omie ficou para trás.

## Como ele descobre de qual SP é o comprovante

Esta é a parte onde um erro custa dinheiro: casar o comprovante com a SP errada
baixa o título errado. As tentativas acontecem nesta ordem, e a primeira que
resolver ganha:

1. **O número da SP escrito no comprovante** (o campo "Descrição"). É o caminho
   mais confiável. Um cuidado: o QR Code do Pix começa com `000201` e já foi
   confundido com número de SP — números assim são ignorados de propósito.
2. **Somapay** (folha de pagamento): por valor, entre as SPs a pagar e
   agendadas, e só para despesas de rescisão, férias, gratificação ou
   participação nos lucros. Não usa a conta, porque o dinheiro sai do Bradesco
   mas a baixa acontece na conta Somapay.
3. **BeeVale** (vale-alimentação): por valor, aceitando o valor da SP com 1,5%
   de acréscimo (é a taxa da BeeVale) ou o valor exato.
4. **FGTS/Caixa**: por valor, entre as SPs a pagar e agendadas; se não achar,
   tenta por palavra-chave no nome do credor.
5. **Boleto**: pelo código de barras, comparado só pelos números, mais o valor.
6. **Valor + conta + tipo de pagamento**, entre as SPs a agendar.
7. **Valor + conta + status agendado**, na SPsBD. Se sobrar mais de uma
   candidata, o desempate procura o nome do credor **no texto bruto do PDF**.
8. **Última tentativa**: as SPs que a planilha já marcou como pagas e que o Omie
   não baixou. Aqui ele executa **só o Omie** e não mexe em mais nada.

**Se sobrar mais de uma candidata e o desempate não resolver, ele não executa
nada** — marca como `pendente_validacao` e alguém precisa olhar.

## O que ele escreve quando casa

Nesta ordem:

1. **Omie**: consulta o título, altera (para garantir que o valor lançado é o
   valor real do comprovante, mesmo que alguém tenha digitado errado) e baixa.
   Juros e multa vão como acréscimo, não como valor do documento. Se o título já
   estiver pago, ele para e registra que já estava.
2. **Registro na LogBaixaBradesco**, para aquele mesmo comprovante não voltar.
3. **SPsBD**: coluna O = `Pago`, V = carimbo de hora, X = data do pagamento,
   AG = link do comprovante no Dropbox, AK = número da conta que pagou.
4. **Pipefy**: preenche os campos do card e move para a fase "Pago/Alimentar
   Omie".
5. **WhatsApp** (Z-API), só se pedirem. Falha de WhatsApp nunca derruba a baixa.

O comprovante **só é guardado no Dropbox depois de casar** com uma SP — assim
não se acumulam arquivos órfãos de comprovante que ninguém sabe de quem é. Se a
SP já tiver comprovante, o link antigo é reaproveitado e nada novo é enviado.

Qualquer falha depois do casamento (Omie, Pipefy ou WhatsApp) vai para a aba
**BaixaBradescoFila**, para ser tentada de novo pela rota de reprocessar.

## Quando o Google diz "chega"

A planilha tem cota por minuto, e a mesma conta de serviço é usada por todos os
módulos do monorepo. Quando o Google recusa por excesso de pedidos, o robô
**não devolve erro para o Make** — grava o pedido em disco (`/tmp`), responde
"recebido, adiado" e o cron reprocessa a cada 5 minutos, até 10 tentativas.

⚠️ O que está em `/tmp` **se perde quando o serviço reinicia** (ou seja, a cada
publicação). Foi aceito assim: o Make pode reenviar.

## Variáveis de ambiente

| Variável | Para quê | Sem ela |
|---|---|---|
| `BAIXABRADESCO_SECRET` | senha das rotas | **as rotas ficam abertas** |
| `BAIXABRADESCO_DEBUG` | devolve o rastro do erro na resposta | erro sai só com a mensagem |
| `GOOGLE_CREDENTIALS_BASE64` | acesso às planilhas | nada funciona |
| `OMIE_BWS_APP_KEY` / `OMIE_BWS_APP_SECRET` | acesso ao Omie (o Make normalmente manda as chaves no próprio pedido) | a baixa no Omie falha |
| `PIPEFY_API_TOKEN` | acesso ao Pipefy | o card não é atualizado |
| `DROPBOX_APP_KEY` / `DROPBOX_APP_SECRET` / `DROPBOX_REFRESH_TOKEN` | guardar o comprovante | o comprovante não é salvo |
| `ZAPI_INSTANCE_ID` / `ZAPI_API_TOKEN` / `ZAPI_CLIENT_TOKEN` | WhatsApp | o aviso é pulado |
| `NOTIFICAR_WHATSAPP` | desliga o WhatsApp de vez | ligado |

## Serviços que ele toca

Google Sheets (leitura e escrita), **Omie** (títulos a pagar e lançamento em
conta corrente), **Pipefy** (cards), **Dropbox** (arquivo do comprovante),
**Z-API** (WhatsApp).

## Os arquivos

| Arquivo | Papel |
|---|---|
| `routes.py` | as quatro rotas e a senha |
| `core.py` | o maestro: lê o PDF, casa, executa, monta o relatório |
| `parser_pdf.py` | tira o texto de cada página do PDF |
| `parser_bradesco.py` | entende o comprovante do Bradesco |
| `parser_sicredi.py` | entende o comprovante do Sicredi (ver a ressalva abaixo) |
| `matcher.py` | decide de qual SP é o comprovante |
| `omie.py` | as chamadas ao Omie, inclusive a transferência da folha Somapay |
| `pipefy.py` | busca e atualização dos cards |
| `sheets.py` | leitura e escrita nas planilhas, e o mapa das colunas |
| `storage.py` | Dropbox |
| `zapi.py` | WhatsApp |
| `fila.py` | fila de falhas depois do casamento |
| `fila_tardia.py` | fila de pedidos adiados por cota do Google |
| `diagnostico.py` | analisa sem executar |
| `models.py` | os formatos de dado que circulam entre os arquivos |
| `utils.py` | conversões de dinheiro, data, conta e texto |

## Cuidados que não são opcionais

- **Memória.** Esta aplicação divide 2 GB com os outros 15 blueprints do monorepo e já derrubou o
  serviço inteiro em julho de 2026. Nunca voltar a ler a planilha inteira
  (`get_all_values()`), nunca baixar arquivo sem teto, nunca duplicar a leitura
  da SPsBD. Detalhe no `CONTEXTO.md` §9.
- **Trabalhar sempre em cima do arquivo que está publicado.** Já se perderam
  correções por aplicar remendo sobre versão velha.
- **Comparar status da planilha sempre normalizado** (`Agendado` e `agendado`
  são a mesma coisa; tratar como diferentes já custou dias).
- **Conferir o mapa de colunas com o dono**, pelo cabeçalho de verdade — nunca
  contando células de uma linha copiada.

## Ressalvas do código de hoje (conferidas em 04/09/2026, na `main`)

Duas coisas que estão escritas na documentação antiga como se funcionassem, e
que **no código de hoje não acontecem**:

1. **O leitor do Sicredi nunca é chamado.** O `core.py` manda toda página para o
   leitor do Bradesco. O `parser_sicredi.py` existe, está completo e ninguém o
   usa.
2. **A trava contra pagar duas vezes só grava, não confere.** A função que
   pergunta "esta página já foi processada?" está escrita e é importada, mas
   nunca é chamada antes de executar. O registro é gravado; a consulta não
   acontece.

Os dois estão registrados aqui e no `HISTORICO.md` para o dono decidir; nenhum
foi alterado.

Uma terceira, essa **já corrigida em 04/09/2026**: o comprovante recusado pelo
banco só era barrado se o texto dissesse exatamente "Operação Não Realizada".
Um comprovante real que dizia "Transação Não Realizada" passava como boleto
normal. Hoje a recusa é uma lista de frases (`FRASES_RECUSA` no
`parser_bradesco.py`), a checagem acontece antes de qualquer extração — um
comprovante recusado não entrega nem valor nem código de barras —, o leitor do
Sicredi usa a mesma trava, e o que foi recusado aparece no resumo da resposta em
`recusados_nao_efetivados`.
