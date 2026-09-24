# AtualizaSPBotão (`/api/atualizaspbotao`)

O "botão" de atualizar uma SP (Solicitação de Pagamento). Uma chamada só faz
cinco coisas em sequência: confere o código de barras do boleto, calcula as
etiquetas do Pipefy, calcula o rateio pelos centros de custo, grava a SP nas
planilhas `Log` e `SPsBD` e registra (ou atualiza) o título a pagar no Omie.
Devolve, além do JSON, uma página HTML pronta com o resultado para quem apertou
o botão.

Escrito em 24/09/2026 a partir do código lido de ponta a ponta e do histórico
de commits. **O que chama esta rota (Pipefy, Make, Apps Script?) não está no
código** — ver `HISTORICO.md`, "O que precisa ser confirmado com o dono".

## De onde veio

Nasceu **dentro deste repositório** em 25/04/2026 (commit `99e769b`), como
tradução para Python de um Apps Script (`Principal.gs`, `AtualizaLogeSPsBD.gs`,
`EtiquetaPipefy.gs`, `BoletoDDA.gs`, `Omie.gs`, `Utils.gs`). A montagem do
pedido ao Omie foi corrigida para ficar igual a um cenário do **Make.com**
(os comentários citam "módulo 198" do blueprint). Não há repositório separado
com esse nome entre os que a conta do GitHub enxerga (conferido em 24/09/2026:
só `aplicacoes`, `erp` e `app-bws`, e nenhum dos dois últimos tem este código).

## Rota

| Método | Rota | O que faz |
|---|---|---|
| POST | `/api/atualizaspbotao/executar` | Executa as cinco etapas abaixo |

- **Autenticação:** campo `secret` no corpo, comparado com
  `ATUALIZASPBOTAO_SECRET`.
- **Obrigatório:** `id` (número da SP).
- **Resposta:** `{'ok': True, 'secoes': {...}, 'response': '<html>…'}`; erro de
  validação é 400, erro na execução é 500 — os dois também trazem `response`
  com HTML de erro.

## As cinco etapas (`core.py::executar`)

1. **Boleto / DDA** (`boleto.py`) — roda quando o tipo de pagamento é "Boleto",
   ou quando veio código de barras e o pagamento não é Pix nem BeeVale. Confere
   os dígitos verificadores (boleto bancário de 47 dígitos ou arrecadação de
   48). Válido: acrescenta uma linha na aba `SPsDDA` (data, código, SP,
   "Baixar") e reordena a aba. O resultado vai para a coluna AI da SPsBD
   (código ou `INVALIDO`).
2. **Etiquetas do Pipefy** (`pipefy.py`) — traduz os nomes das etiquetas em
   IDs e acrescenta a etiqueta "Boleto" quando o boleto é válido. **Só
   calcula; não chama o Pipefy.** Quem chamou a rota é quem aplica.
3. **Parâmetros do Omie** (`parametros_omie.py`) — só se o corpo trouxer
   `OmieApiA`…`OmieApiV`. Usa uma **planilha como calculadora**
   (`1FyswS4ZlCr2f8VaVvA52hOmajLIQhQISh49S2l-75Wc`, abas "Bases Resumo Botão" e
   "Bases Resumo Parcela Botão"): escreve os dados numa das 5 linhas livres,
   espera 2 s as fórmulas recalcularem, lê o resultado (centros de custo,
   valores, categoria, vencimento, cliente Omie, pedido) e apaga a linha. As
   regras de cálculo vivem nas fórmulas da planilha, não no Python.
4. **Log e SPsBD** (`sheets.py`) — planilha "Registros de SP"
   (`1lrP1HOvwqyXiVdP2kuTgG7sJjl2QXl0WT4lwkd392DA`).
   - `Log`: uma linha por centro de custo (até 5). Se a SP já tinha o mesmo
     número de linhas, atualiza; senão, "borra" as antigas com `######` e
     acrescenta novas.
   - `SPsBD`: se a SP existe, atualiza só as colunas que vieram preenchidas
     (A…AI); se não existe, cria a linha. A coluna V recebe sempre o carimbo de
     data/hora de Fortaleza.
5. **Omie** (`omie.py`) — só se o corpo trouxer `omieAppKey` e
   `omieAppSecret`. Cadastra o fornecedor se a planilha-calculadora disser que
   ele não existe, e então inclui (`IncluirContaPagar`, código `Int<id>`) ou
   altera (`AlterarContaPagar`) o título a pagar. Dois consertos automáticos:
   incluir que esbarra em "já cadastrado" vira alterar; alterar que esbarra em
   "não cadastrado" vira incluir. O código de integração devolvido é gravado
   na coluna P da SPsBD.

## Variáveis de ambiente

- `ATUALIZASPBOTAO_SECRET` — senha da rota.
- `GOOGLE_CREDENTIALS_BASE64` — conta de serviço do Google (padrão do repo).
- **As chaves do Omie NÃO são variável de ambiente aqui**: chegam no corpo de
  cada chamada (`omieAppKey`, `omieAppSecret`). Diferente dos outros módulos,
  que usam `OMIE_KEY`.

## Quem mais usa este código

- `processarnovasp` copia os algoritmos de boleto, o normalizador de código de
  barras e o formato do payload daqui.
- `validasp` importa `as_string` de `utils.py`.
- `analisesps/aportes_omie.py` tirou daqui os nomes dos campos de inclusão de
  conta a pagar.

Mexer em `utils.py` ou `boleto.py` afeta esses módulos.

## Arquivos

| Arquivo | Papel |
|---|---|
| `routes.py` | Rota e as páginas HTML de resultado e de erro |
| `core.py` | Orquestra as cinco etapas; senha e planilha padrão |
| `boleto.py` | Dígitos verificadores, aba `SPsDDA` |
| `pipefy.py` | Tabela nome → ID das etiquetas |
| `parametros_omie.py` | Planilha-calculadora de rateio |
| `sheets.py` | Escrita em `Log` e `SPsBD` |
| `omie.py` | Chamadas ao Omie (fornecedor e conta a pagar) |
| `utils.py` | Números em formato brasileiro, letras de coluna |
| `main_atualizado.py` | **Sobra da migração**: o `main.py` que acompanhou a tradução em abril/2026. Não é importado por ninguém |

Não há teste automatizado para este módulo.
