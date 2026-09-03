# Painel OMIE — decisões, incidentes e o que falta

Este arquivo existe para uma sessão nova (ou uma pessoa nova) pegar o trabalho
sem repetir o que já foi discutido e sem repetir o que já deu errado.

O `README.md` ao lado explica **como o painel é**. Este aqui explica **por que
ele é assim**, e o que aconteceu no caminho. Leia os dois antes de mexer.

---

## Onde o trabalho está

O painel financeiro do OMIE rodava em Streamlit, no computador do dono, lendo
arquivos de uma pasta de 153 MB. Virou módulo Flask do monorepo, em `/painel`,
com login próprio e os dados no Postgres do ERP (schema `painel`).

**Oito telas convertidas**, todas conferidas contra a versão original com dados
reais: Visão Geral, DRE, Receita de Obra, Fluxo de Caixa, Resultado por Obra,
Comprometido × Executado, Necessidade de Caixa e Prestação de Contas.

**Estado em 02/09/2026:** versão `cc48712` publicada, 403 testes passando.

### O que está pendente AGORA

O dono está tentando rodar a **primeira carga** — baixar toda a base do OMIE.
Ela já falhou quatro vezes, cada uma por um motivo diferente; todos corrigidos.
**Pergunte a ele se a carga terminou antes de qualquer outra coisa.**

Sinal de que terminou: a tela de Configurações mostra
*"185.422 linhas de lançamento e ... recebimentos em X min"*.

---

## Regras que não se discutem

Cada uma custou horas. Não são preferências.

### 1. Não publique na `main` enquanto uma carga estiver rodando

Publicar reinicia o serviço no Render, e o reinício mata a carga. Isso já
aconteceu **duas vezes**, uma delas causada por outra sessão do Claude
trabalhando no ERP, que juntou um ramo na main sem saber que havia carga em
curso.

**Pergunte antes de publicar. Sempre.** Trabalhar num ramo é seguro; juntar na
main não é.

### 2. Trabalho longo não roda dentro do gunicorn

O serviço sobe com `--workers 1 --max-requests 150`. O gunicorn **reinicia o
processo** a cada ~150 requisições — proteção contra vazamento de memória, posta
depois do OOM de julho de 2026. Com um worker só, esse reinício leva junto
qualquer thread de fundo.

Três cargas morreram por isso, sem deixar rastro. E a própria tela de
acompanhamento, consultando de 5 em 5 segundos, era parte do que as matava.

A saída **não** é mexer no `--max-requests`: ele protege os outros 14 módulos.
A saída está em `executar_sync.py` — processo separado e destacado.

### 3. Nada de abrir a base inteira em memória

A instância tem 2 GB, divididos com 14 módulos, e já teve OOM. O painel antigo
abria um arquivo de 4 MB que virava **179 MB** na memória. Hoje a reconstrução
inteira usa **14,6 MB** porque quem soma é o Postgres.

### 4. Tabelas em schema próprio

O ERP tem tabelas chamadas `titulos`, `categorias` e `rateios`. O espelho do
OMIE tem tabelas com esses mesmos nomes e significado completamente diferente.
Schemas separados são o que faz as duas conviverem.

### 5. O SQL é Postgres, não SQLite

O código do espelho nasceu falando com SQLite. **Três** construções que só
existem lá chegaram em produção, cada uma custando uma carga inteira:
`GROUP BY <apelido>`, `conn.cursor()` sem tradução, e `MAX(a, b)`.

`tests/test_painel_sql_portavel.py` varre a classe inteira desse problema.
Rode-o sempre que tocar em SQL.

### 6. A produção não é alcançável a partir dos testes

O `.env` da raiz aponta para o banco de **produção** — é assim que se
desenvolve local. Um teste que esqueça de dublar a conexão vai lá; aconteceu.
`db.py` agora recusa qualquer banco que não seja local e com "teste" no nome
enquanto o pytest roda.

### 7. Como falar com o dono

Ele não é programador. Português simples, o efeito e não a implementação, e o
risco sempre explícito. Nada de esconder o que não foi testado atrás de
"está pronto".

---

## Decisões já tomadas — não reabrir sem motivo novo

| Decisão | Por quê |
|---|---|
| Postgres do ERP, schema `painel` | O disco do Render é apagado a cada reinício; a configuração da prestação de contas não é regenerável |
| Módulo do serviço que já existe, não serviço novo | Sem custo adicional; reusa login, deploy e banco |
| Gráficos em SVG desenhado na página | O Plotly custava 3 MB de JavaScript por tela |
| Exportação em CSV, não `.xlsx` | Excel de verdade exigiria biblioteca nova; a regra da casa é não acrescentar sem combinar |
| `pandas` não é dependência do painel | A única parte que o usava foi feita em Python puro |
| Migrações aplicadas por botão, nunca no boot | Uma migração com defeito no start derrubaria os 15 módulos juntos |
| Hora convertida para Brasília **na fonte** | O servidor roda em UTC; se cada tela convertesse, uma esqueceria |

---

## A conferência dos números

Cada tela foi comparada com a original, sobre a base real:

- a tabela `fato`: **185.422 linhas nas duas**, diferença máxima de **R$ 4,60**
  em R$ 343 milhões — arredondamento para centavos ao gravar;
- a prestação de contas, sócio a sócio: diferença máxima de **R$ 0,22** em
  R$ 11 milhões.

Se alguma tela for mexida, refaça a comparação. As telas originais estão em
`referencia_streamlit/` justamente para isso.

---

## O que falta

1. **PDF do DRE.** O gerador original usa `reportlab`, que não está no serviço.
   Daria para refazer com `fpdf2`, que já está — mas é reescrever o relatório.
2. **Mensagem duplicada** na tela de Configurações: o mesmo erro aparece na
   linha "Última atualização" e na caixa vermelha de interrupção.
3. **Cenários da prestação** — comparar duas configurações de rateio lado a lado.
4. **Converter `app/apps/spsbd_app`** (análise de SPs) do mesmo jeito. É
   Streamlit, tem 835 MB (com um Python empacotado dentro), usa
   `streamlit-aggrid` — a parte mais difícil de portar — e traz um
   `render.yaml` propondo um serviço separado com disco pago, o que é uma
   decisão diferente da que foi tomada aqui e precisa ser conversada.

---

## Coisas pequenas que mordem

- Nunca deixe um arquivo chamado `app.py` solto numa pasta: ele sombreia o
  pacote `app` do projeto. Aconteceu com `referencia_streamlit/app.py`, hoje
  renomeado.
- `/painel/saude` mostra a versão publicada (`RENDER_GIT_COMMIT`). Use isso
  para saber se uma correção já subiu, em vez de clicar e torcer.
- Banco fora do ar dá erro em 10 segundos, não trava a tela. Antes travava.
- A carga inicial **retoma de onde parou**, por etapas marcadas no banco. Uma
  carga concluída apaga as marcas — senão a próxima pularia tudo e não faria
  nada.
