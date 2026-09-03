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

**Nove telas convertidas**, todas conferidas contra a versão original com dados
reais: Visão Geral, DRE, Despesas Analítico, Receita de Obra, Fluxo de Caixa,
Resultado por Obra, Comprometido × Executado, Necessidade de Caixa e Prestação
de Contas.

**Estado em 03/09/2026 (tarde):** a primeira carga terminou — o dono confirmou
— e o painel e o ERP abrem normalmente; o banco de produção voltou a aceitar
conexão (ver a seção sobre isso mais abaixo).

O ramo **`painel-dre-fiel`** é o `painel-fiel` com a `main` trazida para dentro
e com o teste do DRE consertado. É o que está pronto para publicar, **e ainda
não foi publicado** — falta o "pode" do dono.

### O que foi verificado neste ramo

- **782 testes passando no GitHub Actions**, com Postgres de verdade (é o
  número que vale; no PC sem banco são 624 e 158 pulados).
- A aplicação sobe com os **18 blueprints** — uma importação quebrada em
  qualquer módulo derrubaria todos juntos.
- Os rótulos e a ordem das linhas do DRE conferidos um a um contra
  `referencia_streamlit/`.
- A classificação de aporte em SQL conferida contra a versão em Python: mesmos
  padrões, mesma ordem de avaliação, mesma remoção de acento.

**Não verificado:** nenhuma tela deste ramo foi aberta contra a base real. O
`.env` com a `DATABASE_URL` de produção não está no repositório, e a regra da
casa é que a produção não é alcançável a partir dos testes.

### O que está pendente AGORA

**Publicar o `painel-dre-fiel`.** Não tem migração de banco, então não é preciso
apertar "Aplicar atualizações do banco" junto. Mas acrescenta uma biblioteca ao
serviço (`openpyxl`, para o Excel), o que reinicia os 18 módulos — logo, vale a
pergunta de sempre: **há carga ou sincronização rodando?**

Depois de publicar, o único pedaço que nunca viu dado real é o **bloco de
aportes** — conferir na tela contra o Streamlit antes de considerar fechado.

### A lição de 02/09/2026 — fidelidade vem antes de gosto

A primeira versão do DRE tinha metade da tela antiga. Eu havia decidido por
conta própria que as abas de Receitas, Top Credores e o bloco inteiro de
Aportes não eram necessárias, e reescrevi os rótulos das linhas. O dono abriu
a tela e disse, com razão:

> *"Levei muito tempo pra construir o que tinha, pra simplesmente mudar. Tô
> achando que era melhor ter deixado o Streamlit tal qual estava."*

Ele chegou a pedir para voltar ao Streamlit. **Converter não é redesenhar.**
Quando a tela nova tira coisa da antiga, quem perde é quem já sabia usá-la — e
o ganho técnico não compra isso. O que se pode melhorar é o que ele reclamou:
o filtro de obras com mais de cem itens, que não tinha busca.

Antes de mexer numa tela, abra a original em `referencia_streamlit/` e confira
item por item. O que sair, sai porque **ele** decidiu, não porque pareceu
supérfluo.

### Erro numérico que isso escondeu

Na pressa de simplificar, a linha **"Juros e Multas Pagos"** ficou de fora do
DRE. Não era só uma linha a menos na tela: os encargos sumiam do total de
custos, e o resultado saía maior do que é. Está de volta.

### E o teste que deveria ter pego isso não rodava no PC

Ao mudar o formato do DRE, o teste `test_dre_fecha_de_cima_a_baixo` ficou lendo
o formato antigo e quebrou — mas ele é `@pytest.mark.banco`, e **sem Postgres
local é pulado calado**. A suíte no PC deu tudo verde; quem acusou foi o GitHub
Actions, que sobe o banco.

Duas consequências práticas, que valem para qualquer mudança aqui:

- **Verde no PC não é verde.** São ~158 testes pulados sem banco, e são
  justamente os que olham o SQL. Antes de pedir para publicar, conferir o
  resultado do GitHub Actions do ramo — ou subir o `docker-compose.teste.yml`.
- **Cenário de teste com campo zerado não testa o campo.** O `juros` e a
  `multa` do cenário eram zero em todas as linhas, então o SQL novo dos
  encargos não era exercitado por nenhum teste com banco de verdade. Agora o
  cenário tem encargo pago (entra) e encargo previsto num título em aberto
  (não entra).

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
| Exportação em `.xlsx` (`openpyxl`), **não** CSV | Revertida em 03/09/2026: o relatório tem oito abas, e em CSV isso vira oito arquivos soltos. O dono pediu Excel. Escreve célula a célula, sem `pandas` |
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

## O banco de produção recusou conexão em 03/09/2026 — passou

Na manhã de 03/09 o Postgres respondeu:

    FATAL: role "erp_admin" is not permitted to log in

Não era senha errada: era o servidor recusando o usuário. Na tarde do mesmo dia
o dono confirmou que painel e ERP voltaram a abrir. Fica registrado porque, se
acontecer de novo, **ERP e painel caem juntos** — usam o mesmo banco — e o lugar
de olhar é a instância do Postgres no Render, não a senha.

O que aquele dia deixou pendente continua pendente: o SQL do **bloco de aportes
nunca rodou contra a base real**. Ele passou pelo parser do Postgres, pelo teste
de portabilidade e pelos testes com dublê — nenhum dos três olha o número que
sai.

## O que falta

1. **Rodar o bloco de aportes contra a base real.** O banco já voltou; o que
   falta é abrir a tela publicada e comparar com o Streamlit. É o único pedaço
   novo que ainda não viu dado de verdade, e este módulo já mandou três erros
   de SQL para a produção.
2. **A planilha não bate com o DRE por causa dos encargos.** No Streamlit, a
   aba de despesas por categoria recebia de propósito uma linha
   "Despesas Financeiras / Juros e Multas Pagos", justamente para fechar com o
   DRE. O Excel novo não tem essa linha: a aba "Despesas Categoria" soma menos
   que a aba "DRE", pela diferença dos encargos. É pequeno e é conserto de
   poucas linhas, mas é uma diferença em relação ao original — e o combinado
   aqui é que o que sai, sai porque o dono decidiu.
3. **PDF do DRE.** O gerador original usa `reportlab`, que não está no serviço.
   Daria para refazer com `fpdf2`, que já está — mas é reescrever o relatório.
4. **Mensagem duplicada** na tela de Configurações: o mesmo erro aparece na
   linha "Última atualização" e na caixa vermelha de interrupção.
5. **Cenários da prestação** — comparar duas configurações de rateio lado a lado.
6. **Converter `app/apps/spsbd_app`** (análise de SPs) do mesmo jeito. É
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
