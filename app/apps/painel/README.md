# Painel Financeiro OMIE

Blueprint do monorepo, em **`/painel`**. Mostra resultado, caixa e DRE a partir
de uma cópia da base financeira do OMIE.

Era um programa em Streamlit que rodava no computador do dono, lendo arquivos
de uma pasta de 153 MB. Isto aqui é a mesma coisa online, no serviço que já
existe — sem Streamlit, sem arquivo, com login.

## Como está montado

```
web.py           rotas e telas
auth.py          login por senha (padrão NEGAR)
consultas.py     as perguntas que as telas fazem ao banco
graficos.py      geometria dos gráficos (SVG, sem biblioteca)
db.py            conexão com o Postgres + adaptador de compatibilidade
tarefas.py       a atualização em segundo plano
migracoes/       .sql numerados; aplicados por botão, nunca no boot
sync/
  omie_client.py cliente HTTP da API do OMIE
  espelho.py     baixa títulos, movimentos e catálogos
  projetos.py    de-para obra -> projeto (planilha "C. Diários")
  fato.py        transforma o espelho na tabela que as telas leem
```

## Onde os dados moram

No **mesmo Postgres do ERP** (`DATABASE_URL`), mas num **schema separado**
chamado `painel`. Isso não é preciosismo: o ERP tem tabelas `titulos`,
`rateios` e `categorias`, e o espelho do OMIE tem tabelas com esses mesmos
nomes e significado completamente diferente. Schemas separados fazem as duas
conviverem sem renomear nada e sem risco de uma escrita do painel encostar em
dado do ERP.

Nada de arquivo: o disco do Render é apagado a cada reinício, e a configuração
da prestação de contas (sócios, percentuais, regras de rateio) não é
regenerável — sumiria junto.

## As duas tabelas que importam

- **`fato`** — uma linha por pedaço de título já apropriado a uma obra
  (~185 mil). É o que as telas somam.
- **`fato_recebimentos`** — uma linha por entrada de caixa, com data e valor
  exatos. Abre cada medição nos recebimentos que a quitaram.

As duas são **derivadas**: apagadas e refeitas a cada atualização, a partir do
espelho. Nunca editar à mão.

## Por que as telas não abrem a base inteira

O painel antigo abria um arquivo de 4 MB que virava **179 MB na memória**. A
instância do Render tem 2 GB divididos com 15 módulos e já teve crise de
memória (`CONTEXTO.md` §9). Aqui quem soma é o Postgres; a tela recebe a dezena
de números que vai mostrar.

Pelo mesmo motivo:
- a reconstrução do fato percorre os títulos em **blocos**, com cursor do lado
  do servidor — o pico fica em poucos MB, não importa o tamanho da base;
- os gráficos são **SVG desenhado na página**, sem biblioteca (o Plotly do
  painel antigo custava mais de 3 MB de JavaScript por abertura de tela);
- `pandas` **não** é dependência do painel. A única parte que o usava mexe em
  poucos milhares de linhas e foi feita em Python puro.

## Migrações

Arquivos `.sql` numerados em `migracoes/`, aplicados pelo botão **"Aplicar
atualizações do banco"** na tela de Configurações. Tabela de controle:
`painel._migracoes`. Cada arquivo roda na própria transação.

**Nunca no start do gunicorn** — mesma razão do ERP: uma migração com defeito no
boot derrubaria o monorepo inteiro, não só o painel.

## A atualização da base

Substitui o `atualiza_omie.bat` que rodava no PC. Dois disparos, o mesmo código:

- **Automático** — o cron-job.org chama `POST /painel/api/sincronizar` toda
  madrugada, com `{"secret": "...", "modo": "rapida"}`. Mesmo arranjo que o
  `baixabradesco` já usa. Sem agendador dentro do processo: gastaria memória e
  não sobreviveria a um reinício.
- **Manual** — botão na tela de Configurações.

Modos: `rapida` (o que mudou), `completa` (inclui a varredura de títulos
excluídos no OMIE — lenta, uma vez por semana), `so_numeros` (só refaz o fato),
`carga_inicial` (baixa tudo; horas).

Uma de cada vez: a segunda chamada é recusada em vez de duplicar o trabalho.

## Variáveis de ambiente

| Variável | Para quê |
|---|---|
| `PAINEL_SENHA` | senha de entrada. **Sem ela ninguém entra** — falha fechado |
| `PAINEL_SECRET` | autoriza a chamada do agendador |
| `PAINEL_SHEET_PROJETOS` | id da planilha "Bases de Dados Pipefy" |
| `OMIE_BWS_APP_KEY` / `OMIE_BWS_APP_SECRET` | acesso ao OMIE (as mesmas do `baixabradesco`) |
| `DATABASE_URL` | Postgres — já existe, é o do ERP |
| `GOOGLE_CREDENTIALS_BASE64` | leitura da planilha — já existe |

## O que ainda não foi convertido

`referencia_streamlit/` guarda as telas originais. Continuam sendo a fonte da
verdade das regras enquanto a conversão não termina:

- Fluxo de Caixa (mês a mês)
- Resultado por Obra/Projeto
- Comprometido vs Executado
- Necessidade de Caixa
- Prestação de Contas (rateio administrativo e divisão entre sócios)
- Exportações em Excel e o PDF do DRE

Cada tela convertida deve bater **número por número** com a original.
