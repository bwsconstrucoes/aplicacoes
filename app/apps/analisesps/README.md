# Análise de SPs

Blueprint do monorepo, em **`/analisesps`**. As solicitações de pagamento da
BWS: o que está a pagar, o que já foi pago, o que tem pendência.

Era um programa em Streamlit que rodava no computador do dono, lendo uma base
local de 60 MB. Isto aqui é a mesma coisa online, no serviço que já existe —
sem Streamlit, sem arquivo, com login.

## Por que virou blueprint, e não um serviço só dele

Havia um `render.yaml` propondo um serviço separado, com disco pago, mantendo
o Streamlit. O número que decidiu: **cada pessoa conectada ao Streamlit segura
162 MB de tabela na memória do servidor** — medido sobre as 59.055 SPs reais,
com pico de 195 MB. É por pessoa, porque o Streamlit dá a cada sessão a sua
própria cópia.

Com quatro pessoas isso não cabe, e um serviço novo ainda seria assinatura à
parte: o plano de 2 GB que a empresa paga vale para o serviço do monorepo, não
para outro. Aqui quem soma é o Postgres, e a tela recebe as 200 linhas da
página. O custo extra é zero.

## Como está montado

```
web.py             rotas e telas
auth.py            login por senha, dois perfis, padrão NEGAR
consultas.py       as perguntas que as telas fazem ao banco
auditoria.py       as sete checagens da tela de Auditoria
lote.py            o lote de trabalho: agrupar, extrair SPs, guardar
agenda.py          calendário de compromissos que se repetem
colunas.py         o mapeamento da aba SPsBD (A:AL) — fonte única
formatos.py        "6.750,00" e "31/12/2026" <-> número e data
credenciais.py     Google pelo padrão do emissaonf
db.py              conexão com o Postgres + adaptador de compatibilidade
horario.py         hora de Brasília (o servidor roda em UTC)
sincronizacao.py   a ponte com a planilha, nos dois sentidos
tarefas.py         a carga em segundo plano, com andamento e retomada
executar_sync.py   o processo separado que faz o trabalho longo
migracoes/         .sql numerados; aplicados por botão, nunca no boot

reaproveitados do Streamlit, quase sem mudança:
  pagamentos.py    QR Pix e código de barras
  pix_brcode.py    montagem do BR Code e o dígito verificador
  bradesco.py      conferência do extrato (só a varredura mudou)
  rateio.py        o rateio que fecha 100% com o menor erro

app/               o Streamlit ORIGINAL — não sobe para o serviço
```

A pasta `app/` continua ali, intocada, e é o que o dono roda no computador
dele. Ela não é importada por nada daqui e não tem `__init__.py`, então não
atrapalha. Fica até a conversão terminar.

## Os dois perfis

| Perfil | O que faz | Variável no Render |
|---|---|---|
| **Consulta** | vê tudo e exporta; não altera nada | `ANALISESPS_SENHA_CONSULTA` |
| **Operador** | tudo o que o Consulta faz, mais alterar | `ANALISESPS_SENHA_OPERADOR` |

Perfil sem senha configurada não existe — ninguém entra por ele. **Sem nenhuma
das duas, o módulo não abre para ninguém.** Falha fechado, de propósito: são os
pagamentos da empresa.

Não há cadastro de usuários porque são até quatro pessoas e o módulo tem prazo
de validade — o ERP vai substituí-lo. A consequência, dita com clareza porque
um dia vai incomodar: **o registro de alterações sabe que PERFIL mexeu, não
qual PESSOA.** Quando isso passar a importar, o lugar certo é o cadastro de
usuários do ERP, não um cadastro novo aqui.

## Toda rota declara o que exige

`@exige_consulta`, `@exige_operador` ou `@publica("motivo")`. **Rota que
esquece de declarar é recusada pelo guarda, não liberada** — a regra do ERP,
pela mesma razão: o esquecimento é o modo de falha mais comum.

Isso não é teoria. Na primeira execução o guarda bloqueou a própria folha de
estilo, que é o único endpoint criado pelo Flask e não tinha onde receber a
declaração. Está liberada nominalmente em `auth.py`, com o motivo escrito.

## Onde os dados moram

No **mesmo Postgres do ERP** (`DATABASE_URL`), num **schema separado** chamado
`analisesps`. O ERP tem tabelas de nome genérico (`titulos`, `categorias`,
`rateios`) e a colisão seria real. Um teste confere que nenhuma tabela ou
índice é criado fora do schema.

A tabela `sps` guarda cada coluna **duas vezes**: o texto que a planilha manda
("6.750,00", "31/12/2026") e a mesma informação já convertida para número e
data. A convertida é o que filtra, ordena e soma; a crua fica porque é a
verdade da planilha, para conferir quando a conversão errar num caso esquisito.

A base é regenerável — sai da planilha. O que **não** é regenerável, e por isso
mora no banco e não em arquivo, é a **fila de escrita** e o **registro de
alterações**. O disco do Render é apagado a cada reinício.

## O caminho de uma alteração

Quando o operador marca "Pago" em vinte SPs:

1. grava no banco na hora — quem está na tela vê o efeito imediatamente;
2. põe cada célula na **fila de escrita** para a planilha;
3. registra no log, com o valor anterior;
4. dispara o processo separado que devolve para a planilha.

Se a internet cair no passo 4, a alteração continua na fila e sobe sozinha
depois. Nada se perde. Reescrever a mesma célula substitui o valor pendente —
sem isso, duas trocas seguidas viriam como duas gravações, e a antiga poderia
chegar depois da nova e desfazê-la.

Só duas colunas são alteráveis por aqui: **Status Pgt** e **Agendado**. A
planilha é dona do resto. Tentar alterar outra é recusado.

## A carga da planilha

`sincronizacao.py` faz três trabalhos, todos no **processo separado**:

- **carga inicial** — as 59 mil SPs, em blocos de 5.000, retomável;
- **sincronização** — lê só as colunas A e V, descobre quem mudou pelo carimbo
  e busca apenas essas linhas. É o que faz a atualização custar segundos;
- **fila** — devolve para a planilha o que foi alterado nas telas.

**Nunca dentro do gunicorn.** O serviço sobe com `--workers 1
--max-requests 150`: o processo se recicla a cada ~150 requisições e leva junto
qualquer thread de fundo. No painel isso matou três cargas seguidas sem que a
causa aparecesse. Aqui a carga roda destacada, o andamento vai para o banco, e
a tela lê de lá — por isso ela continua certa depois de um reinício, e de
outro aparelho.

**Memória:** a planilha inteira nunca é aberta de uma vez. `get_all_values()`
numa aba de 59 mil linhas por 38 colunas devolve mais de dois milhões de
textos. Os blocos mantêm o pico em poucos MB.

## Credenciais

Pelo padrão do `emissaonf`, sem inventar nada: a service account vem de
`GOOGLE_CREDENTIALS_BASE64`, que **já existe no Render**, e os tokens saem da
aba "Credenciais" da mesma planilha que os outros módulos usam. Variável de
ambiente ganha da planilha.

Nenhum arquivo `credenciais.json` no servidor, nenhum "Secret File". O
contêiner do Render é apagado a cada reinício: um arquivo de credencial ali
seria ou perdido, ou versionado por engano.

## Variáveis de ambiente

| Variável | Para quê |
|---|---|
| `ANALISESPS_SENHA_OPERADOR` | senha de quem altera. **Sem ela, ninguém opera** |
| `ANALISESPS_SENHA_CONSULTA` | senha de quem só olha. **Sem ela, ninguém consulta** |
| `ANALISESPS_SECRET` | autoriza a chamada do agendador |
| `DATABASE_URL` | Postgres — já existe, é o do ERP |
| `GOOGLE_CREDENTIALS_BASE64` | leitura da planilha — já existe |

Opcionais, com valor embutido: `ANALISESPS_SHEET_SPS`,
`ANALISESPS_SHEET_FISCAL`, `ANALISESPS_SHEET_CREDENCIAIS`.

## Qual versão está no ar

`GET /analisesps/saude` responde o commit publicado (`RENDER_GIT_COMMIT`) e
quantas senhas estão configuradas. Serve para responder "a correção já subiu?"
sem abrir nada nem perguntar a ninguém. Não devolve dado da empresa.

## O que os testes cobrem

- **acesso** — o inventário que quebra quando uma rota esquece de declarar; os
  dois perfis; falha fechada sem senha; o destino do login não apontar para
  fora do módulo;
- **formatos** — a conversão comparada, SP a SP, com a do Streamlit sobre a
  base real: os valores bateram na casa do centavo (R$ 250.061.950,39 nos
  dois);
- **filtros** — a montagem do SQL: todo valor vindo de fora entra como
  parâmetro, e a coluna dos filtros é escolhida por nós, não por quem chama;
- **sql portável** — construção de SQLite que sobrou, e o `%` sobrevivendo à
  tradução dos marcadores (um `LIKE '%falha%'` mal traduzido volta vazio, sem
  erro nenhum — silencioso, que é o pior tipo);
- **telas** — cada página monta, com os números em português;
- **banco** (`@pytest.mark.banco`) — o SQL contra um Postgres de verdade: as
  migrações, o `ON CONFLICT`, as cinco situações, o fuso de Brasília, a
  paginação, a fila, as somas do relatório, as sete checagens da auditoria, o
  lote e a agenda;
- **lote, rateio e agenda** — o agrupamento do lote, a extração de SPs de
  mensagens, o rateio fechando 100% em qualquer combinação, e o calendário:
  Páscoa, feriados móveis, ajuste para dia útil e o "dia 31 = último dia do
  mês".

E dois testes que valem por muitos: **nenhuma tela pode devolver erro 500 com o
banco fora do ar** (uma tela que estoura é justamente a que ninguém consegue
usar para descobrir o que houve), e **toda tela precisa dizer o que houve** em
vez de abrir vazia — abrir vazia faria alguém concluir que não há contas a
pagar.

## Duas coisas que a base real ensinou

**1.664 SPs** têm a data de autorização gravada em duplicidade, separada por
quebra de linha (`24/07/2024\n24/07/2024`) — alguma automação escreveu duas
vezes. O conversor lê a primeira metade; sem isso, essas 1.664 autorizações
apareceriam em branco. **857 datas** que o Streamlit hoje mostra vazias passam
a ser lidas.

**Cinco SPs** têm o ano digitado errado na planilha: 202, 203, 204, 260, 2925.
São recusadas de propósito. Um vencimento no ano 202 encabeça qualquer lista
ordenada por data, e um no ano 2925 nunca vence — os dois envenenariam todo
filtro por período sem ninguém notar. Recusadas, aparecem em branco: visível, e
cobrável de quem preencheu.

## As telas

| Tela | O que responde |
|---|---|
| Solicitações | a lista, com todos os filtros e as ações em lote |
| Lote | a remessa que está sendo tratada agora, em grupos |
| Relatório | quanto, por obra, projeto, tipo, conta e credor |
| Auditoria | sete checagens do que está errado na base |
| Ratear | o JSON que atualiza o título no Omie |
| Bradesco | o extrato colado, cruzado com as SPs |
| Agenda | compromissos que se repetem, já ajustados a dia útil |
| Log | toda alteração feita por aqui, e se já subiu |
| Configurações | migrações do banco e a sincronização |

Mais a **ficha de cada SP** e a tela de **códigos de pagamento**, que monta o
QR Pix ou o código de barras das SPs marcadas — substitui abrir card por card
no Pipefy para copiar a chave.

## O que ficou de fora, e por quê

- **Cancelar SP no Pipefy** e **gerar BeeVale**. As duas conversam com o Pipefy
  e com o Google Drive escrevendo, não lendo. São ações sem volta, e o BeeVale
  ainda tem a pendência do Drive (erro 403 de cota da service account, que
  precisa de um Shared Drive). Ficam para quando isso estiver resolvido.
- **Relatório em PDF e em Excel.** A exportação é CSV, pela regra da casa de
  não acrescentar dependência sem combinar. O serviço tem `fpdf2`, então o PDF
  é possível — mas é reescrever o relatório do zero.
- **Enviar comprovante por e-mail.** Depende de SMTP configurado no serviço.

O Streamlit continua rodando no computador do dono enquanto isso, intocado.
