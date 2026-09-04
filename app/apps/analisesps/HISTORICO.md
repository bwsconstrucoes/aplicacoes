# Análise de SPs — onde o trabalho está, decisões, incidentes e o que falta

Este arquivo existe para uma sessão nova pegar o módulo sem repetir o que já
foi discutido e sem repetir o que já deu errado. O `README.md` ao lado explica
**como** ele é; este diz **em que pé está e por quê**. Leia os dois antes de
mexer; atualize este ao encerrar a sessão.

> Reconstruído em 03/09/2026 pela sessão do ERP a partir das mensagens de
> commit do módulo (que são detalhadas) — o chat que fez a conversão, no PC do
> dono, foi fechado antes de escrever isto. O que estava só na conversa e não
> nos commits se perdeu; o que segue é o que os commits permitem afirmar.

---

## Onde o trabalho está

Era um programa em Streamlit no computador do dono, lendo uma base local de
60 MB (59.055 SPs). Virou módulo Flask em `/analisesps`, no serviço que já
existe, com login próprio, dados no Postgres do ERP em schema próprio
(`analisesps`) e carga da planilha SPsBD em processo separado.

**Feito em 02/09/2026, em quatro commits:**

1. `da41ae2` — primeira tela (Solicitações), login com dois perfis, migração
   001, carga em processo separado com retomada por etapas.
2. `a8c3b16` — as outras sete telas: Lote, Relatório, Auditoria, Ratear,
   Bradesco, Agenda, Log; mais a tela de códigos de pagamento (QR Pix e código
   de barras). Migração 002 (agenda, feriados, lote, listas do rateio).
3. `f687e66` — correção: um import "curto" herdado do Streamlit quebrava em
   silêncio a conferência do Bradesco (boleto de 47 dígitos nunca casava).
4. `f58d097` — exportação CSV em todas as telas que geram número de reunião,
   relatório e lote em PDF (fpdf2), ações direto na ficha da SP.

**Estado em 02/09/2026:** 611 testes verdes sem banco (297 deste módulo);
57 com banco rodam só no GitHub Actions. Conferido contra a base real: a soma
das SPs bate na casa do centavo com o Streamlit (R$ 250.061.950,39).

**Em 03/09/2026 a pasta foi enxugada:** sobrou só o que roda no Render. O
Streamlit original saiu daqui — ver "Decisões já tomadas" abaixo.

### O que está pendente AGORA

Respondido pelo dono em **03/09/2026**:

- **Senhas:** cadastradas no Render. ✔
- **Migrações 001 e 002:** **NÃO aplicadas.** O dono achava que sim, mas o
  defeito abaixo prova que não — e explica por quê: a única tela que aplica
  as migrações era a que estourava.
- **Carga da planilha:** nunca rodou. A base está vazia no ar.
- **Streamlit no PC:** continua em uso, e continuará até este módulo estar de
  pé de verdade.

**Verificado em 04/09/2026, com um Postgres de verdade** (descartável, nesta
máquina — a produção não foi tocada), reproduzindo o estado exato da estreia:

- com o código que está **hoje no ar**, Configurações responde **500 "Deu
  erro"**. Confirmado, não deduzido.
- com a correção do ramo, ela responde **200**, lista as duas atualizações
  pendentes e mostra o botão.
- apertando o botão, as **duas migrações aplicam sem erro**, e em seguida as
  **nove telas abrem** com a base ainda vazia.

Ou seja: falta publicar. A correção resolve o impasse e a sequência seguinte
funciona.

**A sequência que falta, nesta ordem:**

1. Publicar a correção do defeito de estreia (abaixo) — sem ela a tela de
   Configurações não abre.
2. Na tela de Configurações, apertar **"Aplicar atualizações do banco"**
   (aplica 001 e 002).
3. Ainda ali, disparar a **Primeira carga**. Demora alguns minutos; pode
   fechar a página.
4. Só então navegar as nove telas com dado real — **ninguém nunca fez isso**.

---

## 04/09/2026 — a volta ao Streamlit

O dono navegou o módulo pela primeira vez e o veredito foi: **"muita coisa foi
mudada do Streamlit sem necessidade; ficaram funções perdidas e nomenclatura
alterada; eu não pedi isso"**. Ele tem razão, e isso muda o critério daqui em
diante:

> **Em dúvida, faça como o Streamlit fazia.** Quem for mexer nesta área tem o
> programa original no histórico do Git (saiu da pasta em 03/09, mas nada se
> perde): `git show 285d236:app/apps/analisesps/app/app.py`. Compare ANTES de
> inventar. O dono trabalhou anos com aquelas telas.

### O achado grave: "Cancelar" queria dizer duas coisas

No Streamlit, **Cancelar SP** abria o formulário público do Pipefy que PEDE o
cancelamento da solicitação. Na conversão virou um botão que grava
`Status Pgt = "Cancelado"` na planilha. Mesma palavra, ação diferente, e o
erro é silencioso: quem queria cancelar a SP marcava a planilha e ia embora
achando que tinha resolvido — enquanto o card seguia vivo no Pipefy.

Corrigido: o botão voltou a ser o formulário do Pipefy, e onde ficam os botões
que escrevem está dito, com todas as letras, que **daqui não se altera o
Pipefy — só a planilha SPsBD**.

### O que foi devolvido ao lugar

| O que era no Streamlit | Como estava | Agora |
|---|---|---|
| Coluna ID = link para o card do Pipefy | link para a ficha interna | link para o card |
| Ficha em **modal** por cima da lista | página inteira; voltar recomeçava tudo | modal no duplo clique na linha |
| Agendamento só com **Validação = "Sim"** | qualquer um agendava qualquer coisa | trava de volta |
| Barra de ações **fixa e única**, para a seleção inteira | uma barra por grupo, cada uma cega para as outras | uma só, colada no alto |
| Sem "Marcar Pago" no Lote | tinha | tirado |
| Agendar / Agendado / Desagendar / Falha Agendar | faltavam no Lote | os quatro, em todo lugar |
| **Enviar Lote** (marcadas → grupo "Novo Lote N") | sumido | de volta |
| **Abrir cards** (todas as marcadas de uma vez) | sumido | de volta |
| **Limpar Pgto** (Status → Pagar, Agendado vazio) | sumido | de volta |
| Links **Consulta** e **Atualizar** do Omie | sumidos | de volta, por variável de ambiente |
| Barra de filtros **única para o programa todo** | só nas Solicitações | Solicitações e Relatório |
| Filtro aplica ao clicar, sem botão | botão "Aplicar filtros" | aplica sozinho, com meio segundo de espera |
| Filtro **guardado** entre sessões | perdido a cada navegação | guardado por pessoa, no banco |
| Cores da tabela (vermelho/azul/roxo/verde/laranja) | paleta nova | as do Streamlit |

### O que o dono pediu de novo (não vem do Streamlit)

- **Nome no login.** O módulo não tinha noção de pessoa; o lote era um só e o
  registro de alterações sabia apenas o perfil. Agora cada um informa o nome ao
  entrar. **O nome não autentica**: quem manda continua sendo a senha. Ele
  serve para separar o lote, guardar os filtros e assinar o log.
- **Lote por pessoa.** Reverte a decisão de 02/09 ("o lote é compartilhado").
  O lote que existia não foi apagado: virou o "lote de antes", e a tela oferece
  trazê-lo por botão para quem ainda não tem o seu.
- **KPIs no cabeçalho de cada grupo do Lote**, com o total em destaque.
- **O total da seleção no meio da barra, grande.** É o número que decide se a
  remessa vai; estava num canto em letra miúda.

### Migração 003

`003_pessoa_e_preferencias.sql`: tabela de preferências, o lote passa a ser
chaveado por pessoa (a linha única `id = 1` vira a pessoa `''`), e o log ganha
a coluna `pessoa`. **Precisa do botão "Aplicar atualizações do banco"** logo
depois de publicar.

### Uma variável nova no Render

`ANALISESPS_HOOK_OMIE` — o endereço do gancho do Make que o Streamlit usava
nos botões **Consulta** e **Atualizar** do Omie. **Não foi copiado para o
código de propósito**: é um endereço que dispara ação, e endereço assim não se
versiona. Sem a variável os dois botões simplesmente não aparecem — melhor do
que aparecerem quebrados. O valor está no `app.py` do Streamlit, na pasta que
foi movida para fora do repositório.

### O que ainda NÃO voltou

- **Validar** (gravar Validação = "Sim"). No Streamlit exigia uma senha
  própria (`SENHA_VALIDACAO`, vinda da aba de Credenciais) e escreve numa
  coluna que hoje é somente leitura. Ficou de fora desta entrega: mexer no
  conjunto de colunas graváveis merece uma conversa antes.
- **Remover Risco**, **Gerar BeeVale**, **Cancelar SP no Pipefy por dentro**,
  **reenviar comprovante por e-mail**, **escolher as colunas da tabela**.
- **Auto-atualizar a cada 90s.**

---

## Regras que não se discutem

### 1. Nada de abrir a base inteira em memória
Foi o número que decidiu a conversão: cada pessoa no Streamlit segurava
162 MB (pico 195 MB). Aqui quem soma é o Postgres; a tela recebe 200 linhas.
As somas do relatório e as sete checagens da auditoria são SQL.

### 2. Trabalho longo roda em processo separado, nunca dentro do gunicorn
`executar_sync.py`. O `--max-requests` do gunicorn matou três cargas do
painel por isso. **Publicar na `main` reinicia o serviço e mata a
sincronização em curso — perguntar ao dono antes de juntar.**

### 3. Nenhum módulo é importado pelo nome curto, e nada de pandas
Há teste que varre o pacote: import só pelo caminho completo; proibidos
`pandas`, `streamlit`, `numpy`, `altair`, `reportlab`, `openpyxl` (o serviço
não os tem); tudo que se importa tem de estar no `requirements.txt`.

### 4. Autorização padrão NEGAR
Rota que não declara o que exige é recusada. A resposta a uma escrita sem
alçada é sempre 403 — antes de qualquer outra checagem (defeito corrigido no
Lote).

### 5. Toda tela abre com recado quando o banco cai
Nunca 500. Há teste para as nove telas. Downloads leem o banco **antes** de
começar a mandar o arquivo — senão sai HTTP 200 com arquivo pela metade.

### 6. PDF é fpdf2 (não `fpdf` 1.7), e o texto passa por conversão para latin-1
O fpdf2 com fonte embutida estoura no meio da geração com travessão ou aspas
curvas. Todo texto é convertido antes, no mesmo caminho do emissaonf.

### 7. A produção não é alcançável a partir dos testes
Mesma regra do ERP e do painel.

---

## Decisões já tomadas — não reabrir sem motivo novo

- **Blueprint no monorepo, não serviço separado.** Um segundo serviço seria
  assinatura à parte, e o Streamlit não cabe em memória com quatro pessoas.
- **O lote é compartilhado** e a tela diz isso: duas pessoas veem o mesmo
  lote; a segunda a salvar sobrescreve, e a tela mostra quem salvou e quando.
- **Quatro módulos do Streamlit reaproveitados quase inteiros** (QR Pix/BR
  Code, código de barras, conferência do Bradesco, matemática do rateio) e a
  matemática de calendário da agenda sem alteração.
- **CSV, não `.xlsx`**; gráficos em CSS, sem biblioteca (o Plotly custava
  3 MB por tela).
- **Datas:** a conversão recupera 857 autorizações que apareciam vazias
  (1.664 SPs com data em duplicidade e quebra de linha) e recusa cinco com ano
  digitado errado (202, 203, 204, 260, 2925).
- **A pasta do módulo só guarda o que roda no Render** (decidido pelo dono em
  03/09/2026). Saíram daqui: o Streamlit original (`app/`), os quatro atalhos
  `.bat` que o abriam, a base local `spsbd_cache.db` de 60 MB, um ambiente
  Python obsoleto de 35 MB e duas pastas vazias. Saiu também o `render.yaml`,
  que já se declarava inerte no próprio cabeçalho.

  **Movido, não apagado.** O Streamlit ainda é o que o dono usa no dia a dia
  até o módulo online estar de pé, e os `.bat` procuram tudo ao lado deles —
  então o conjunto inteiro foi para uma pasta irmã fora do repositório,
  `analise-sps-streamlit-pc/`, e continua funcionando como antes. Nada do
  serviço importava aquilo: nem o código, nem a suíte, nem o Render.

  A base de 60 MB é regenerável — sai da planilha SPsBD. Quando o dono parar
  de usar o Streamlit, a pasta inteira pode ser apagada.

- **Cancelar SP no Pipefy** e **gerar BeeVale**: ações sem volta, e o BeeVale
  depende de um Shared Drive (erro 403 de cota da service account).
- **Enviar comprovante por e-mail**: depende de SMTP no serviço.
- **Excel**: exigiria biblioteca nova.

## Incidentes

- **02/09 — import silencioso quebrado** (`f687e66`): o `bradesco.py`
  importava `pagamentos` pelo nome curto dentro de um try/except; a falha não
  aparecia e o boleto de 47 dígitos nunca casava com a SP.
- **02/09 — `fpdf` antigo no PC** mascarava diferenças com o `fpdf2` do
  serviço. Ambiente local foi alinhado.
- **03/09 — o impasse de estreia: a tela que conserta era a tela quebrada.**
  Na primeira vez que o dono abriu o módulo no ar, Solicitações mostrou "a
  base ainda não foi carregada" e Configurações estourou com "Deu erro".

  Causa: `tarefas.ultima_concluida()` lia `analisesps.execucoes` **sem
  proteção**, e essa tabela só nasce na migração 001. Com o banco de pé e as
  migrações por aplicar — o estado exato de qualquer estreia — a leitura
  estourava e derrubava a tela. E era a **única** tela com o botão que aplica
  as migrações: sem ela, não havia como sair do estado.

  Por que passou por 611 testes: os testes de Configurações ou derrubavam o
  banco **inteiro** (aí `listar_estado()` falha primeiro, e a leitura da
  última execução nem é tentada), ou dublavam `ultima_concluida`. Nenhum
  cobria o meio-termo — e `listar_estado()` sobrevive sem migração nenhuma
  porque ela mesma cria o schema e a tabela de controle.

  A lição, que vale para o módulo todo: **"banco fora do ar" e "banco de pé,
  estrutura ainda não criada" são dois cenários diferentes**, e o segundo é o
  que todo mundo encontra no primeiro dia. Corrigido protegendo
  `ultima_concluida()` (igual à `estado()` ao lado) e a leitura de
  `analisesps.meta` em `base_carregada()`, que tinha o mesmo defeito. Um
  teste novo monta a tela nesse estado.

- **04/09 — a tela afirmava o que não tinha conseguido perguntar.** Com o
  banco fora de alcance, Configurações mostrava o recado de erro certo e, logo
  abaixo, "0 aplicada(s), 0 pendente(s) — **O banco está em dia**" e "A base:
  **vazia**". As duas frases dizem o contrário do que estava acontecendo:
  quem lê conclui que a estrutura está pronta e que não há SPs, e para de
  procurar a causa no lugar certo. Agora as duas dizem **"não deu para
  saber"**, e a estrutura ganha um aviso explicando que a pergunta não chegou
  a ser feita. `base_carregada()` passa a devolver `desconhecida`, que separa
  "consultei e deu zero" de "não consegui consultar". Teste novo trava as
  duas frases; conferido que ele falha sem a correção.

## Coisas pequenas que mordem

- As telas **quase não foram vistas num navegador**. O chat que as fez não
  conseguiu conectar a extensão e as conferiu só estruturalmente. Em 03/09
  duas foram abertas de verdade, num navegador, contra a aplicação rodando no
  PC: a de **entrada** (o logo, o campo, o texto de ajuda e a folha de estilo
  carregam; o login funciona) e a de **Configurações** com o banco fora de
  alcance (o recado aparece no lugar certo, sem estourar). **As outras sete
  continuam sem nenhuma navegação real**, e nenhuma foi vista com dado de
  verdade — não há dado no ar ainda.
- ~~Na tela de Configurações com o banco inalcançável, o resumo da estrutura
  diz "0 aplicada(s), 0 pendente(s) — O banco está em dia".~~ **Corrigido em
  04/09/2026** (ver abaixo).
- Os 57 testes com banco só rodam no GitHub Actions. Sem `ERP_TEST_DATABASE_URL`
  são pulados.
