# Folha de pagamento — levantamento do processo atual (26/09/2026)

Este documento existe porque o chat não é memória: o repositório é. Ele registra o
que foi **lido de ponta a ponta** dos arquivos que o dono entregou em 26/09/2026,
o que ficou **entendido**, o que é **suposição** e o que **só ele pode responder**.

Quem começar uma sessão da folha de pagamento lê isto primeiro.

> ⚠️ **Nada aqui é código pronto.** Nenhuma linha foi escrita para o sistema ainda.
> O pedido dele foi: *"faça uma varredura criteriosa e profunda nos arquivos que eu
> lhe mandei (…) e não se acanhe em me perguntar"*.

---

## 1. O que o processo faz hoje, em uma frase

A contabilidade (externa) manda a **Folha Sintética**; o DP cruza aquilo com o
**cadastro de colaboradores** e com o **ponto**; disso sai **um arquivo de
pagamento no padrão SomaPay** (nome, CPF, valor), **dois cards no Pipefy** (uma
Despesa com Colaboradores e uma Solicitação de Transferência de Recursos por conta
corrente) e uma **planilha de análise** que serve de prova do que está sendo pago.

Pagamento é por **quinzena**: dia 1 ao 15, e 16 ao fim do mês.

---

## 2. As fontes de dados — o que cada uma entrega

### 2.1 A Folha Sintética (contabilidade → arquivo `.xls`)

Arquivo real lido: `Folha Sintética - Adiantamento de Folha`, mês 08/2026,
gerado pelo **Fortes Pessoal 8.26.0**. É um `.xls` binário antigo (BIFF, não XML;
precisa de `xlrd`, não de `openpyxl`).

Formato: relatório impresso, não tabela.

```
Folha Sintética - Adiantamento de Folha        : 1      ← número da PÁGINA
Empresa: BWS CONSTRUCOES LTDA - CNPJ: ...      Fortes Pessoal 8.26.0
Mês/Ano: 08/2026
Código | Empregado | (vazio) | (vazio) | Líquido
001 - CONSTRUTORA                                       ← cabeçalho de FILIAL
000013 | GERLANIO GOMES LIMA |  |  | 1198.84
...
Total: 001 - CONSTRUTORA ...                   5990.68
090 - OBRA ESTADIOITA CONST ESTADIO ITAITINGA
...
Total: Geral (507 Empregado(s))                445199.96
Fim
```

O que isso significa para o leitor que vamos escrever:

- **O único identificador da pessoa é o `Código` de 6 dígitos com zeros à
  esquerda — o ID Fortes. Não vem CPF.** É por isso que o cruzamento com o
  cadastro é obrigatório, e é por isso que colaborador sem ID Fortes no cadastro
  **não pode** ser resolvido por nome: homônimo paga a pessoa errada.
- O relatório é **paginado**: a cada ~64 linhas repete o cabeçalho
  (`Folha Sintética…`, `Empresa:`, `Código|Empregado|Líquido`) e **repete o
  cabeçalho da filial** quando ela atravessa a quebra de página. O leitor tem de
  ignorar repetição de cabeçalho sem perder linha nenhuma.
- A filial (`001 - CONSTRUTORA`, `090 - OBRA ESTADIOITA…`) é a **obra do
  registro na contabilidade**, e **não** necessariamente onde a pessoa trabalhou.
  A apropriação por obra de verdade vem do ponto. Guardar as duas e comparar é
  de graça e vale um alerta (ver §6).

#### ⚠️ Uma divergência DENTRO do próprio arquivo, que precisa de resposta

| O que | Pessoas | Valor |
|---|---|---|
| Somando as linhas de empregado | **491** | **430.129,75** |
| Somando os `Total:` de cada filial | (47 filiais) | **430.129,75** |
| O que o rodapé declara: `Total: Geral (507 Empregado(s))` | **507** | **445.199,96** |

As linhas detalhadas e os totais por filial batem **na casa do centavo** entre si.
O **rodapé do próprio relatório não bate** com eles: sobram 16 pessoas e
R$ 15.070,21.

Hipótese (NÃO confirmada): o rodapé conta a folha inteira e o corpo lista só quem
tem adiantamento a receber. **Isto é pergunta para o dono** (§7, P1), e a resposta
decide qual número o sistema usa como conferência de fechamento. Enquanto não
houver resposta, a conferência segura é *soma das linhas == soma dos totais por
filial*, e o `Total: Geral` entra no relatório como informação, não como trava.

### 2.2 O ponto (Mobponto, três endpoints)

Base: `https://www.mobponto.com.br/ponto/api/endpoint.php`

> ⚠️ **As credenciais estão hardcoded nos Apps Script** (um header `Authorization:
> Basic` e uma `api-key`). Elas **não entram neste repositório** — vão para
> variáveis de ambiente (`MOBPONTO_*`) e valem a mesma regra do certificado A1:
> não passam pelo chat. Ver §8.

| `type_data` | Parâmetros | O que devolve |
|---|---|---|
| `FOLHA_BWS_EXCEL` | `status=false`, `mes`, `ano`, `pagina` | **o ponto dia a dia**, paginado (`result.total_paginas`, `result.funcionarios[]`) |
| `REL_PRESENCA_BWS` | `tp_relatorio=M`, `dia_inicial=1`, `dia_final`, `mes`, `ano` | **resumo de presença** do mês atual e do anterior, por pessoa |
| `FUNCIONARIOS` | — | o cadastro do lado do Mobponto |

**O ponto dia a dia** (`FOLHA_BWS_EXCEL`) — uma linha por pessoa por dia:

```
cpf, nome, data, dia_semana,
obra_entrada, obra_almoco, obra_retorno, obra_saida,
hr_entrada, hr_almoco, hr_retorno, hr_saida,
desc_falta, totalHrs, saldo, ad_noturno, presenca_ausencia
```

- **São QUATRO marcações por dia, cada uma com a SUA obra.** É exatamente o que o
  dono descreveu: a obra do dia é a que mais aparece entre as quatro. Três numa
  obra e uma em outra → vale a das três. **O critério de desempate 2×2 é pergunta
  aberta** (§7, P5).
- `presenca_ausencia` traz `PRESENÇA`, `FERIADO`, `COMPENSAÇÃO`, vazio (fim de
  semana) e `PRESENÇA -[motivo do ajuste…]` quando houve ajuste manual de ponto.
- `desc_falta` traz a descrição da falta.
- **Dia sem falta E sem marcação existe** e é o caso que o dono levantou: ou a
  pessoa esqueceu de bater, ou quem devia lançar a falta não lançou. Não é
  presença nem falta — é *indefinido*, e merece alerta próprio.
- A pessoa é identificada por **CPF**. A folha da contabilidade vem por **ID
  Fortes**. O cadastro é a ponte.

Volume medido nas células de controle da planilha: **18.570 linhas** no mês
corrente, **20.584** no anterior, **24.893** em janeiro. Ou seja ~500 pessoas ×
~30 dias + ajustes. Guardar 12 meses fica na ordem de 250 mil linhas — tranquilo
para o Postgres, desde que a carga seja incremental e por competência.

**O resumo de presença** (`REL_PRESENCA_BWS`) — uma linha por pessoa:

```
cpf, nome,
mes_anterior.desc, mes_anterior.qtde, mes_anterior.local,
mes_atual.desc,    mes_atual.qtde,    mes_atual.local,
mudou
```

`qtde` é **quantidade de marcações**, não de dias. `local` é a obra. `mudou` é
SIM/NÃO (trocou de obra entre os dois meses). `qtde` **vem vazia** quando a pessoa
não bateu ponto nenhum naquele mês — é o sinal de "sem presença".

### 2.3 O cadastro de colaboradores (planilha `Registro de Colaboradores`)

11,9 MB, aba `Dados Documentos`, ~75 colunas. Espelha os cards do Pipefy.

**Do que a folha precisa (e só disso):**

| Coluna | Para quê |
|---|---|
| `ID Fortes` | **a chave** que casa com a folha da contabilidade |
| `CPF (Cadastro de Pessoa Física)` | casa com o ponto, e vai no arquivo SomaPay |
| `Nome Completo` | vai no arquivo SomaPay |
| `Nº Registro Pipefy` | abrir o card da pessoa |
| `Matrícula` | conferência |
| `Fase Atual` | **detecta desligado / em processo de desligamento** |
| `Data de Admissão`, `Data de Início` | pagamento antes de admitir é erro |
| `Data do Aviso Prévio`, `Último dia Trabalhado`, `Data de Saída` | pagamento depois de sair é erro |
| `Cargo`, `Código da Obra` | conferência contra a obra do ponto |
| `Salário`, `Valor Diária` | ordem de grandeza (crítica de valor fora da curva) |
| `Tipo de Cadastro`, `Tipo de Contrato`, `Convenção` | separa CTPS de diarista etc. |
| `Recebe Gratificação Mensal`, `Valor da Gratificação`, `Valor do Mês` | outras folhas |
| `Recebe Parcela Única`, `Recebe via SP`, `Nome Credor`, `Pix CNPJ` | quem não entra no arquivo SomaPay |
| `Chave Pix Tipo`, `Chave Pix` | pagamento fora do SomaPay |
| `Categoria/Valor Auxílio Alimentação`, `Categoria/Valor Auxílio Transporte` | as outras folhas |
| `CNPJ Admissão` | qual empresa do grupo admitiu |

Tem também `ID Telegram` — serve para avisar a pessoa, se um dia quisermos.

### 2.4 O espelho do OMIE (planilha `Bases de Dados Pipefy`)

Primeira aba `ClientesOmie`: `codigo_cliente_omie`, `codigo_cliente_integracao`,
`cnpj_cpf`, `nome_fantasia`, `tags.tag` (Fornecedor / Cliente / Colaborador).

**Boa notícia: isto já existe no nosso banco.** O painel OMIE carrega
`painel.clientes`, `painel.cat` (plano financeiro) e `painel.contas_correntes`
toda noite, e o Análise de SPs já lê esses espelhos. Não precisamos de mais uma
cópia nem de mais uma credencial.

---

## 3. O que a planilha `Folha de Pagamento - Fortes` faz hoje

Duas abas iguais, `Quinzena` (dia 1 a 15) e `Fim de Mês` (16 ao fim). Também
existem `CTPS`, `Diaristas`, `GM`, `DC`, `Alimentação`, `Transporte`,
`Parcela 1`, `Parcela 2` — uma por processo de folha.

Estrutura reconstruída a partir do relatório de análise e das faixas que o Make
lê (ver §4):

- **Bloco da esquerda** — a Folha Sintética colada, e ao lado dela o cruzamento
  por pessoa: `ID Fortes | Colaborador | Líquido | CPF | Dias | Valor x Dia |
  Valor | Função | Centro de Custo`, mais um marcador `Pagar QZ` / `Não Pagar`
  (coluna M na aba Quinzena, M4:M917; na Fim de Mês M4:M1667) — é o que os botões
  do Apps Script preenchem em massa.
- **`Valor x Dia`** é o líquido da pessoa dividido pelos dias de presença: é com
  ele que o valor é rateado para cada dia e cada dia vai para a obra do ponto.
- **Bloco do meio** — o rateio somado por centro de custo, com o código do
  departamento no OMIE ao lado.
- **Bloco da direita** — a divisão por **conta corrente de origem**.
- **`U1`** (ou `W1`/`AA1` conforme a aba) guarda o **total a pagar**.

O Apps Script daquela planilha **não tem regra de negócio** — são só quatro botões
que preenchem a coluna do marcador em massa, mais um exportador de abas para XLSX
"somente valores". A regra toda está em **fórmula**, e é o que o dono chama de
"muito poluído, pesado, cheio de fórmula".

> ⚠️ **O que NÃO foi lido:** as fórmulas, célula por célula. As planilhas somam
> 1,1 MB + 11,9 MB e o acesso disponível devolve valores, não fórmulas. As
> críticas que hoje vivem nelas estão listadas em §7 como pergunta, porque
> adivinhar regra de conferência de pagamento é o pior lugar para adivinhar.

---

## 4. O que o Make faz hoje (cenário `DP - FIN - Botão Folha de Pagamento (🆕SP)`)

Disparado por webhook com `acao`, `grupo`, `tipo`, `descricao`, `valor`.
O `grupo` seleciona planilha, aba e faixas — dez grupos diferentes na mesma
engrenagem (CTPS, Diárias, Quinzena, Fim de Mês, Gratificados e Mensalistas,
Despesa com Colaboradores, Auxílio Transporte, Auxílio Alimentação, Décimo
Parcela 1 e 2).

Passo a passo, para a Quinzena:

1. Lê `V4:Y53` da aba (centro de custo, valor, …, departamento OMIE) e `U1` (total).
2. **Cria o card de Despesa com Colaboradores** no pipe `301433085`, com o total,
   a descrição, o tipo de despesa por grupo e **até 75 pares de centro de custo +
   valor + código de departamento OMIE**.
3. Pega a **Planilha de Pagamento** (o arquivo SomaPay) no Drive, joga no Dropbox
   (`/BWS DP/DESPESAS COM COLABORADORES/Pgt Conjunto`), sobe uma cópia no Drive,
   **compartilha com `anyone` (link público)** e grava o link no card.
4. Faz o mesmo com a **Planilha de Análise**.
5. Lê `AF4:AG53` — **conta de origem + valor** — e para **cada linha** cria uma
   **SP de Transferência de Recursos** no pipe `301426645`, com a conta na
   descrição, valor, Pix aleatório fixo, CNPJ da BWS, e o id do card de Despesa em
   `conex_o_dc_id`.
6. Liga os dois lados: o campo `conex_o_dc` na SP e o `conex_o_sp` no card de
   Despesa (até 10 SPs).
7. Manda um Telegram por `https://aplicacoes.bwsconstrucoes.com.br/telegram/enviar`
   — **o módulo `telegram` deste repositório**.
8. Responde uma página HTML de "pronto".

### Três defeitos encontrados no blueprint

1. **Campo repetido, valor no lugar errado.** O par 62 grava em
   `valor_centro_de_custo_63` (e existe um `valor_centro_de_custo_63_1`); o mesmo
   acontece no par 72/73. Ou seja: **com 62 ou mais centros de custo, valor de
   centro de custo cai no campo do vizinho.** Merece conferência no Pipefy.
2. **Teto silencioso em 50 linhas.** As faixas param em `53` (linha 4 até 53). Com
   mais de 50 centros de custo ou mais de 50 contas de origem, o que passa disso
   **não entra e ninguém é avisado**.
3. **Link público.** As planilhas de pagamento e de análise — que têm nome, CPF e
   valor de ~500 pessoas — são compartilhadas como `anyone`/`reader` e o link vai
   para o card. Quem tiver o link, lê. Ver §8.

---

## 5. O desenho proposto (para o dono aprovar ou mudar)

### 5.1 O que fica guardado no banco

| Tabela | Para quê | Chave |
|---|---|---|
| `folha_colaborador` | o recorte do cadastro que a folha usa (§2.3) | CPF; com `id_fortes` único |
| `folha_ponto_dia` | uma linha por pessoa/dia, com as 4 obras, horários, falta e situação | CPF + data |
| `folha_presenca_mes` | o resumo `REL_PRESENCA_BWS` por competência | CPF + competência |
| `folha_competencia` | a folha recebida: mês, tipo (quinzena/fim de mês/…), quem subiu, quando | competência + tipo |
| `folha_linha` | uma linha por pessoa por folha: ID Fortes, líquido, filial da contabilidade | folha + id_fortes |
| `folha_apropriacao` | o rateio: pessoa, dia, obra, valor do dia, de onde veio a obra (ponto ou mão) | folha + CPF + dia |
| `folha_critica` | cada alerta levantado, com situação (aberto / tratado / ignorado com motivo) | folha + pessoa + tipo |
| `folha_remessa` | o arquivo gerado por conta corrente, valor, quando, por quem, cards criados | folha + conta |

**Por que guardar o ponto e não ler na hora:** o download do mês inteiro hoje leva
~26 minutos (o Apps Script baixa 1 página por minuto). Uma tela não pode esperar
isso. E o dono já disse o ciclo certo: o **mês corrente** recarrega sempre; o
**mês anterior** recarrega até ~15 dias depois do fechamento, porque o ponto ainda
muda nesse período; depois disso congela.

### 5.2 O caminho na tela

1. **Solta a Folha Sintética** (o `.xls` do Fortes) — o mesmo padrão da casa: uma
   porta só, o documento entra DENTRO do formulário e preenche o que der.
2. O sistema **lê, casa e critica** — e mostra a **prévia** antes de gerar
   qualquer coisa: quantas pessoas, quanto, por obra, por conta corrente, e a
   lista de críticas.
3. O DP **trata as críticas**: resolve, ou marca "ciente, segue assim" **com
   motivo escrito**. Crítica que impede pagamento não tem "seguir assim".
4. **Define à mão a obra de quem não bate ponto** (o dono já avisou que existem).
5. **Gera**: um arquivo SomaPay **por conta corrente**, o relatório em PDF e em
   Excel, e os cards do Pipefy — com a mesma ligação DC ↔ SP de hoje.
6. Fica **registrado**: o que foi gerado, por quem, quando, e com que números.

### 5.3 O que muda de verdade em relação à planilha

- **Um arquivo de pagamento por conta corrente**, como ele pediu — hoje o arquivo
  é um só e só as SPs de transferência são separadas por conta.
- A mesma pessoa pode aparecer em **duas contas** no mesmo pagamento, com a parte
  de cada obra, e isso fica evidenciado no relatório.
- **Sem teto de 50** linhas, e sem par de campo trocado.
- **Sem link público**: o arquivo sai do nosso sistema, para quem tem login.

---

## 6. Sugestões de crítica que hoje NÃO existem (pedido do dono, como gestor de DP)

Ele pediu para eu pensar como gestor de DP e trazer o que ninguém tratou. Em
ordem de quanto dinheiro cada uma protege:

**Impedem o pagamento (bloqueio):**

1. **ID Fortes que não existe no cadastro.** Sem ele não há CPF, e sem CPF não há
   como pagar. Nunca cair para busca por nome.
2. **Dois ID Fortes apontando para o mesmo CPF**, ou dois CPF com o mesmo ID
   Fortes. É cadastro duplicado, e paga duas vezes.
3. **CPF inválido** (dígito verificador) ou repetido dentro da mesma remessa.
4. **Pessoa com `Data de Saída` anterior ao período** e valor na folha.
5. **Pessoa com `Data de Admissão` posterior ao fim do período** e valor na folha.
6. **Soma das linhas ≠ soma dos totais por filial** no arquivo da contabilidade —
   o arquivo veio truncado ou mal lido.
7. **Soma dos arquivos por conta corrente ≠ total da folha.** A conferência que
   fecha tudo: se não bate no centavo, não gera.
8. **Obra sem conta corrente associada.** Não há de onde pagar.

**Alertam, mas o DP decide (com motivo escrito):**

9. **Zero presença no período e valor na folha.** O caso que ele citou.
10. **Fase no Pipefy de desligamento / desligado** e valor na folha.
11. **Aviso prévio em curso** — o valor costuma ser outro.
12. **Obra da contabilidade ≠ obra do ponto.** As duas informações existem e hoje
    ninguém compara.
13. **Valor fora da curva contra o salário cadastrado** (ex.: acima de 1,5× ou
    abaixo de 0,4× do proporcional). Pega erro de digitação da contabilidade.
14. **Valor muito diferente da mesma pessoa na quinzena anterior**, sem mudança de
    cadastro que explique.
15. **Dia sem falta e sem marcação** — nem presença nem falta. É o furo que o dono
    apontou: ou a pessoa esqueceu de bater, ou ninguém lançou a falta. Se for
    muito no mesmo encarregado ou na mesma obra, é sinal de gestão de ponto ruim
    — ou de coisa pior.
16. **Presença com ajuste manual de ponto** acima de um limite no período. O campo
    `presenca_ausencia` já traz o motivo do ajuste; concentração de ajuste numa
    obra é onde fraude aparece.
17. **Marcação nas quatro pontas em obras diferentes** (2×2), sem desempate claro.
18. **Pessoa no cadastro ativa, com presença, e SEM linha na folha.** O inverso do
    que ele pediu — e é o que faz alguém trabalhar e não receber.
19. **Pessoa na folha da contabilidade que não existe no Mobponto** (nem cadastro
    de ponto). Pode ser admissão nova; pode ser gente que não existe.
20. **Presença em obra que já foi encerrada** no nosso cadastro de obras.
21. **Chave Pix do colaborador igual à de outro colaborador.** Dinheiro indo para
    a conta de terceiro é o jeito mais simples de desviar folha.
22. **Reprocessamento da mesma competência** — a mesma folha gerada duas vezes.
    Tem de dizer "esta competência já foi paga em tal dia, no valor X".

**Para o gestor, não para o pagamento:**

23. Custo de mão de obra por obra, mês a mês, e a curva — onde está subindo.
24. Horas de presença por obra ÷ valor apropriado: a obra onde o custo por hora
    foge da média.
25. Gente que bate ponto em obra diferente da de admissão com frequência — ou é
    remanejamento não registrado, ou o cadastro está errado.
26. Faltas por obra e por encarregado.
27. Quem está há N quinzenas sem presença e continua no cadastro — o dono falou
    disso: *"verificar pessoas que não estão trabalhando e demitir"*.

---

## 7. Perguntas que travam trabalho de verdade

**P1 — O rodapé da Folha Sintética não bate com o corpo dela** (§2.1): 507
pessoas / 445.199,96 declarado contra 491 / 430.129,75 somado. Qual dos dois é o
que você paga? O sistema precisa saber qual número usar como trava de fechamento.

**P2 — De onde sai o "Dias" de cada pessoa?** É a contagem de dias com presença no
ponto dentro da quinzena, ou é um número que a contabilidade manda? E dia de
`FERIADO` e de `COMPENSAÇÃO` conta como dia para o rateio?

**P3 — Quem não bate ponto**: hoje você define a obra à mão. São sempre as mesmas
pessoas (dá para deixar uma obra padrão no cadastro delas), ou muda a cada
quinzena?

**P4 — A obra vira conta corrente onde?** Hoje esse "de/para" mora em qual
planilha ou aba? Quero ler do nosso banco, não de planilha — o ERP já tem obras e
o painel já tem as contas do OMIE.

**P5 — Empate no ponto do dia**: duas marcações numa obra e duas em outra. Vale a
da entrada? A da saída? Ou entra como crítica para você decidir?

**P6 — Pessoa que trabalhou em duas obras no mesmo dia**: o valor do dia vai
inteiro para a obra que ganhou, ou é dividido entre as obras do dia? (Hoje,
pelo que entendi, vai inteiro. Confirma?)

**P7 — Diferença de centavo no rateio**: dividir o líquido por dias e somar de
volta quase nunca fecha exato. Onde cai a sobra — no primeiro dia, no último, na
maior obra?

**P8 — Quais das dez "folhas" do Make entram nesta primeira tela?** Você disse
começar pela folha da contabilidade (Quinzena e Fim de Mês). As outras oito
(Diaristas, GM, Alimentação, Transporte, Décimo 1 e 2, CTPS, DC) ficam para depois
— confirma?

**P9 — O card de Despesa com Colaboradores tem 75 pares de centro de custo.** Já
aconteceu uma folha passar de 50 obras? (É o teto silencioso do §4.)

**P10 — Quem pode gerar folha?** Hoje qualquer um com a planilha aberta. No
sistema, é o mestre? Um perfil "DP"? E gerar arquivo de pagamento exige uma
segunda pessoa aprovando, como o aval em duas pessoas do ERP?

**P11 — Onde esta tela mora?** Área nova (`app/apps/folha/`, como o Análise de
SPs) ou dentro do ERP? A folha conversa com ponto, Pipefy, SomaPay e OMIE — tem
cara de área própria; mas se o DP vai operar junto com o financeiro do ERP, talvez
seja melhor dentro dele.

**P12 — O arquivo SomaPay**: o modelo que vi tem CPF com ponto e traço
(`997.133.493-34`) mas o cabeçalho da planilha diz "deve conter 11 dígitos". Qual
dos dois o SomaPay aceita de verdade? E o valor vai como `1.126,60` mesmo?

---

## 8. Segurança — três coisas que já são risco hoje

1. **Credenciais do Mobponto estão escritas dentro dos Apps Script**, em duas
   planilhas. Quem abre a planilha e o editor de script lê a chave. Ao trazer
   para cá elas vão para variáveis de ambiente, e vale **trocar na origem**, pelo
   mesmo motivo do `EL_NFSE_TOKEN` (ver `CONTEXTO.md` §9): chave que já circulou
   é chave a trocar.
2. **Um dos Web Apps está publicado como `ANYONE_ANONYMOUS`** — qualquer pessoa
   com o link dispara a atualização de presenças e cadastros.
3. **As planilhas de pagamento e de análise são compartilhadas com `anyone`** e o
   link vai para o card do Pipefy. São nome, CPF e valor de ~500 pessoas num link
   sem senha. É dado pessoal em volume, e é o risco mais alto dos três.

---

## 9. O que este documento NÃO cobre

- **As fórmulas das planilhas, célula por célula.** Ver §3.
- **A aba `DC`** (Despesa com Colaboradores) e as outras oito folhas do Make, além
  do que a §4 registra.
- **Qualquer teste em produção.** Nada foi executado; nada foi escrito no Pipefy,
  no Drive, no Dropbox ou no OMIE.
