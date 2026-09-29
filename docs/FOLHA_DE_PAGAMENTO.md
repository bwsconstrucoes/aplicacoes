# Folha de pagamento — levantamento do processo atual (26/09/2026)

Este documento existe porque o chat não é memória: o repositório é. Ele registra o
que foi **lido de ponta a ponta** dos arquivos que o dono entregou em 26/09/2026,
o que ficou **entendido**, o que é **suposição** e o que **só ele pode responder**.

Quem começar uma sessão da folha de pagamento lê isto primeiro.

> ⚠️ **Nada aqui é código pronto.** Nenhuma linha foi escrita para o sistema ainda.
> O pedido dele foi: *"faça uma varredura criteriosa e profunda nos arquivos que eu
> lhe mandei (…) e não se acanhe em me perguntar"*.

---

## 0. O que já está escrito (26/09/2026)

| O quê | Onde | Estado |
|---|---|---|
| **Leitor da Folha Sintética** | `app/apps/analisesps/folha_sintetica.py` | pronto e testado contra o arquivo real de 08/2026: 491 pessoas, R$ 430.129,75, 47 filiais, fechando no centavo |
| **Conserto do de/para obra → conta** | `sincronizacao.py`, `_contas_da_aba` | pronto; lia a coluna errada (ver D5) |
| **Regras de rateio + tela** | `folha_rateio.py`, migração 027, tela "Rateio da Folha" | pronto; é o que ele pediu em 26/09 para quem o ponto não apropria (ver §7.4) |
| **A apropriação** (para qual obra vai cada real) | `folha_apropriacao.py` | pronta e testada: obra do dia pelas 4 marcações, rateio pelos dias, mão > regra > ponto, e a ORIGEM de cada valor |

⚠️ **O RELATÓRIO AINDA NÃO EXISTE.** Ele perguntou (*"espero que já tenha sido
gerado esses relatórios"*) — não foram. Existe a conta que o relatório precisa
para ter o que mostrar, incluindo a origem de cada valor; falta desenhar o
relatório e a tela. O que o relatório tem de trazer está em §7.5.

Nada mais. Não há tela, nem tabela nova, nem carga de ponto ainda.

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

**RESOLVIDO pelo dono em 26/09/2026:** *"vamos nos ocupar com as pessoas que
aparecem no relatório. E o valor de cada uma. Então, se o somatório total final
seja maior, ignore isso."*

Ou seja: **a verdade é o corpo do relatório**, pessoa por pessoa. O `Total: Geral`
do rodapé é ignorado — nem trava, nem alerta, nem aparece como divergência.

A conferência de fechamento é outra, e continua valendo: *soma das linhas ==
soma dos totais por filial* (as duas batem no centavo neste arquivo). Se essas
duas divergirem, o arquivo veio truncado ou mal lido — aí sim é bloqueio.

#### O formato do arquivo SomaPay — respondido pelo arquivo, não por suposição

A `Planilha de Pagamento Quinzena CTPS (Fortes)` — o arquivo que o Make anexa ao
card, ou seja, o que de fato é enviado — tem uma aba `Valores` assim:

```
Nome do funcionário | CPF* (obrigatório) | Valor* (obrigatório)
GERLANIO GOMES LIMA | 997.133.493-34     | 1.126,60
```

Ou seja: **CPF formatado, com ponto e traço**, e **valor em formato brasileiro**.
O texto de instrução no alto da planilha diz "deve conter 11 dígitos", mas as
linhas que vêm sendo enviadas usam o CPF formatado — e funcionam. O sistema vai
gerar igual ao que já funciona.

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
>
> **E ISSO TEM SOLUÇÃO — ver `EXPORTAR_FORMULAS.md`, ao lado.** O dono cobrou com
> razão em 26/09/2026: *"você precisa ler as fórmulas das planilhas, nelas foi
> criado todo o regramento, do contrário você vai criar algo errado"*. Há um
> script de Apps Script pronto que junta **só as fórmulas** num documento do
> Drive, que eu leio — sem trazer nome, CPF ou salário de ninguém para a
> conversa. **Enquanto isso não for feito, tudo que depende de fórmula está em
> suposição.**

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
| `folha_apropriacao` | o rateio: pessoa, dia, obra que o PONTO disse, obra que VALE, valor do dia | folha + CPF + dia |
| `folha_ajuste` | cada ajuste de mão: nível, o que era, o que passou a valer, quem, quando, motivo | folha + CPF (+ dia) |
| `folha_critica` | cada alerta levantado, com situação (aberto / tratado / ignorado com motivo) | folha + pessoa + tipo |
| `folha_remessa` | o arquivo gerado por conta corrente, valor, quando, por quem, cards criados | folha + conta |

⚠️ **`folha_apropriacao` guarda as DUAS obras — a do ponto e a que vale.** Uma
coluna só, sobrescrita pelo ajuste, faria o relatório perder a capacidade de
dizer "o ponto dizia A, mudaram para B" — que é justamente o que o relatório
existe para provar. Ver §7.3.

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
4. **Faz o ajuste fino** — desmarcar pessoa, trocar a obra, dia a dia ou por
   proporção. Ver §7.3; a tela junta num só lugar os ~10 casos que precisam da
   mão dele, para o resto da folha não precisar ser aberto.
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
18. **Pessoa CTPS, com presença, e SEM linha na folha da contabilidade.** O
    inverso do que ele pediu — e é o que faz alguém trabalhar e não receber.
    ⚠️ **Só para quem é CTPS**, e o porquê está em D24: o ponto tem mais gente do
    que a folha da contabilidade, de propósito. Sem essa qualificação, esta
    crítica dispararia centenas de vezes por quinzena para gente que está certa.
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

## 7. Decisões do dono (26/09/2026) e o que ainda falta

### 7.1 Decidido — não reabrir sem motivo novo

**D1 — Qual número vale.** Só as pessoas que aparecem no relatório, com o valor de
cada uma. O `Total: Geral` do rodapé é ignorado (§2.1).

**D2 — Os dias saem do PONTO, não da contabilidade.** *"Os dias de cada pessoa é
baseado exatamente na folha do ponto. Cada dia, ele vai estar presente em uma
determinada obra. Então o dia é o dia daquela obra."* A pessoa pode passar o mês
todo numa obra ou variar; é por isso que se pesca o dia, para ratear o valor dela
por dia.

**D3 — Qual pedaço do ponto entra em cada pagamento.** Quinzena
(adiantamento) = dias **1 a 15**. Fim de mês = **16 até o último dia do mês**.
*"Se eu estou pagando a quinzena, eu só vou fazer a leitura dos dias de 1 ao 15."*
Nada de ler o mês inteiro e dividir depois.

**D4 — Empate no ponto do dia.** Vale a obra que **mais aparece** entre as quatro
marcações. No empate 2×2, vale a obra das **duas primeiras marcações** (entrada e
almoço). Pessoa em duas obras no mesmo dia é, na maioria dos casos, **erro de
batida** — o dia vai inteiro para uma obra só, nunca dividido.

**D5 — Obra → conta corrente: já está no nosso banco.** Mora na aba **`C. Diários`**
da planilha `Registro de SPs`, e o Análise de SPs **já a carrega toda noite**:

| Onde | O que | Tabela nossa |
|---|---|---|
| `C. Diários` col. A (`Código Primário`) → col. B | centro de custo → **conta de pagamento** | `analisesps.contas_diarios` |
| `C. Diários` (`Código Primário`/`Obra`) → (`Código Omie`) | obra → **código do departamento no OMIE** | `analisesps.referencias_rateio`, tipo `obra` |

#### ⚠️ CORREÇÃO DE UM ALARME FALSO MEU — 27/09/2026

Eu disse ao dono, em 26/09, que a carga lia a coluna errada. **NÃO LIA.**

Eu havia lido o cabeçalho de uma **cópia** da planilha da folha, onde a segunda
coluna é o ID do Pipefy, e concluí que a nossa carga pegava o ID. As fórmulas que
ele exportou mostraram a aba de verdade — a do `Registro de SPs` — e ela tem
**quatro colunas só**:

| A | B | C | D |
|---|---|---|---|
| Código Primário | **Conta de Pagamento** | Projeto | Código Omie |

Todas vindas por `IMPORTRANGE` da planilha "Bases de Dados Pipefy" (colunas T, AH,
AK e AJ da `C. Diários` de lá). A posição estava certa.

#### ⚠️ MAS O DEFEITO DE VERDADE ESTÁ NA MESMA COLUNA

A coluna **não guarda a conta — guarda um TEXTO com a conta dentro**, porque vem
da coluna AH do Pipefy:

```
BRADESCO S/A - AG 1234 | 0007011-4 | CONS
```

A planilha da folha extrai com `REGEXEXTRACT(...; "\|\s*0*(\d+-\d+)\s*\|")`
(coluna G da `C. Diários` de lá), e a aba `SPsBD` faz igual na coluna U. O zero à
esquerda sai: o que vale é **`7011-4`**.

A nossa carga guardava **o texto cru**. Isso não casa com nada — nem com a conta
da conciliação, nem com o que o OMIE conhece. E, como nenhuma tela lia a tabela,
só ia aparecer no primeiro uso, que seria a folha decidindo de qual conta sai o
dinheiro de ~500 pessoas.

**Consertado:** a carga extrai a conta do texto (e aceita o texto já limpo, para o
dia em que a planilha for arrumada). Sete formatos testados.

**A lição, para ficar:** eu reportei um defeito olhando o cabeçalho da planilha
errada. O defeito existia, na mesma coluna, por outro motivo — sorte, não método.
**Sem as fórmulas, o que eu digo sobre essas planilhas é palpite.**

**D6 — Onde a tela mora.** O DP **não** opera junto com o financeiro: *"eu vou
disponibilizar apenas aquela tela (…) quando eu for liberar para acessarem, aí eu
libero só aquela tela."* Isso é exatamente o controle por tela que o Análise de
SPs ganhou em 25/09/2026 (cadastro de usuários com as telas de cada um). Ver §7.2,
Q2 para a confirmação que falta.

**D7 — A sobra de centavo do rateio vai para a obra com MAIS DIAS.**

**D8 — Só Quinzena e Fim de Mês nesta primeira tela.** As outras oito folhas
(Diaristas, Gratificados e Mensalistas, Auxílio Alimentação, Auxílio Transporte,
Décimo Parcela 1 e 2, CTPS, DC) ficam para depois.

**D9 — Quem faz o quê.** Por enquanto **não** tem aval em duas pessoas. O
desenho é: *"vai ter o usuário do DP que vai estar fazendo a leitura, mas o
usuário master, que sou eu, administrador, eu gero o arquivo."* Ou seja: o DP
sobe a folha, cruza, trata as críticas e olha a prévia; **gerar o arquivo e criar
os cards é só do mestre**. Isso é exatamente o `SO_DO_MESTRE` que o Análise de
SPs já tem.

**D10 — A tela nasce dentro do Análise de SPs.** Confirmado. E ele levantou uma
coisa que faz sentido: *"a gente pode até alterar essa Análise de SPs para
Análise de Pagamentos, que já fica mais abrangente"* — ver §7.2, Q2, sobre o
tamanho certo dessa mudança.

**D11 — Por que aqui e não no ERP (com as palavras dele).** *"Tem algumas coisas
que a gente está trabalhando aqui na Análise de SPs porque eu ainda não consegui
implantar o ERP. São coisas que a gente precisa hoje, mas que provavelmente a
gente vai utilizar no ERP depois. Esse início de folha de pagamento já é algo que
a gente deve utilizar lá no futuro."* Fica registrado: **isto vai migrar para o
ERP um dia.** O que a decisão obriga na prática: regra de negócio em módulo
próprio (`folha_*.py`), longe da tela, para a migração ser mover arquivo e não
reescrever.

**D12 — Quem não bate ponto: são DOIS casos, e não um.** Eu havia entendido
errado (achei que fosse "obra padrão no cadastro"); ele corrigiu:

1. **Quem não bate ponto por dinâmica da função** — *"normalmente supervisores de
   obras, mas ainda assim a gente precisa apropriar"*. Não há dia nenhum no ponto
   para ratear.
2. **Quem bate ponto na MATRIZ ou na FILIAL** (`CONS`, `BWSNE`) — o ponto existe,
   mas aponta para a matriz, e o valor tem de ser **rateado entre obras** depois.

São problemas diferentes: no primeiro não há dia; no segundo há dia, mas no lugar
"errado" de propósito. Como decidir a divisão em cada um é a pergunta Q1 de §7.2.

### 7.2 O que ainda falta responder

**Q2 — Renomear "Análise de SPs" para "Análise de Pagamentos": até onde?** Eu
recomendo mudar **só o que gente lê** — o nome no menu, o título das telas, os
textos. E **não** mexer na pasta, no nome do schema do banco (`analisesps`) nem
nas variáveis de ambiente (`ANALISESPS_*`). O motivo é seco: renomear aquilo é
migração de banco, troca de variável no Render e risco de o módulo não subir, em
troca de zero diferença para quem usa. O nome técnico ninguém vê.

**Q3 — E sobre "aquele módulo cresce mais":** eu me expressei mal. Não é um
problema técnico, é um alerta sobre um arquivo grande. O Análise de SPs já tem
umas 50 telas e rotinas; enfiando a folha de pagamento ali, ele vira o módulo
mais gordo do repositório, e um chat novo leva mais tempo para entender onde
mexer. A alternativa era área separada, com a desvantagem de ter que recriar
login, permissão e navegação. **Sua decisão de deixar dentro está certa** — e o
jeito de pagar esse preço é o que a D11 já obriga: a regra da folha em arquivos
próprios, para poder sair inteira quando for para o ERP.

### 7.3 O AJUSTE FINO — o que ele pediu, e como proponho fazer

Pedido dele, com todas as letras:

> *"Eu preciso ter a liberdade de escolher, de desmarcar uma pessoa para gerar o
> pagamento ou não. Preciso ter a liberdade, caso eu queira alterar a obra que
> aquela pessoa vai ficar apropriada, porque de repente tem um erro de ponto (…)
> Às vezes tem umas pessoas — normalmente são as 10 que eu preciso alterar para
> onde o valor delas vai — e às vezes eu distribuo em várias obras: bota um dia
> numa obra, um dia em outra obra. Aí eu altero a planilha do ponto de onde ela
> puxa. Eu não altero a base do ponto, altero só a planilha naquele momento, para
> ajustar essa apropriação financeira."*

**A regra que sustenta tudo isso: o ponto é FONTE, e fonte não se edita.** O
ajuste mora na FOLHA, nunca no ponto. Três consequências, e são elas que fazem a
coisa funcionar:

1. Recarregar o ponto **não apaga** o ajuste que você fez.
2. O ajuste de uma quinzena **não contamina** a próxima nem nenhum relatório.
3. Dá para mostrar, linha por linha, **o que o ponto disse e o que você mudou** —
   que é o que o relatório precisa provar.

É exatamente o que você faz hoje (mexer na cópia, não na base), só que sem
depender de você lembrar de não salvar na base errada.

**Cinco níveis, do mais preguiçoso ao mais fino** — porque quase todo caso é do
primeiro tipo, e obrigar a descer ao dia a dia em todos seria pior que a planilha:

| Nível | O que faz | Para quê |
|---|---|---|
| **0 — regra de quem não tem dia** | vem da resposta da Q1 (§7.2): supervisor rateado entre as obras que responde, ou quem bate na matriz rateado depois | ⚠️ NÃO é "obra padrão no cadastro" — eu havia entendido errado. Ver D12 |
| **1 — fora** | tique "não pagar" na pessoa | o que você chamou de desmarcar. Sai do arquivo e do rateio; continua na prévia, riscada, com o motivo |
| **2 — uma obra só** | escolhe a obra; **todos** os dias vão para lá | o caso mais comum: erro de ponto, a pessoa trabalhou noutro lugar. Um clique |
| **3 — dia a dia** | abre os dias do período e troca a obra de cada um | o seu "bota um dia numa obra, um dia em outra" |
| **4 — por proporção** | você diz "70% obra A, 30% obra B" e o sistema escolhe os dias | mais rápido que o nível 3 quando o que você quer é dividir o DINHEIRO, não escolher dias |

Sobre o **nível 4**: é sugestão minha, não pedido seu. Hoje você conta dia por dia
porque a planilha só permite isso — mas o que você quer no fim é o valor dividido
entre as obras. O preço é que o sistema escolhe quais dias vão para onde, e isso é
arbitrário; por isso ele **marca a linha como "dividido por proporção"** no
relatório, para ninguém achar que aqueles dias vieram do ponto. Se você preferir
não ter esse nível, ele sai.

**O que a tela mostra para os ~10 casos**: uma faixa "**precisa da sua mão**" no
alto, com as pessoas sem ponto, as com ponto estranho e as que você ajustou. O
resto da folha nem precisa ser aberto.

⚠️ **Quem GERA é o mestre (D9).** O DP faz tudo isto — sobe a folha, cruza, trata
crítica, ajusta obra, olha a prévia — e para no botão de gerar. O arquivo de
pagamento e os cards saem com o mestre.

**O que fica registrado em cada ajuste:** quem fez, quando, o que o ponto dizia,
o que passou a valer e o motivo. Sem isso, o relatório do mês que vem não explica
por que a obra X custou mais.

### 7.4 As regras de rateio — decidido e FEITO (26/09/2026)

A resposta dele à minha pergunta sobre como dividir o valor de quem o ponto não
apropria:

> *"Em algum local a gente eleger as pessoas que vão ser rateadas e, para cada
> uma — ou para um grupo, porque pode ser que tenha variação entre uma e outra —
> definir para quais obras o valor dela vai ser rateado. Um detalhe: pode ser que
> uma obra entre mais que a outra. Tipo assim, uma obra é 50% e o restante
> dividido entre outras. (…) Tem que ter algum ambiente aí, alguma telinha. E na
> leitura da tela, algum indicador, uma tag, dizendo que aquela pessoa ali está
> tendo o distribuído, e eu clicar e abrir de novo e poder editar, poder ver."*

Ou seja: **não é regra automática, é cadastro** — ele elege quem e diz para onde.
Nada de "proporcional ao valor da folha", que era a minha sugestão. Está certo:
regra automática esconderia a decisão dele atrás de uma conta.

**O que existe agora** (migração 027, `folha_rateio.py`, tela "Rateio da Folha"):

- **Regra por GRUPO**, com uma ou muitas pessoas. Grupo de um é o caso individual
  — assim existe uma forma só de perguntar "como essa pessoa é rateada?".
- **Peso por obra**: percentual por obra somando 100%, **ou** algumas obras
  marcadas como **"o resto"** — e o resto é dividido em partes iguais entre elas.
  É o "50% numa obra e o restante dividido entre as outras", sem ninguém fazer a
  conta de cabeça.
- **Não fecha 100% → recusa, e não arredonda por conta própria.** Rateio que não
  fecha esconde ou inventa dinheiro, e a diferença apareceria depois num total de
  obra que ninguém consegue explicar.
- **A soma das partes é sempre igual ao valor.** A sobra do centavo vai para a
  **maior fatia** — a mesma regra que ele escolheu para o rateio por dias (D7).
- **Conferir antes de gravar**: um campo de valor na tela mostra quanto vai para
  cada obra, usando a MESMA conta da gravação.
- **Uma pessoa só pode estar em uma regra VALENDO.** Duas não têm resposta certa.
  ⚠️ Esta trava é no **código**, não no banco: o Postgres não aceita subconsulta
  na condição de um índice parcial, e a coluna `ativa` mora na outra tabela. Está
  escrito na migração e tem teste com banco de verdade — é o único sustento dela.
- **Desativar em vez de apagar**: regra desativada explica como a folha de março
  foi rateada. A pessoa pode reaparecer numa regra desativada; em duas ativas,
  não.
- **O CPF é conferido pelo dígito verificador.** Não é frescura: um dígito trocado
  faz a regra nunca encontrar a pessoa, e ela volta a ser apropriada pelo ponto da
  matriz — com a regra ali, escrita, sem pegar.
- **Só do MESTRE** (D9): a tela não aparece no menu de quem não é, e não pode ser
  liberada no cadastro de acesso.

**O que FALTA desse pedido:** a **tag na tela da folha**, dizendo que aquela pessoa
está sendo distribuída, com o clique que abre a regra para ver e editar. Falta
porque **a tela da folha ainda não existe** — é a próxima peça. A regra e a conta
estão prontas e testadas; a tag é uma linha quando houver onde pendurá-la.

### 7.5 Gerar, relatar e lançar — decidido em 26/09/2026 (parte 2)

#### D14 — Gerar os arquivos e os relatórios NÃO cria os cards do Pipefy

> *"Eu vou poder gerar, por exemplo, arquivo de pagamento e folha e relatórios,
> tudo sem necessariamente gerar os cards do Pipefy. É melhor dessa forma."*

São **duas ações separadas**, e a segunda é opcional. O que isso resolve: hoje o
botão do Make faz tudo de uma vez, então conferir o relatório obriga a criar dois
cards no Pipefy — e se o relatório estiver errado, os cards já existem. Separando,
o relatório pode ser gerado quantas vezes quiser sem sujar nada lá fora.

#### D15 — Regerar é normal, e os dois lados NÃO ficam interligados

> *"Pode ser que seja necessário gerar novamente os arquivos (…) não vai estar
> interligado, mas a gente faz o cancelamento no Pipefy e gera de novo quando for
> necessário, se a gente detectar posteriormente alguma falha."*

Ou seja: o sistema **não** tenta consertar o Pipefy. Gerar de novo é livre; o
cancelamento do card antigo é feito por ele, à mão, lá. O sistema só precisa
**registrar** que gerou de novo, e quando.

⚠️ E aqui vale uma nota de risco que ele não pediu mas que a decisão cria: nada
impede lançar a mesma competência duas vezes no Pipefy. Vou fazer a tela **avisar**
("esta quinzena já foi lançada em tal dia, nos cards X e Y — quer lançar de novo?")
em vez de impedir. Avisar respeita a decisão dele; impedir seria eu decidindo no
lugar dele.

#### D16 — O mínimo de requisições ao Pipefy, e um log de tudo

> *"Precisa ser inteligente. O mínimo de requisições ao Pipefy possível. E
> registrar isso aí (…) ter um log de tudo que foi gravado. Foi quinzena, período
> tal, foi gravado, foi dos cards tais e tais. Tudo sem tela de sistema, né? Não
> precisa nem mandar pelo Telegram, porque tendo no sistema é até melhor."*

Duas coisas:

1. **Poucas chamadas.** O cenário do Make de hoje faz, por lançamento: 1 leitura de
   planilha, 1 criação de card, 2 uploads, 2 compartilhamentos, 2 atualizações de
   campo, mais 1 card por conta de origem, mais a ligação entre eles. O caminho
   novo cria o card de Despesa com **todos os centros de custo numa chamada só**
   (é assim que a API aceita) e as SPs de transferência numa por conta — sem
   upload nem compartilhamento, porque o arquivo fica no nosso sistema.
2. **O log fica AQUI, não no Telegram.** Competência, tipo (quinzena/fim de mês),
   período, quem gerou, quando, os arquivos gerados, os cards criados e o que cada
   chamada respondeu. É o que permite responder "o que foi pago nessa quinzena, e
   para onde foi" meses depois, sem depender de ninguém lembrar.

#### D17 — O relatório é a prova, e tem de explicar a apropriação

> *"É importante a questão do relatório: estar detalhado toda essa, como é que foi
> feita essa divisão, essa apropriação. Se for uma pessoa que tem ponto completo
> na obra, beleza, está tudo certo — mas se for uma pessoa que tem uma
> distribuição, dias em obras diferentes, isso tem que estar exposto nesse
> relatório. Mostrar o valor global, o valor por obra, o valor de cada colaborador
> e a distribuição de como é que foi feito de cada um. (…) No relatório tem várias
> visões: agrupado, desagrupado, analítico, resumido, para poder uma auditoria
> futura, se alguém precisar compreender, entender. E saber até de onde é que foi
> que veio aquela informação, se foi do ponto, se foi colocada de forma manual."*

**As visões que o relatório precisa ter:**

| Visão | Responde |
|---|---|
| **Resumo** | quanto, quantas pessoas, quantas obras, quanto por conta corrente — a capa |
| **Por obra** (agrupado) | quanto cada obra carregou, e com quantas pessoas |
| **Por conta corrente** | o que sai de cada conta — é o que amarra com o arquivo de pagamento |
| **Por colaborador** | o valor de cada pessoa e em que obras ele se dividiu |
| **Analítico** (desagrupado) | uma linha por pessoa × dia × obra, com o valor do dia |
| **O que precisou de mão** | só quem teve regra ou ajuste, com o que era antes e o que passou a valer |

**E em toda linha, a ORIGEM**: `do ponto`, `da regra` (com o nome da regra) ou
`da mão` (com quem mudou, quando e por quê). É por isso que
`folha_apropriacao.py` carrega a origem em cada nível — sem ela o relatório não
tem como se explicar, e relatório que não se explica não serve de prova.

**Os dois formatos**: PDF (para anexar e assinar) e Excel (para quem quer conferir
somando). O mesmo conteúdo, para os dois nunca divergirem.

### 7.6 A tela — o que ele pediu em 26/09/2026 (parte 3)

#### D18 — Quinzena ou fim de mês: o arquivo sugere, ele confirma

> *"Como é que a gente vai saber se a gente está tratando de quinzena, se está
> tratando de fim de mês, e se a gente informa, se seleciona para informar de qual
> arquivo é aquele dali que a gente está tratando."*

**O título do relatório responde.** O arquivo real se chama "Folha Sintética -
**Adiantamento** de Folha", e adiantamento é a quinzena (dias 1 a 15). Então: o
sistema lê o título, **sugere**, e a tela mostra a escolha já marcada — mas
marcável.

**OS DOIS TÍTULOS DE VERDADE** — ele colou o de fim de mês em 26/09/2026:

| Pagamento | Título do relatório | Dias do ponto |
|---|---|---|
| Quinzena (adiantamento) | `Folha Sintética - Adiantamento de Folha` | 1 a 15 |
| Fim de mês | `Folha Sintética - Folha de Pagamento` | 16 ao último |

⚠️ **A ordem da conferência importa:** o título de fim de mês é o mais genérico dos
dois ("Folha de Pagamento"), então o **adiantamento é testado primeiro**. Um
relatório que fosse "Adiantamento - Folha de Pagamento" tem de cair em quinzena.

⚠️ **E quando o título não bate com nenhum dos dois, a tela PERGUNTA em vez de
adivinhar.** Chutar faria ler o pedaço errado do ponto, e o erro sairia como um
valor plausível, na obra errada. A competência (`Mês/Ano: 08/2026`) sai do arquivo.

#### ⚠️ D18-b — A armadilha de cem vezes, achada ao ler a folha de fim de mês

As linhas que ele colou trazem o valor em **dois formatos na mesma folha**:

```
000013  GERLANIO GOMES LIMA          1.074,64   ← texto, com ponto de milhar
000387  LUELIA MADIDA GOMES TOMAS    1362,56    ← texto, sem milhar
```

E o arquivo de adiantamento trazia número de verdade na célula (`1198.84`). Ou
seja: **não dá para assumir formato nenhum**.

A leitura antiga apagava todo ponto e trocava vírgula por ponto. Isso acerta os
dois de cima — mas um valor que venha como texto com **ponto decimal**
(`1198.84`, que é o que sai de uma reexportação) viraria **119884**.

**E a conferência de fechamento não pegaria:** o total da filial vem no mesmo
formato e inflaria igual, então as duas somas continuariam batendo e a tela diria
"fecha" com todo mundo recebendo cem vezes mais.

A regra agora: **vírgula manda** (é decimal); sem vírgula, um ponto seguido de um
ou dois dígitos no fim também é decimal; qualquer outro ponto é milhar. Onze casos
testados, incluindo `R$`, negativo com sinal e negativo entre parênteses.

#### D19 — O arquivo entra por uma área de soltar, como o OFX

> *"A questão do arquivo de importação, semelhante àquela do extrato bancário, o
> OFX, aquele retangulozinho para você jogar o arquivo de importação dentro."*

Mesmo padrão da Conciliação, e mesma sequência: solta → o sistema **lê e mostra o
que entendeu** (competência, tipo, quantas pessoas, quanto, e as críticas) → e só
grava depois que ele confere. É também a regra da casa (`CLAUDE.md`, 17/09): uma
porta só, o documento entra DENTRO do formulário.

#### D20 — Barra lateral com filtros, para analisar depois de importar

> *"Na tela é interessante ter um sidebar para a gente poder filtrar (…) tipo
> assim: eu já importei, o sistema já sugeriu, mas deixa eu ver o que é que tá
> ficando na obra tal, o que é que tá ficando na conta tal."*

Os filtros que a tela precisa ter (mesma barra lateral das outras telas do
módulo):

| Filtro | Responde |
|---|---|
| **Obra** | o que está ficando naquela obra |
| **Conta corrente** | o que vai sair daquela conta |
| **Origem** (ponto / regra / mão) | o que o sistema decidiu e o que foi decidido por gente |
| **Situação** | só quem tem crítica, só quem está sem obra, só quem foi desmarcado, só quem foi ajustado |
| **Nome ou CPF** | achar uma pessoa |
| **Faixa de valor** | os maiores, que é onde o erro custa mais |
| **Filial da contabilidade** | comparar com a obra do ponto |

E os totais da tela respeitam o filtro — senão o número de cima não explica a
lista de baixo.

#### D21 — A idade do ponto tem de estar na cara, e atualizável na hora

> *"Se a gente precisar atualizar o ponto naquele instante, aí a gente informar.
> A gente tem que ter a visualização e a informação fácil da última atualização do
> ponto que está lá, para a gente saber: de repente eu atualizei agora, eu quero
> saber se é o ponto da atualização da madrugada ou se é o ponto que eu atualizei
> recente."*

⚠️ **É a informação mais importante da tela depois do valor**, e o motivo é seco: a
apropriação inteira sai do ponto. Ponto de ontem com ajuste feito hoje de manhã
gera uma apropriação que já nasceu errada — e nada na tela denunciaria isso.

O que a tela mostra, sempre visível:

- **de quando é o ponto** de cada competência que a folha usa (o mês corrente e,
  quando o período atravessa, o anterior), em data e hora de Brasília;
- **quanto tempo faz** ("há 3 horas", "ontem à noite"), porque é isso que ele lê
  de relance;
- **quem disparou** a última carga: a rotina da madrugada ou uma pessoa;
- um botão **"atualizar o ponto agora"**, com o andamento à vista — a carga de um
  mês leva minutos, e botão que não mostra andamento é apertado três vezes.

⚠️ E uma coisa que o sistema deve fazer sozinho: se o ponto foi carregado **antes**
de a folha ser importada, a tela avisa. É o caso em que a conta está velha sem
ninguém ter feito nada errado.

### 7.7 Correções e o que o dono precisa criar no Render

#### ⚠️ D22 — Quem não tem ID Fortes no cadastro NÃO PODE FICAR ESCONDIDO

Correção dele, sobre uma decisão minha que estava errada:

> *"Pessoas sem ID Fortes no cadastro não entram. Na verdade, ela vai entrar após
> tratamento. Vamos tratar para poder entrar. Então não pode ficar oculto,
> escondido."*

Eu havia tirado essas pessoas da lista e posto numa lista à parte. **Ele está certo
e o erro é grave:** lista à parte é lista que alguém esquece de abrir — e aí a
pessoa desaparece da folha. Trabalhou e não recebeu, sem nada na tela gritando.

Como ficou (já implementado, com testes):

- a pessoa fica na **mesma lista**, marcada como **pendente**, sem CPF e sem
  apropriação, com a crítica escrita;
- ela **conta no total a pagar** sem ter obra, então **a folha não fecha** até ser
  tratada — que é exatamente o que tem de acontecer;
- a lista de pendentes continua existindo, mas como **atalho** para a tela montar a
  faixa "precisa da sua mão": são os MESMOS objetos, não uma cópia que pode
  divergir;
- tratada (o cadastro passa a ter o ID), ela entra normalmente na apropriação.

**Duas formas de tratar**, e a tela vai oferecer as duas:

1. **preencher o ID Fortes no cadastro** — o certo, porque resolve para sempre;
2. **amarrar o CPF ali na tela** — rápido, para a folha não ficar travada esperando
   uma ida à planilha. O sistema fica avisando que o cadastro continua sem o ID,
   senão o mesmo trabalho volta na quinzena seguinte.

#### D23 — As variáveis do Mobponto, para criar no Render

Ele pediu os nomes. São **duas**, e as duas saem do Apps Script que ele mandou
(cabeçalhos da requisição) — **copiadas de lá direto para o Render, sem passar por
chat nenhum**:

Ele mesmo identificou quais são, e acertou: *"acho que as variáveis do Mobponto
são Authorization e api-key"*. Os nomes das variáveis seguem exatamente isso, para
não haver dúvida sobre o que colar em cada uma:

| Variável no Render | O que colar | Onde está hoje |
|---|---|---|
| `MOBPONTO_AUTHORIZATION` | o valor **inteiro** do cabeçalho `Authorization`, incluindo a palavra `Basic` e o espaço | no `Código.gs` / `Mobponto.gs`, em `CONF.headers` |
| `MOBPONTO_API_KEY` | o valor **inteiro** do cabeçalho `api-key` | idem |

⚠️ **Copiar inteiro, sem cortar nada.** O código aceita as duas formas do
`Authorization` (com ou sem o `Basic ` na frente, ele completa se faltar) — mas a
instrução para quem cria é uma só: **copie o valor inteiro**. Instrução com duas
opções é instrução que se erra.

E uma **opcional**, que eu não preciso que ele crie agora:

| Variável | Para quê |
|---|---|
| `MOBPONTO_URL` | o endereço do endpoint, se um dia mudar. Sem ela, vale o de hoje, que fica no código |

O `api-version: 1.0.0` é constante e fica no código — não é segredo.

⚠️ **E o aviso que vem junto:** essas credenciais estão **escritas dentro de duas
planilhas**, em Apps Script. Quem abre a planilha e o editor de script lê a chave.
Depois de pôr no Render, **trocar a chave no Mobponto** — pelo mesmo motivo do
token da NFS-e que já vazou (`CONTEXTO.md` §9): chave que circulou é chave a
trocar. E o Web App publicado como "qualquer pessoa, sem login" deve ser
despublicado quando o sistema assumir a carga.

### 7.8 O ponto tem mais gente do que a folha — e isso é NORMAL (D24)

> *"No ponto tem mais informação de pessoas do que tem na folha de pagamento, no
> arquivo. Até porque esse arquivo é de parte do pessoal. Outros entram num outro
> método de pagamento — o pessoal que não vem da contabilidade, mas tem ponto
> batido. Aí depois a gente vai criar uma outra tela para verificar eles."*

⚠️ **ISSO DESFEZ UMA CRÍTICA QUE EU HAVIA PROPOSTO.** Na minha lista (§6, item 18)
estava "pessoa ativa, com presença, e SEM linha na folha" — como alerta. Para essas
pessoas, **não estar na folha da contabilidade é o estado normal**. Centenas de
alertas esperados por quinzena é o jeito mais rápido de fazer ninguém ler mais
nenhum alerta desta tela, inclusive os que importam.

**Como ficou** (já implementado, com testes): quem bateu ponto no período e não
está nesta folha sai em **três listas separadas**, e a separação é pelo
`Tipo de Cadastro`:

| Lista | Quem | O que é |
|---|---|---|
| **deveria estar** | é **CTPS** e tem ponto, mas não está na folha | ⚠️ alerta de verdade: trabalhou e pode não receber |
| **outro método** | não é CTPS (prestador, RPA, diarista…) | **normal.** É a lista de entrada da tela que ele vai pedir depois |
| **sem cadastro** | tem ponto e não está no cadastro de colaboradores | pode ser admissão nova; pode ser gente que não existe. Outra conversa |

E **na dúvida não alerta**: cadastro sem tipo cai em "outro método". É melhor a
pessoa aparecer numa lista que alguém vai olhar do que gerar alerta falso na folha
— alerta falso faz parar de ler alerta.

**O que isso prepara:** a tela futura dos pagamentos que não vêm da contabilidade
já tem a entrada dela pronta — quem tem ponto, quantos dias, em quais obras.

### 7.9 CTPS ou DIÁRIA — a primeira fórmula recuperada (D25)

O dono mandou, em 26/09/2026, a fórmula da **coluna AH da aba `Mobponto`** da
planilha "Folha de Pagamento - Fortes", que é quem separa **CTPS** (vem da
contabilidade) de **DIÁRIA**. Está traduzida fielmente em
`app/apps/analisesps/folha_vinculo.py`, com a fórmula original no cabeçalho para
conferência.

#### ⚠️ E ela derrubou o que eu havia feito um dia antes

Eu classificava pelo **`Tipo de Cadastro` da pessoa**. A fórmula classifica **por
DIA**, comparando a data do dia de ponto com a **Data de Início** e a **Data de
Admissão**:

| Situação | Vínculo |
|---|---|
| o dia caiu **entre começar a trabalhar e ser registrada** (`início <= dia < admissão`) | **DIÁRIA** |
| começou e **nunca foi registrada** (`início <= dia`, admissão em branco) | **DIÁRIA** |
| **Prestador de Serviço + Autônomo (RPA)**, nos três arranjos de data que a fórmula lista | **DIÁRIA** |
| as **duas datas em branco**, ou **CLT sem data de admissão** | **DT INÍCIO/ADMISSÃO** (pendência de cadastro) |
| a pessoa **não está no cadastro** | **NÃO ENCONTRADO** |
| o resto | **CTPS** |

**Por que isso importa tanto:** quem foi admitido no dia 10 tem dias de diária
(antes) e dias de CTPS (depois) **na mesma quinzena**. Classificando por pessoa,
metade do dinheiro dela iria para o método de pagamento errado — a pessoa
recebendo de menos num lado e de mais no outro, com os dois relatórios fechando.

O que mudou no código: a separação de "quem tem ponto e não está nesta folha"
agora tem **quatro** listas, não três — entrou `falta_data`, para o cadastro pela
metade, que não é diarista nem CTPS. E basta **um** dia de CTPS para a pessoa ir
para o alerta: exigir que todos fossem CTPS esconderia justamente o caso do
admitido no meio do período.

#### Duas coisas fielmente replicadas, e uma delas é candidata a pergunta

1. **A ordem dos testes é a da fórmula**, e não pode ser "arrumada": vários casos
   se sobrepõem, e a ordem é o que decide qual vence.
2. ⚠️ **Célula vazia numa comparação de data vale ZERO no Google Sheets**
   (30/12/1899). Então, com a Data de Início em branco, `início <= dia` é
   **verdade** — e um dia antes da admissão cai em DIÁRIA. Não é escolha minha;
   é o que está em produção. Replicado de propósito, porque mudar faria a
   classificação divergir da planilha justamente nos casos de cadastro incompleto.
   **Mas é candidato a pergunta:** é isso que ele quer, ou é acidente do Sheets?

## 7.10 O REGRAMENTO RECUPERADO DAS FÓRMULAS (27/09/2026)

O dono rodou o script e pôs os documentos na pasta
`1pc3UDuICXyN_OT_pVKUZpg621dNPnGNW`. Isto é o que as fórmulas da planilha
**Folha de Pagamento - Fortes** dizem — lido, não suposto.

⚠️ **Leia esta seção antes de escrever qualquer linha da tela.** Metade do que eu
havia suposto estava certo; a outra metade, não.

### 7.10.1 De onde cada coisa vem (a cadeia de planilhas)

```
Bases de Dados Pipefy ──┬─> C. Diários      (T=Código Primário, AH=Conta,
                        │                    AJ=Código Omie, AK=Projeto)
                        ├─> Centros de Custo (código, Pipefy, Omie)
                        ├─> PlanoFinanceiro
                        └─> BaseBancos      (conta → nCodCC do Omie → cód Pipefy)

Registro de Colaboradores ─> CadastroColaboradores (16 colunas escolhidas)
Mobponto (Geral Mensal)   ─> MobPonto  (4 blocos de 10.000 linhas por IMPORTRANGE)
Mobponto (Presenças)      ─> Cadastros (cpf, nome, mes_atual.local)
```

⚠️ **O `MobPonto` da folha é montado em quatro `IMPORTRANGE` de 10.000 linhas cada**
(`A1:R10000`, `A10001:R20000`, …) — 40.000 linhas. É o teto prático de hoje, e
explica por que a planilha é lenta.

### 7.10.2 As colunas calculadas do `MobPonto` — o coração de tudo

| Col | Nome | O que faz |
|---|---|---|
| AG | HORAS TRABALHADAS | saída − entrada, escolhendo entre as marcações que existem |
| **AH** | **CTPS ou DIÁRIA** | ver §7.9 — já implementado |
| AI | CORRIGIR CONTRATO | Prestador com contrato CLT, ou CTPS com RPA/Estágio/PJ → erro de cadastro. E `início > admissão` → CORRIGIR ADMISSÃO |
| AK–AN | centro de custo de cada uma das 4 marcações | `VLOOKUP` da obra na `C. Diários` |
| **AO** | **CENTRO DE CUSTO do dia** | **`MODE.SNGL(AK:AN)`**, com a marcação de ENTRADA como desempate, e só quando é PRESENÇA ou PRESENÇA PARCIAL |
| AQ | PAGAR DIÁRIA / PAGAR EXTRA | ver 7.10.3 |
| AR | INFORMAR VALOR DIÁRIA | CTPS sem valor de diária cadastrado, com dia de DIÁRIA |
| AT | OBRA NÃO INFORMADA | bateu ponto (hora > 0) e a obra veio vazia |
| AU | FERIADO | `VLOOKUP` na aba `Feriados` |
| AV/AY | compensação | casa com a aba `Compensação` pela chave `cpf-data` |
| AW | FÉRIAS | `VLOOKUP` na aba `Férias`: o dia entre início e fim |
| AX | DEMITIDO | CTPS cujo dia é depois da Data de Saída |
| **AZ** | **VALOR POR DIA** | `VLOOKUP` do CPF na `Quinzena` ou `Fim de Mês`, coluna I |
| **BA** | **PAGAR** | o marcador (`Pagar QZ` / `Pagar FM`) puxado da aba do pagamento |

✔️ **A regra da obra do dia que eu implementei está CERTA**: é a moda das quatro
marcações, com a entrada como desempate. Confirmado pela fórmula `AO`.

### 7.10.3 `AQ` — quando o dia vira diária ou extra

- **Prestador de Serviço + Autônomo (RPA)** → sempre `PAGAR DIÁRIA`
- **CTPS cujo dia é DIÁRIA** (antes da admissão) → `PAGAR DIÁRIA`
- **CTPS em dia de CTPS, num sábado, domingo ou feriado** → `PAGAR EXTRA`
- **CTPS em dia de CTPS, de FÉRIAS** → `PAGAR EXTRA`
- **CTPS em dia de CTPS, já DEMITIDO** → `PAGAR EXTRA`

⚠️ **Isto responde o que ele perguntou sobre férias e feriados:** as duas regras
**existem e estão ativas** na fórmula. Ele achava que a de feriado não estava em
uso — está. O que talvez não esteja em uso é a aba `Feriados` estar preenchida.

### 7.10.4 `AJ` — QTD DIÁRIAS, e a MEIA diária

A fórmula devolve **1**, **0,5** ou vazio. Vale 1 quando há presença cheia; vale
**0,5** quando é `PRESENÇA PARCIAL` com horas acima de `0,2916` (= **7h00**, que
em fração de dia é 0,29166…), ou quando é falta justificada em sábado/domingo com
mais de `0,2708` (= **6h30**).

⚠️ **Dois limites de hora escritos como fração de dia, sem explicação na planilha.**
7h e 6h30. Vou implementar com o número e o motivo escritos; e é candidato a
pergunta: são esses mesmo?

### 7.10.5 As abas `Quinzena` e `Fim de Mês` — como o pagamento é montado

| Col | O que é |
|---|---|
| A | **link para o card do Pipefy** da pessoa (`HYPERLINK("https://app.pipefy.com/open-cards/"&…)`) — é o que ele pediu |
| C | Empregado (nome, colado da Folha Sintética) |
| F | valor líquido da pessoa; `F2` = soma de tudo |
| **G** | **CPF, por `VLOOKUP` DO NOME** no cadastro, excluindo "Colaboradores Desligados". Não achou → **"Checar Cadastro"** |
| H | **Dias** = `COUNTIFS` no MobPonto: mesmo CPF, coluna E (obra) não vazia, **diferente de "PAGAR EXTRA"**, e data no período |
| I | Valor x Dia = `F/H` |
| K1 | a data de fim do período, **calculada**: se hoje é dia ≤ 10, vale o **mês anterior** |
| L | centro de custo sugerido = `mes_atual.local` do resumo de presença |
| M | o marcador `Pagar QZ` / `Não Pagar` |
| O | **o arquivo de pagamento**: `QUERY` de nome, CPF e valor dos marcados |
| S | **"PT"** quando a pessoa tem **zero dias** de ponto; senão "OK" |
| T/U | **o total por centro de custo**: soma `AO` × `AZ` do MobPonto (dia a dia!) **mais** os "PT", cujo centro de custo vem da aba **`Página37`** |
| AA/AD | os códigos do Pipefy e do OMIE de cada centro de custo |
| AE | **a conta**, com `REGEXEXTRACT(...;"^[^,]+")` — ou seja, **pega até a primeira vírgula: uma obra pode ter mais de uma conta, e vale a primeira** |
| AF | **o bloco conta corrente × valor** que o Make lê |

#### ⚠️ Cinco achados que mudam decisões

1. **O cruzamento de hoje é POR NOME, não por ID Fortes.** A aba
   `CadastroColaboradores` importa 16 colunas e **o ID Fortes não está entre
   elas** — não há como cruzar por ID na planilha. Ou seja: **a planilha faz
   exatamente o que ele me disse para não fazer** (*"por nome pode ter homônimo e
   encontrar a pessoa errada"*). O sistema novo, cruzando por ID, **corrige um
   risco que existe hoje** — e isso explica o "Checar Cadastro" aparecendo.
2. **Dia com `PAGAR EXTRA` NÃO conta como dia da folha** (o `<>"PAGAR EXTRA"` do
   `COUNTIFS`). Faz sentido: aquele dia vai ser pago como extra, à parte. Eu não
   tinha isso.
3. **Quem tem zero dias ("PT") tem o centro de custo vindo da aba `Página37`** —
   uma tabela de 46 linhas, digitada à mão. **É a "regra de rateio" de hoje**, e é
   o que o cadastro de regras que eu fiz em 26/09 substitui.
4. **A competência é automática:** se hoje é dia ≤ 10, a folha é do **mês
   anterior**. A tela deve sugerir assim.
5. **O teto de 50 está explícito na planilha**, não só no Make: o link de envio só
   aparece se `COUNTA(V4:V) <= 50` **e** não houver nenhum "Checar Cadastro".

### 7.10.6 A aba `Pendências` — as críticas que existem HOJE

Esta é a lista de verdade, e ela vale mais que a minha de §6:

| Crítica | Como é detectada |
|---|---|
| Falta Data de Início ou Admissão | `AH = 'DT INÍCIO/ADMISSÃO'` |
| Pessoa não encontrada no cadastro | `AH = 'NÃO ENCONTRADO'` |
| Tipo de Cadastro × Tipo de Contrato incompatíveis | `AI = 'CORRIGIR CONTRATO'` |
| Admissão anterior ao início | `AI = 'CORRIGIR ADMISSÃO'` |
| Desligado com saída até o fim do mês anterior, ainda batendo ponto | fase = 'Colaboradores Desligados' e saída ≤ fim do mês anterior |
| Falta valor da diária | `AP = 'CORRIGIR VALOR DIÁRIA'` |
| Falta valor da diária para quem hoje é CTPS | `AR = 'VALOR DIÁRIA'` |
| Descadastrar (no Mobponto) | aba `Calendário`, `U = 'Descadastrar'` |
| **CTPS sem qualquer ponto** | aba `Calendário`, `U = 'Ponto CTPS'` |
| **Ponto sem identificação de obra** | `AT = 'OBRA NÃO INFORMADA'` |

⚠️ **Duas delas moram na aba `Calendário`**, que eu ainda não li. E a `Página37`
(o rateio manual), a `Feriados`, a `Férias` e a `Compensação` **não têm fórmula** —
são tabelas digitadas, então o script não as trouxe. **Preciso do conteúdo delas**,
e aí não é fórmula: é dado. Ver §7.11.

### 7.11 O que ainda falta para fechar o regramento

⚠️ **Atualizado em 27/09/2026.** Metade do que estava aqui foi respondido pela
leitura do documento de fórmulas da planilha de Diaristas/Extras/GM (§7.14) — que
**estava na pasta**; eu tinha errado ao dizer que faltava. O que sobrou:

#### Ainda preciso ver

| O que | Por que preciso |
|---|---|
| aba **`Página37`** da Folha de Pagamento - Fortes | é o rateio manual de quem não tem ponto. São ~46 linhas; ele pode colar aqui se confirmar que não tem dado pessoal |
| aba **`Calendário`** (abaixo da linha 60) | duas críticas saem dela (`Descadastrar`, `Ponto CTPS`) e o script de fórmulas lê só as 60 primeiras linhas |
| **conteúdo** (não fórmula) das abas `Feriados`, `Férias`, `Compensação` | as fórmulas eu já tenho (§7.14.6); o que falta é o dado, e isso vira **carga**, não cópia — `Férias` e `Compensação` têm CPF |
| fórmulas da planilha **`Diaristas Barbalha`** e da aba **`AnáliseSaídas`** | aparecem referenciadas e não foram lidas |
| ⚠️ **o nome exato das cinco colunas de auxílio** na aba `Dados Documentos` | são as únicas sem título confirmado (§7.15.3). O primeiro clique em "Atualizar cadastro" vai dizer se acertei: se não achar, a tela avisa com o cabeçalho de verdade |

#### Perguntas que só ele responde — e uma delas é dinheiro

| Pergunta | Por que importa |
|---|---|
| ~~**O transporte deve descontar feriado e férias?**~~ | ✅ **RESPONDIDO em 27/09/2026 — ver §7.16:** férias **sim**, proporcional; feriado de um dia **não**. Falta só o dado de férias para implementar |
| **`VIGIA` não recebe diária extra** — é regra ou remendo? (§7.14.5) | Está escrito só dentro de uma fórmula. Se for regra, vira cadastro; se for remendo, sai |
| **Os +20 / +10 / +20** de feriado, sábado e domingo continuam? (§7.14.5) | São valores fixos escritos na fórmula. Viram cadastro no sistema novo |
| **O que significa `E = "SIM"` na aba `Feriados`?** | Esse feriado deixa de contar. Suponho "foi trabalhado" ou "foi compensado" |
| **SomaPay × BeeVale: por pessoa ou por obra/CNPJ?** | Hoje é **por pessoa**, na coluna `I` da GM (§7.14.3). Ele descreveu por obra→CNPJ. São desenhos diferentes |
| **Os 1,5% da BeeVale** entram no valor do título? | Hoje o card vai **sem** a taxa; ela só aparece no texto (§7.12.4) |
| **Quem paga a aba `Cesta`** e por onde? | Tem marcação, não tem botão (§7.12.1) |
| **O Pipefy tem limite de linhas de rateio?** | As planilhas param em 30 (esta) e 50 (a da Fortes). Se o limite é do Pipefy, ele manda (§7.14.10) |

## 7.12 OS OUTROS SEIS PAGAMENTOS — lidos do script `Diaristas, Extras e GM` (27/09/2026)

Base: os seis arquivos de script da planilha **Folha de Pagamento - Diaristas,
Extras e GM** (`BeeVale.gs`, `Código.gs`, `Code.gs`, `BloqueioPGT.gs`,
`onOpen.gs`, `Sem título.gs`), lidos linha por linha. Isto é **código lido**, não
relato — o que for suposição está marcado.

### 7.12.1 Uma planilha, seis abas, quatro botões

A planilha `1Q39sdTbZ4edNthTU3HsCc8ahkBLWfqOcffbBXp3_RI8` tem as abas
`CTPS`, `Diaristas`, `GM`, `Cesta`, `Alimentação` e `Transporte`. O menu
*Gerar SP* oferece **quatro** botões: BeeVale Transporte, BeeVale Alimentação,
BeeVale GM e BeeVale Diaristas. Ou seja:

- `Cesta` tem marcação (`R4:R`, "Pagar QZ") mas **não tem botão** no menu.
- `CTPS` (as diárias de quem tem carteira assinada) tem marcação (`P4:P`) e tem
  o botão de **bloqueio** (`openUrlBloqueiaPGT`), mas **não tem botão de gerar
  pagamento** neste script. Ou o pagamento dela sai por outro caminho, ou a aba
  entra num dos quatro botões por fórmula. **Não confirmado.**

Todas as abas começam os dados na **linha 4** (cabeçalho na 3).

### 7.12.2 O marcar/desmarcar em massa — e uma inconsistência real

`Código.gs` tem um par marcar/desmarcar por aba, que preenche a coluna de
decisão de cima a baixo:

| Aba | Coluna preenchida | Valor escrito | Até a linha |
|---|---|---|---|
| CTPS | P | `Pagar` / `Não Pagar` | 500 |
| Diaristas | **N** | `Pagar` / `Não Pagar` | 600 |
| GM | O | **`Pagar QZ`** / `Não Pagar` | 500 |
| Cesta | R | **`Pagar QZ`** / `Não Pagar` | 500 |
| Alimentação | T | `Pagar` / `Não Pagar` | 500 |
| Transporte | T | `Pagar` / `Não Pagar` | 500 |

**Defeito encontrado:** a função `Entrada()`, que marca tudo de uma vez no
começo do processo, escreve em `Diaristas` na coluna **M**, enquanto
`DiaristaMarca()` escreve na coluna **N** — e é a **N** que o gerador de
pagamento lê (`COL_PAGAR: 14`). Quem usar o `Entrada()` marca a coluna errada e
a rodada sai **vazia** (ou pior, sai com o que sobrou de uma rodada anterior).
Sinal típico de uma coluna inserida na planilha sem atualizar o script.

**Segundo ponto de atenção:** o gerador filtra por `pagar === 'Pagar'`, texto
exato. GM e Cesta são marcadas com `'Pagar QZ'`. Para GM funcionar, a coluna que
o gerador lê (`BB`) tem de ser **outra** que a coluna marcada (`O`) — provável
fórmula que traduz `Pagar QZ` → `Pagar`. ⚠️ **RESPONDIDO, e era pior: ver §7.14.2.** A fórmula de `BB` **não olha a
marcação nenhuma** — e tem um defeito que faz o "Não Pagar" por pessoa não
funcionar.

### 7.12.3 Como o pagamento BeeVale é montado (o mesmo motor para as 4 abas)

1. **Lê a aba** a partir da linha 4, com `getDisplayValues()` — ou seja, lê o
   **texto formatado**, não o número. É por isso que existe um conversor de
   número brasileiro no script.
2. **Filtra** `Pagar` **e** conta preenchida. Linha marcada sem conta é
   silenciosamente ignorada — **não há aviso**. Essa é uma crítica que o
   sistema novo deve dar.
3. **Consolida por conta + CPF + categoria de despesa.** Duas linhas da mesma
   pessoa, na mesma conta e na mesma categoria, viram **uma**, somando o valor.
   Se essas linhas tiverem **centros de custo diferentes**, o script fica com o
   **primeiro** e só escreve um aviso no log técnico (que ninguém lê) — o
   dinheiro da segunda obra é apropriado na primeira. **Isto é perda de
   informação de rateio, e é silenciosa.** No sistema novo isso tem de ser uma
   crítica na tela, não um log.
4. **Pergunta a conta** numa caixinha de texto: a pessoa **digita** o nome da
   conta, ou `TODAS`. Digitar errado aborta.
5. **Um arquivo de pagamento por conta**, com 11 colunas fixas:
   `Nome, Email, Carteira, Benefício, Valor, Tipo de Recarga, Dias úteis,
   Documento de Identificação, Nome Completo, Centro de Custo, Categoria`.
   - O **e-mail é inventado**: `<cpf só dígitos>@bwsconstrucoes.com.br`.
   - `Benefício` = `Livre`, `Tipo de Recarga` = `Mensal`, `Dias úteis` = `0`,
     `Categoria` = `BWS`, `Carteira` = o que vier da planilha ou `Produção`.
   - O CPF vai **formatado** (`000.000.000-00`) na coluna de documento.
6. **Confere três vezes** antes de subir o arquivo (isto é bom e vale copiar):
   soma em memória × soma esperada; relê a planilha e soma de novo; e
   **descompacta o próprio .xlsx exportado** para checar se os CPFs esperados
   estão lá dentro. Esse terceiro teste existe porque o export do Drive **já
   entregou o arquivo de outra conta** — está escrito no comentário do script.
7. **Sobe dois arquivos ao Dropbox** (pasta `/BWS DP/DESPESAS COM
   COLABORADORES/Pgt Conjunto`): o arquivo de pagamento e uma **cópia da
   planilha de análise** (`1LoTJtYKHpSuxnvr03tBvxjpLK6c2Wvuyo4YbVk3IYEk`)
   exportada em .xlsx.
8. **Cria UM card no Pipefy por conta**, com o rateio por centro de custo e por
   categoria em **percentual com 7 casas decimais**, ajustado para fechar 100%
   exatos (sobra distribuída pelas maiores frações). O card vai com
   `autoriza_o_dupla: SIM` e `anu_ncia_sp: Sim`.
9. **Avisa por WhatsApp** (Z-API) dois telefones, com os links dos arquivos.
10. **Registra** em três abas da própria planilha: `LogSP` (log da execução),
    `HistoricoBeeVale` (uma linha por conta) e `HistoricoBeeValeItens` (uma
    linha por pessoa). Isso é exatamente o log que o dono pediu para o sistema
    novo — e a estrutura já está provada na prática.

### 7.12.4 Os 1,5% da BeeVale

O script calcula `Valor BeeVale = Valor × 1,015`, **truncado** em 2 casas (não
arredondado). Esse valor vai na descrição do card como referência, mas o campo
`valor` do card é o valor **original**, sem os 1,5%. Ou seja: a taxa é
informada, não cobrada no título. **Confirmar com o dono** se é isso mesmo, ou
se o título deveria sair com a taxa.

### 7.12.5 O bloqueio de pagamento da aba CTPS

`openUrlBloqueiaPGT` confere a célula `AC6` da aba `CTPS` (tem de estar `OK`),
mostra o texto de `AC4` para a pessoa confirmar, abre a URL de `AC7` — que é um
webhook do Make — espera 20 segundos e **apaga a coluna Q inteira**
(`Q4:Q`). Se `AC6` não estiver `OK`, mostra a mensagem de `A62`.

⚠️ **CORRIGIDO em §7.14.1: NÃO é uma trava de pagamento.** A fórmula de `AC6`
só confere se alguém escreveu o bloqueio na coluna `Q` — é campo obrigatório, não
regra de negócio. Eu supus regra onde havia formulário.

O `Utilities.sleep(20000)` antes de limpar a coluna Q é uma aposta: se o Make
demorar mais de 20 segundos para ler a planilha, o dado é apagado **antes** de
ser lido. É frágil por construção.

## 7.13 PJ e PRÓ-LABORE — lido do segundo blueprint (27/09/2026)

Base: `DP - FIN - Botão Folha de Pagamento (🆕SP)`, blueprint 2, lido módulo por
módulo.

### 7.13.1 O caminho

`webhook` → descarta `acao = atualizarobra` → descarta `grupo = Despesa com
Colaboradores`, `acao = planilhadeanalise`, `grupo = cestabasica`,
`grupo = bloqueiopgt`, `acao = dcbeevale` → se `tipo = SP`, lê a aba **`GM`**,
faixa **`AC4:AS500`** → agrupa → **um card por pessoa** → avisa Luelia e
Marcelo (WhatsApp Z-API **e** Telegram, os dois) → devolve uma página HTML de
"pronto".

Note a diferença de desenho: **BeeVale faz um card por conta; PJ/Pró-labore faz
um card por pessoa.** São regras de negócio diferentes, não inconsistência — o
PJ tem nota, CNPJ e credor próprios.

### 7.13.2 O que vai no card

Da faixa lida, usa as posições 0–6 e 16, que em coluna são
`AC, AD, AE, AF, AG, AH, AI` e `AS`:
nome, CPF, valor, centro de custo, código do centro de custo, CNPJ, razão
social, e conta de origem.

- `local` = o **nome da pessoa** em maiúsculas.
- rateio múltiplo = **Não**; um centro de custo só.
- tipo de despesa fixo (`383928967`).
- `valida_o_sp_1: Sim`, `lan_amento_via_api: Sim`.

### 7.13.3 O defeito grave: como ele decide PF ou PJ

O blueprint decide pessoa física × jurídica assim: **se o texto do CNPJ tem 14
caracteres, é Pessoa Física; senão, Pessoa Jurídica.**

Isso funciona **só enquanto o documento vier formatado**: um CPF com pontuação
(`000.000.000-00`) tem 14 caracteres, e um CNPJ com pontuação
(`00.000.000/0000-00`) tem 18. Mas um **CNPJ sem pontuação tem exatamente 14
dígitos** — e seria classificado como **Pessoa Física**. O card sairia com o
CNPJ preenchido no campo de CPF e na chave Pix, e com o título errado.

No sistema novo isso se resolve pelo **número de dígitos** (11 = CPF, 14 =
CNPJ), nunca pelo tamanho do texto. Já é assim no `folha_rateio.cpf_valido`.

## 7.14 AS FÓRMULAS DA PLANILHA DE DIARISTAS, EXTRAS E GM (27/09/2026)

O documento de fórmulas **estava na pasta** — eu tinha errado ao dizer que
faltava. São 26 abas. O que segue é **lido da fórmula**, não suposto. Isto
responde quatro perguntas que eu havia deixado abertas em §7.11 e derruba duas
coisas que eu tinha escrito em §7.12.

### 7.14.1 ⚠️ CORREÇÃO: a "trava de pagamento" da aba CTPS não é trava nenhuma

Em §7.12.5 eu escrevi que existia uma trava de pagamento governada por uma
fórmula invisível em `AC6`. **Está errado.** A fórmula é:

- `AB` = para cada linha com a coluna `Q` preenchida, monta um texto
  "nome dia/mês/ano - dia da semana - <o que foi escrito em Q>".
- `AC4` = junta todas essas linhas numa só.
- `AC6` = `se AC4 está vazio → "Informe na Coluna Q o bloqueio de PGT"; senão → "OK"`.

Ou seja: o botão **não decide** se o pagamento pode sair. Ele só **recusa
disparar sem que alguém tenha escrito o bloqueio na coluna Q**. É um campo
obrigatório, não uma regra de negócio. `AC7` é a URL do Make montada com esse
texto.

**O que isso muda no desenho:** eu ia procurar uma regra que não existe. O
bloqueio é **digitado à mão**, por pessoa e por dia. No sistema novo isso é um
campo de observação na linha, com o mesmo efeito — e sem apagar a coluna depois
(hoje o script espera 20 segundos e apaga a `Q`; se o Make atrasar, o dado vai
embora antes de ser lido).

### 7.14.2 ⚠️ CORREÇÃO: a coluna `BB` da GM ignora a marcação — e tem um defeito

Em §7.12.2 eu supus que alguma fórmula traduzia `Pagar QZ` → `Pagar`. **Não
traduz.** A fórmula é:

```
BB = se R (CPF) está vazio → ""; senão → se BA4 está vazio → "Não Pagar"; senão → "Pagar"
```

Duas coisas graves aqui:

1. **A marcação da coluna `O` (`Pagar QZ`) não entra nesta conta.** Quem decide o
   pagamento **BeeVale** da GM é só "tem CPF e tem centro de custo resolvido".
   O `Pagar QZ` que o DP marca alimenta **outra** lista — a do **SomaPay**
   (`Q3`: `WHERE O = 'Pagar QZ' and M > 0 and I != 'Sim'`). São **dois portões
   diferentes na mesma aba**, e só um obedece a marcação.
2. **A fórmula é um `ARRAYFORMULA` que olha só a linha 4.** Ela varre `R4:R` mas
   compara `BA4` — fixo, sem `:`. O resultado da linha 4 é **copiado para todas
   as outras**. Se a linha 4 tiver centro de custo, **todo mundo vira "Pagar"**;
   se não tiver, todo mundo vira "Não Pagar". O "Não Pagar" por pessoa,
   nesta coluna, **não funciona**.

No sistema novo o desmarcar é por pessoa, guardado no banco, e o arquivo de
pagamento sai da mesma decisão que a tela mostra — não de uma segunda fórmula.

### 7.14.3 RESPOSTA: SomaPay ou BeeVale já é escolha — é a coluna `I`

Ele perguntou se daria para escolher entre pagar pela BeeVale ou pela SomaPay.
**Na GM isso já existe:** a coluna `I` da aba GM decide, por pessoa.

- `I = 'Sim'` → entra na lista **BeeVale** (`AC3`).
- `I != 'Sim'` → entra na lista **SomaPay** (`Q3`).

Então a liberdade que ele quer não é nova: é a que a planilha já tem, e que o
sistema deve manter — **por pessoa**, não só por conta. Vale confirmar com ele se
a escolha deve continuar por pessoa ou passar a ser por obra/CNPJ, como ele
descreveu.

### 7.14.4 A CONTA CORRENTE sai do NOME da obra — por expressão, com um perigo

A aba CTPS decide a conta corrente assim (coluna `AI`), a partir do nome do
centro de custo:

| Nome do centro de custo começa com | Conta |
|---|---|
| `BIBLISOBRAL` | 7011-4 |
| `MERCADOBARBALHA` | 22069-8 |
| `IFPESANTACRUZ` | 2541-0 |
| contém `BIBLI`, `IFSP`, `ESCABREU`, `CREFOSFATO`, `ESCCAETES`, `CABOPONTE` | 50302-9 |
| contém `AREPE`, `CREPE`, `ESCCYNIRA`, `ESCBOAVISTA`, `CTBELOJARDIM`, `CREATAPUZ`, `CRESAOLOURENCO`, `CREPEDRAS`, `ESCVARZEA` | 50024-0 |
| **qualquer outro** | **7011-4** |

⚠️ **O perigo está na última linha.** Obra nova, com nome que não casa com nenhum
desses pedaços, vai para a **7011-4 sem avisar ninguém**. Não dá erro, não dá
aviso: paga pela conta errada e ninguém fica sabendo. As demais abas (GM,
Alimentação, Transporte) resolvem a conta por uma **tabela** (`C. Diários`
coluna G), que é o caminho certo.

**No sistema novo:** a conta vem da obra, pela tabela, e **obra sem conta
definida é crítica que segura o arquivo** — nunca um padrão silencioso.

### 7.14.5 O valor da diária de quem tem carteira assinada (aba CTPS)

A aba CTPS é o pagamento de **diária extra a quem é CTPS** — o que ele chamou de
"gente que é carteira assinada, mas que trabalhou um feriado, um final de
semana". A lista sai do ponto com **três filtros**:

```
dias entre o início e o fim do período
  e  AQ = 'PAGAR EXTRA'
  e  AJ (qtd de diárias) > 0
  e  função != 'VIGIA'
```

**A exclusão do VIGIA eu não conhecia, e ela não está escrita em lugar nenhum.**
Precisa ser confirmada: é regra (vigia tem escala e não recebe extra) ou é
remendo antigo?

**E o valor de cada dia (coluna `O`) tem adicional por tipo de dia:**

| Dia | Valor pago |
|---|---|
| Feriado | qtd × valor **+ 20** |
| Sábado | qtd × valor **+ 10** |
| Domingo | qtd × valor **+ 20** |
| Dia comum | qtd × valor |

E, antes de tudo: **se o dia tem compensação, não paga** — a linha sai em branco
quando a coluna `Compensação` está preenchida. A compensação é buscada por
`CPF-data` na aba `Compensação` (que ainda marca `DUPLICADO` quando a mesma
pessoa tem duas compensações no mesmo dia).

Esses +20/+10/+20 são **dinheiro**, e nenhum deles estava no meu levantamento.

### 7.14.6 Auxílio alimentação — a regra completa

- **Quem entra:** cadastro que não está em `Colaboradores Desligados` **nem em
  `Colaboradores Afastados`**, e que tem valor de alimentação preenchido no
  cadastro. (Confirma o que ele disse: o valor vem do cadastro.)
- **O período é o MÊS INTEIRO**, do dia 1 ao último — não quinzena.
- **Quatro modalidades**, e cada uma tem sua base de dias:
  - `Mensal` → todos os dias do mês **− 3**
  - `Segunda à Sexta` → dias úteis (sáb/dom fora)
  - `Segunda à Quinta` → dias úteis **menos as sextas**
  - `Mês` → valor fixo, não conta dia
- **Dos dias, desconta:** feriados nacionais, feriados estaduais/municipais **do
  município da pessoa**, e os **dias de férias** — e soma um ajuste manual.
- **Férias entra de verdade:** é buscada na aba `Férias` por `CPF-mês/ano`, e o
  que se desconta é **dia útil dentro do período**, não dia de calendário. Ele
  perguntou se as férias eram checadas: **são.**
- **A sutileza que é fácil errar:** para quem é `Segunda à Quinta`, os feriados
  que caem numa **sexta** são somados de volta — porque aquela sexta já não
  contava. É para isso que a aba `Feriados` conta feriado de sexta à parte. Quem
  reescrever isso sem saber vai descontar duas vezes.
- **Feriado com `E = "SIM"` não conta.** (Provavelmente "foi trabalhado" ou "foi
  compensado" — **confirmar o significado da coluna E**.)
- Valor: `Mês` → valor fixo + ajuste; nas outras → valor do dia × dias.

### 7.14.7 Auxílio transporte — quase igual, com UMA diferença que é dinheiro

Transporte é a mesma planilha da alimentação, com `Despesas com Transporte` no
lugar da categoria, e **duas diferenças**:

1. **Quem tem `Cartão` no cadastro não entra** (só quem recebe em dinheiro).
2. ⚠️ **NÃO desconta feriado nem férias.** Os dias são só a base da modalidade.
   As colunas de feriado existem na aba, são calculadas… e **não entram na
   conta**. A alimentação desconta; o transporte não.

Isso quer dizer que **hoje se paga transporte de dia de férias e de feriado**. Pode
ser de propósito (vale-transporte tem regra própria) ou pode ser uma fórmula que
alguém esqueceu de completar. **Só ele decide** — e é a pergunta mais cara deste
documento, porque multiplica por ~500 pessoas todo mês.

Há ainda uma **inconsistência interna**: nas colunas de feriado da aba
Transporte, a **linha 4** usa uma fórmula e a **linha 5 em diante** usa outra (a
da alimentação, que desconta feriado de sexta). Sinal de fórmula copiada pela
metade.

### 7.14.8 O que a GM paga, e para quem

A lista da GM sai do cadastro, com este filtro:

```
não está em 'Colaboradores Desligados'
  e  (tipo = 'Autônomo Mensalista' ou 'PJ' ou 'Pró-labore' ou 'Estágio'
       ou a marcação P = 'Sim')
```

Ou seja: **GM é a folha de quem não passa pela contabilidade** — PJ,
pró-labore, autônomo mensalista, estágio — **mais** quem está marcado para
receber gratificação. Isso fecha com o que ele descreveu.

**Metade na quinzena, metade no fim do mês:** as colunas `J` e `K` dividem o
valor mensal por 2, e a coluna `M` escolhe qual usar pelo dia final do período
(dia 15 → quinzena; depois do 15 → fim de mês). Quem tem a marcação `H = "Sim"`
recebe **o valor inteiro** no fim do mês, em vez de metade.

Outras coisas úteis: a **chave Pix** sai da coluna 22 do cadastro, caindo para o
próprio CPF quando está vazia; a **razão social**, da coluna 21; a categoria é
sempre `Gratificações e Extras`.

### 7.14.9 O link para o card do Pipefy — ele pediu, e já existe

Ele pediu: *"eles estão associados a uma numeração no Pipefy, cada colaborador,
então é bom ter um link para clicar nele e ser direcionado, abre o card do
Pipefy."*

Todas as abas fazem isso na coluna `A`:
`https://app.pipefy.com/open-cards/` + a **coluna X (24) do
`CadastroColaboradores`**. É daí que sai o número do card de cada pessoa, e é o
que a carga do cadastro precisa trazer para o sistema novo poder montar o mesmo
link.

### 7.14.10 O teto de centros de custo é 30 aqui (e 50 na folha da contabilidade)

Todas as abas desta planilha recusam disparar acima de **30 centros de custo**,
com a frase "Mais de 30 Centros de Custo". Na planilha da Fortes o teto é **50**.
Dois tetos diferentes, nenhum dos dois escrito em lugar nenhum além da fórmula.
No sistema novo não há teto de planilha — mas **se o Pipefy tiver limite de
rateio, é ele que manda**, e isso precisa ser confirmado antes de gerar card com
mais de 30 linhas de rateio.

### 7.14.12 Diaristas — o espelho da CTPS, com duas diferenças

A aba `Diaristas` é montada igual à `CTPS`, do mesmo ponto, com o mesmo
`+20 / +10 / +20` de feriado, sábado e domingo. As diferenças:

| | CTPS (diária extra) | Diaristas |
|---|---|---|
| Filtro no ponto | `AQ = 'PAGAR EXTRA'` | `AQ = 'PAGAR DIÁRIA'` |
| Também exige | — | `AP != 'CORRIGIR VALOR DIÁRIA'` (pula quem está sem valor) |
| Exclui `VIGIA` | sim | sim |
| Desconta dia compensado | **sim** | **não** |
| Categoria | Pagamento de CTPS | `Salários e Ordenados` |

A **compensação só vale para a diária extra de quem é CTPS** — faz sentido (quem
é diarista não tem banco de horas), mas é uma assimetria que precisa ficar escrita,
senão alguém "conserta" para os dois lados e passa a descontar do diarista.

A conta corrente da aba Diaristas usa **a mesma expressão** da CTPS, com o mesmo
**padrão silencioso para a 7011-4** (§7.14.4). São **dois** lugares com a mesma
lista de obras escrita à mão: mudar uma obra obriga a lembrar das duas.

### 7.14.11 Dois detalhes que valem lembrar

- **`CONS` é especial.** O centro de custo `CONS` (consolidado/matriz) não passa
  pela tabela: tem IDs fixos no OMIE escritos direto na fórmula
  (`384052839` e `583753491`). Precisa virar cadastro, não número no código.
- **A competência é automática.** `Período!C14`/`D14`: se hoje é dia **10 ou
  antes**, a competência é o **mês anterior**; senão, o mês corrente. Mesma regra
  que já está em §7.10, agora confirmada nesta planilha também.

## 7.15 O CADASTRO ESPELHADO — feito em 27/09/2026

Pedido dele, e é a primeira peça da folha que virou tela de verdade:

> *"A planilha de cadastros dos colaboradores, tudo vem do Pipefy (…) às vezes é
> preciso fazer a alteração do auxílio de alimentação, do valor de um auxílio de
> transporte, ou um valor da gratificação. (…) o nome do colaborador já
> redireciona (…) eu preciso poder atualizar as informações que estão na análise
> SP que espelham o que está na planilha (…) um botão fácil."*

### 7.15.1 O caminho do dado, e a regra que dele decorre

```
Pipefy (o card)  →  automação  →  "Registro de Colaboradores" / aba
"Dados Documentos"  →  botão "Atualizar cadastro"  →  analisesps.colaborador
```

**Daí sai a regra da tela: ela NÃO edita.** O que fosse digitado aqui seria
apagado na atualização seguinte, sem explicação para ninguém. Então o que a tela
oferece é o contrário: **o nome da pessoa é link para o card do Pipefy**. Corrige
lá, aperta o botão aqui.

Isso responde a pergunta "onde o DP muda o valor de um auxílio": **no Pipefy,
como hoje**. O sistema novo não toma isso para si — e é melhor assim, porque o
card é onde mora o histórico e a aprovação.

### 7.15.2 As 30 colunas — a planilha escolheu, não eu

A aba `Dados Documentos` tem **78 colunas**. A aba oculta
`CadastroColaboradores` da planilha de Diaristas/Extras/GM — que é quem alimenta
alimentação, transporte, GM e diaristas — importa exatamente **30** delas:

```
Col1, Col5, Col23, Col24, Col25, Col26, Col47, Col49 … Col70, Col2
```

O espelho traz essas. Duas razões, e a segunda é a que importa:

1. **Custo:** trazer as 78 leria mais que o dobro de células a cada botão.
2. **Dado pessoal que não serve para nada aqui:** endereço, nome da mãe, RG,
   PIS e salário **ficam na planilha**. O que o sistema não busca não pode vazar
   por ele.

Para conseguir as duas coisas a leitura pede **faixas de coluna** (5 faixas), não
de A até a última — e há teste que falha se alguém trocar isso por uma faixa só.

### 7.15.3 O que o espelho decifrou das colunas sem nome

Cruzando a ordem da aba oculta com o uso nas abas de pagamento, dá para nomear o
que antes era só "Col65":

| Coluna da origem | O que é, pelo uso |
|---|---|
| `B` | **Nº Registro Pipefy** — é o número do card, e o que monta o link |
| `BM` | modalidade do auxílio **alimentação** (Mês / Mensal / Seg-Qui / Seg-Sex) |
| `BN` | **valor** do auxílio alimentação |
| `BO` | modalidade do **transporte** — e é aqui que `Cartão` quer dizer "não paga em dinheiro" |
| `BP` | **valor** do transporte |
| `BF` | **Valor da Gratificação** |
| `BH` | **Recebe Parcela Única** — `Sim` = valor inteiro no fim do mês, não metade |
| `BI` | **a marcação que escolhe BeeVale × SomaPay** (§7.14.3) |
| `AX` | **Fase Atual** — é onde mora `Colaboradores Desligados` e `Afastados` |
| `AY` | **Tipo de Contrato** — CLT, PJ, Pró-labore, Estágio, Autônomo Mensalista |

⚠️ **As cinco dos auxílios são as únicas cujo TÍTULO continua não confirmado.**
As fórmulas provam que existem e o que fazem, mas chegam por `IMPORTRANGE` de uma
faixa, sem nome. O módulo tenta os nomes prováveis e, **quando não acha, avisa na
tela** com o cabeçalho de verdade e pede o nome exato — em vez de gravar em
branco calado. **Campo de auxílio em branco vira pagamento a menos, e pagamento a
menos ninguém nota tão rápido quanto um a mais.** É o primeiro clique no botão
que vai fechar esse ponto.

### 7.15.4 Duas armadilhas da planilha que o código já desvia

1. **A linha 2 não é gente.** O cabeçalho está na linha 1 e a linha 2 guarda o
   **número de cada coluna** (1, 2, 3…), para uma fórmula com `INDIRECT` da aba
   `Dados Gerais`. Lida como dado, criaria um colaborador chamado "2" — que
   entraria nas listas de pagamento. Os dados começam na **3**, como a própria
   planilha faz (`QUERY('Dados Documentos'!A3:AX; …)`).

2. **O Sheets corta o fim vazio de cada faixa.** Uma faixa cujas últimas linhas
   estão em branco volta **mais curta** que as outras. Juntar por posição sem
   repor essas linhas faz **o auxílio de uma pessoa ir para o CPF de outra**, em
   silêncio. É o defeito mais caro possível aqui; tem teste próprio, conferido
   com mutação.

### 7.15.5 O que isto já destrava no desenho da folha

- **A crítica "fora do cadastro"** deixa de ser ideia e passa a ser possível: a
  tela de Rateio já marca o CPF que não está no cadastro. Uma regra de rateio com
  CPF errado rateia o salário de ninguém, e o erro ficava invisível até a folha
  não fechar.
- **Quem saiu sai das listas** pela data de saída, sem ser apagado — a folha de
  julho continua explicável.
- **O link do card** vale para todas as telas da folha que vierem: é sempre o
  mesmo número, no mesmo lugar.
- **Falta ligar** o espelho ao `folha_apropriacao` (hoje ele recebe o cadastro
  por parâmetro) e às futuras telas de alimentação, transporte e GM, que leem
  daqui o valor e a modalidade de cada pessoa.

## 7.16 FÉRIAS, FERIADO E DESLIGAMENTO — a decisão dele (27/09/2026)

Esta seção fecha a pergunta que estava aberta em §7.11 ("o transporte deve
descontar feriado e férias?") e abre um cuidado novo que ele levantou.

### 7.16.1 A resposta: sim, mas proporcional — e depende da modalidade

Palavras dele:

> *"O fato de se estar de férias e um feriado em dia de semana poderia sim afetar
> o cálculo do auxílio transporte. Isso depende também do tipo. Tem uns que são
> mensal. Um único dia não precisaria, mas férias, como são mais dias, sim,
> deveríamos proporcionalizar. É importante sinalizar que a pessoa está de
> férias."*

Traduzido em regra:

| Situação | Transporte | Por quê |
|---|---|---|
| **um feriado** no meio do mês | **não desconta** | um dia solto não move a conta o bastante para valer o risco de errar |
| **férias** (vários dias) | **desconta, proporcional** | são muitos dias; pagar o mês cheio é pagar transporte de quem não foi trabalhar |
| modalidade **mensal** | o desconto é **sobre o mês**, não por dia | quem recebe valor fechado não tem "valor do dia" para multiplicar |
| modalidade **por dia** (Seg-Sex, Seg-Qui) | desconta os **dias úteis** de férias | é o mesmo caminho que a alimentação já usa |

⚠️ **Isto é DIFERENTE do que a planilha faz hoje.** Hoje o transporte não
desconta nada (§7.14.7): as colunas de feriado existem na aba, são calculadas e
não entram na conta. A alimentação desconta feriado **e** férias. Então:

- **na alimentação**, a mudança é tirar o desconto de **feriado de um dia**?
  **NÃO — não mexer sem ele dizer.** Ele falou do transporte. A alimentação é
  refeição: faz sentido não pagar o feriado, porque não houve refeição. Não
  inferir dali uma mudança na alimentação.
- **no transporte**, a mudança é acrescentar o desconto de **férias**, e **não**
  o de feriado.

### 7.16.2 O que falta para fazer: o dado de férias

A regra está decidida e **não dá para implementar ainda**, porque o sistema não
sabe quem está de férias. Hoje isso vive na aba `Férias` da planilha, que o
sistema não lê.

Ele já disse o caminho:

> *"Depois vou ver o modelo de relatório de férias e de pessoas em demissão para
> importar pra ser feito a crítica."*

Então a ordem é: **relatório de férias → carga → sinalizar na tela → descontar no
transporte.** Sem o primeiro, os outros três não existem. A marca "está de
férias" que ele pediu entra junto com a carga, no mesmo lugar onde a marca de
desligamento já está (§7.16.3).

### 7.16.3 Desligamento: FEITO em 27/09/2026, e por que não esperou o relatório

Palavras dele, e é o cuidado mais caro desta área:

> *"Isso impacta em tudo, pois não podemos pagar esse tipo de verba indenizatória
> ou ainda pagar salário ou diárias pra quem saiu, tá saindo. Tem que ter
> cuidados e alerta."*

**Isto já deu para fazer sem o relatório**, porque o cadastro que vem do Pipefy
já traz **data do aviso prévio, último dia trabalhado, data de saída e a fase**.
O relatório que ele vai mandar será uma **segunda fonte**, para conferir uma
contra a outra — não é a primeira.

`colaboradores.situacao_no_pagamento(ficha, ate)` responde, num lugar só, para as
duas telas e para o gerador de pagamento que vier:

| Situação | Trava o pagamento? | Quando |
|---|---|---|
| **saiu** | **sim** | data de saída até o fim do período, **ou** a fase diz `Colaboradores Desligados` |
| **saindo** | não | saída depois do fim do período, último dia marcado, ou aviso prévio dado |
| **afastado** | **sim** (auxílios) | a fase diz `Colaboradores Afastados` |
| ativo | não | nada disso |

Três coisas nesse desenho que não são óbvias:

1. **A data que manda é o FIM DO PERÍODO que se paga, não hoje.** Pagar a
   quinzena de 1 a 15 no dia 20 é normal; quem saiu no dia 18 trabalhou a
   quinzena inteira e **recebe**. Usar "hoje" bloquearia pagamento devido — e
   bloquear o que é devido gera reclamação trabalhista, que é o outro lado do
   mesmo risco.
2. **A fase manda, e não fui eu quem decidiu.** As abas de alimentação e de
   transporte já excluem pela fase (§7.14.6). Uma segunda regra aqui criaria
   duas respostas para a mesma pergunta.
3. **O desacordo entre os sinais é notícia, não é para resolver calado.** Fase
   dizendo desligado sem data de saída, data de saída com a fase dizendo ativo,
   último dia já passado sem saída lançada — os três ficam escritos na tela.
   **Cadastro pela metade é por onde se paga quem já saiu**, e quem tem desacordo
   **continua aparecendo na lista**, marcado (esconder o caso inconsistente é o
   erro que ele corrigiu em 26/09/2026).

Na tela: um aviso no alto com a **contagem do cadastro inteiro** (não da lista,
que tem teto de 200), um filtro "só quem está saindo" que **traz quem já saiu**
junto, a frase do que não pagar em cada linha, e a linha destacada. Âmbar para
"está saindo" e cinza para "saiu": cores iguais fariam tratar do mesmo jeito dois
casos em que um tem pagamento legítimo.

### 7.16.4 O que a rescisão NÃO faz por aqui

Ele falou de **verba indenizatória**. Fica escrito, porque é limite de escopo:
**rescisão não se calcula nem se paga por este módulo.** O aviso da tela diz
isso com todas as letras ("rescisão não se paga por aqui"). O que o módulo faz é
impedir que a folha, a diária ou o auxílio do período saiam para quem já saiu —
e mostrar quem está saindo, para alguém conferir o que é devido até o último dia.

### 7.16.5 O que fica pendente desta conversa

| O que | Depende de |
|---|---|
| carga do **relatório de férias** | ele mandar o modelo |
| **marca "está de férias"** na tela | a carga acima |
| **desconto proporcional de férias no transporte** | a carga acima |
| carga do **relatório de demissão** (segunda fonte) | ele mandar o modelo |
| conferir uma fonte contra a outra (cadastro × relatório) | as duas cargas |
| **o feriado de um dia continua sem descontar no transporte** — decidido | nada; já é o comportamento |

## 7.17 ONDE OS ARQUIVOS DA FOLHA VÃO SER GUARDADOS (27/09/2026)

Pergunta dele:

> *"Você tá salvando as planilhas e relatórios onde? Quero que seja salvo no
> drive/dropbox, conforme o cenário do make. Fez assim? Isso os que são gerados
> e processados completo. Os que foram gerados só o relatório a partir da tela,
> não precisa salvar em canto nenhum."*

### 7.17.1 A resposta honesta: ainda não salvo em lugar nenhum, porque ainda não gero

**Nenhum arquivo da folha é gerado hoje.** Não existe gerador de arquivo SomaPay,
nem PDF, nem Excel da folha. O que existe é a leitura (Folha Sintética, ponto,
cadastro) e o cálculo (rateio, apropriação, vínculo). Então a pergunta é sobre
algo que ainda não foi escrito — e a regra dele fica valendo para quando for.

### 7.17.2 A regra, do jeito que ele definiu

| Tipo de saída | Guarda? | Onde |
|---|---|---|
| **arquivo de pagamento gerado e processado completo** (com card no Pipefy) | **sim** | Drive/Dropbox, como o Make fazia |
| **relatório tirado da tela** (PDF/Excel para olhar ou conferir) | **não** | vai direto para o navegador de quem pediu, e acabou |

A segunda linha já é como o módulo funciona hoje em `exportar.py`: monta e
devolve numa `Response`, sem passar por armazenamento. Nada a fazer ali.

### 7.17.3 ⚠️ O que descobri conferindo, e muda a conversa

**O módulo já sobe arquivo para o Drive hoje — e todo arquivo que ele sobe fica
PÚBLICO por link.** Está em `drive.subir_arquivo`: depois de criar e enviar o
conteúdo, ele faz uma terceira chamada concedendo `{"role": "reader", "type":
"anyone"}`. O link vai para a descrição do card do Pipefy.

Isso é o caminho da geração do **BeeVale** (`beevale.gerar`), que está em
produção e cujos arquivos têm **nome, CPF e valor** de quem recebe.

**Ou seja: é o mesmo risco que eu apontei no script da BeeVale (§8, itens 3 e
6) — só que dentro do nosso código.** E o comentário do próprio
`subir_arquivo` explica por que está assim: sem liberar por link, quem clica no
card recebe "sem permissão". Foi uma escolha para o fluxo funcionar, não
descuido.

⚠️ **Isto corrige uma frase minha de §8.** Eu escrevi que os itens 3 e 6
"morrem por construção" quando o arquivo passa a sair de dentro do sistema.
**Só morrem se a subida deixar de ser pública.** Do jeito que o upload está
escrito hoje, mudar de Dropbox para Drive não resolveria nada: trocaria um link
aberto por outro.

**Não mexi no `subir_arquivo`.** Mudar a permissão hoje quebraria os links dos
cards do BeeVale para quem clica sem conta Google na organização — é fluxo que
funciona, e a decisão é do dono, não minha. Fica como pergunta (§7.17.5).

### 7.17.4 O desenho que proponho para a folha (é novo, então nada quebra)

1. **Guarda no Drive**, na pasta que o módulo já tem configurada
   (`DRIVE_FOLDER_ID`, ou a que ele cola na tela de Configurações), numa
   subpasta por competência e tipo — `Folha/2026-09 quinzena/`.
2. **Sem liberar por link.** O arquivo fica na pasta, visível para quem tem
   acesso à pasta. Isso já é o time do DP e ele.
3. **O card do Pipefy recebe o link da TELA do sistema**, não do arquivo. Quem
   abre passa pelo login e pela permissão, e vê o relatório com o histórico —
   não um arquivo solto que pode estar velho.
4. **O nome do arquivo carrega competência, tipo e a hora da geração**, como o
   script da BeeVale aprendeu na dor: sem hora no nome, duas gerações no mesmo
   dia sobrescrevem a primeira e o card antigo passa a apontar para o arquivo
   novo (está escrito no comentário dele — "contaminando cards já criados").
5. **Guarda no banco o que foi gerado**: competência, tipo, quem gerou, quando,
   o total, e o identificador do arquivo. É o log que ele pediu em 26/09, e é o
   que permite gerar de novo sem duplicar.

**Por que Drive e não Dropbox:** o Drive já está funcionando aqui, com a mesma
credencial de serviço que lê as planilhas, e a pasta já é configurável pela
tela. O Dropbox **não existe no código Python** — só no Apps Script. Trazê-lo
significa credencial nova no Render e, antes disso, **trocar na origem a que
está hoje escrita dentro do script** (§8, item 4). Dá para fazer, mas é
trabalho a mais por um ganho que eu não sei qual é.

### 7.17.5 ✅ RESPONDIDO por ele em 27/09/2026

| Pergunta | Resposta dele |
|---|---|
| Drive ou Dropbox? | **Tudo Drive**, sem problema nenhum. O Dropbox sai do desenho — nada de credencial nova. |
| O arquivo pode deixar de ser público por link? | **Continua público, como está hoje.** Decisão dele, com o motivo: *"é um simples arquivo de folha de pagamento, isso vai transitar só dentro da empresa (…) e o que é que alguém vai fazer com isso? Pagar o funcionário?"* |
| O card recebe link da tela ou do arquivo? | **Do arquivo.** |
| Quantos arquivos? | **No mínimo dois:** o de **pagamento** e um de **análise da folha** — *"com as informações separadas, agrupadas, qual obra, qual funcionário, rateio de folhas, se for o caso"*. |

E ele acrescentou uma coisa que eu não tinha: **o log tem de ficar na aplicação**,
com as informações e o **link para baixar por lá** — não só no Drive e no card.
Ou seja, a tela guarda o histórico do que foi gerado, e é de lá que ele baixa.

#### ⚠️ Um fato sobre o link público, dito uma vez e registrado

Ele decidiu, e a decisão é dele. Fica só o registro de um ponto factual do
raciocínio, para quem ler isto depois: **o link público do Drive não depende de
ter acesso à pasta.** Quem recebe a URL baixa o arquivo sem conta nenhuma — o
"não é todo mundo que tem acesso a esse drive" vale para a pasta, não para o
link, e o link circula por WhatsApp e fica no histórico do card.

O risco é o de **exposição de dado pessoal em volume** (nome, CPF e salário de
~500 pessoas), não o de alguém pagar no lugar dele. **Segue como ele decidiu**, e
sem custo nenhum de implementação: é o comportamento que `drive.subir_arquivo` já
tem. Se um dia ele quiser fechar, é uma linha — e aí os links dos cards antigos
param de abrir para quem não tem conta Google da empresa.

## 7.18 A GERAÇÃO DOS ARQUIVOS — o que eu entendi, e o que ele acrescentou (27/09/2026)

Ele cobrou, com razão, que eu nunca tinha dito se havia **entendido** a geração
dos arquivos. Esta seção é a resposta, e vale como prova: o que está aqui saiu
do script e do blueprint, não de suposição.

### 7.18.1 O que eu entendi da geração de HOJE

**SomaPay** (o que a planilha da Fortes gera): um arquivo por **conta corrente**,
com uma linha por pessoa. É o caminho da folha da contabilidade.

**BeeVale** (`BeeVale.gs`, quatro botões — Transporte, Alimentação, GM,
Diaristas): para cada aba,

1. lê a aba a partir da linha 4, com o **texto formatado** (não o número);
2. filtra `Pagar` **e** conta preenchida — linha marcada sem conta é ignorada em
   silêncio (crítica que o sistema novo deve dar);
3. **consolida por conta + CPF + categoria de despesa**: duas linhas da mesma
   pessoa, mesma conta e mesma categoria viram uma, somando o valor;
4. pergunta **qual conta** (digitada, ou `TODAS`);
5. monta **um arquivo por conta**, com 11 colunas fixas — e o **e-mail é
   inventado** (`<cpf>@bwsconstrucoes.com.br`), `Benefício` = `Livre`,
   `Tipo de Recarga` = `Mensal`, `Dias úteis` = `0`, `Categoria` = `BWS`;
6. **confere três vezes** antes de subir, incluindo **abrir o .xlsx gerado** e
   checar se os CPFs esperados estão dentro — porque o Drive já entregou o
   arquivo de outra conta (está escrito no comentário do script);
7. sobe dois arquivos ao Dropbox (o de pagamento e uma cópia da planilha de
   análise) e **cria um card por conta** no Pipefy;
8. o rateio do card vai em **percentual com 7 casas**, ajustado para fechar 100%;
9. avisa dois telefones por WhatsApp e registra em `LogSP`, `HistoricoBeeVale` e
   `HistoricoBeeValeItens`.

**PJ e Pró-labore** (blueprint 2): lê a faixa `AC4:AS500` da aba `GM` e cria
**um card por PESSOA** — diferente do BeeVale, que é um por conta. Decide pessoa
física × jurídica pelo **tamanho do texto** do documento, o que é frágil
(§7.13.3).

**A "categoria" que ele mencionou** é a peça que eu já tinha mapeado e que
amarra tudo: cada aba busca três valores fixos por tipo de verba —
`Código Categoria da Despesa` e `Tipo de Despesa Omie` na aba `PlanoFinanceiro`,
e `Categoria BeeVale` na aba `DatabaseBeeVale`, pela descrição da verba
("Despesas com Alimentação", "Despesas com Transporte",
"Gratificações e Extras", "Salários e Ordenados"). É essa categoria que:

- entra na **chave de consolidação** (conta + CPF + categoria);
- vira a linha de **categorias do rateio** do card;
- define o **tipo de despesa** do card no Pipefy.

**Sim, está compreendido.** O que eu NÃO tenho é a tabela `DatabaseBeeVale` e a
`PlanoFinanceiro` em si (são abas ocultas de poucas linhas) — elas precisam virar
cadastro no sistema, e é carga, não cópia.

### 7.18.2 O que ele acrescentou: poder JUNTAR verbas no mesmo arquivo

> *"Normalmente a gente gera para cada tipo de verba, e para cada tipo de conta,
> um arquivo. Mas de repente eu quero gerar alimentação e transporte no mesmo
> arquivo do BeeVale. (…) ao invés de fazer três pagamentos, a gente faz só um.
> Então isso facilita. Então eu quero ter essa opção."*

A regra que fica:

| | BeeVale | SomaPay |
|---|---|---|
| juntar **verbas diferentes** no mesmo arquivo | **pode**, e ele quer poder escolher | — |
| o **mesmo CPF** aparecer duas vezes | **pode**, se a **natureza da verba** for diferente | **NÃO PODE** — o SomaPay recusa |
| um arquivo por **conta** | sim | sim |

⚠️ **A restrição do SomaPay é uma trava, não uma preferência.** Palavras dele:
*"o Soma tem uma particularidade, tem que ser separado, porque eu não posso
juntar no mesmo arquivo de pagamento mais de uma pessoa — quer dizer, aparecer
uma, o mesmo CPF duas vezes. Não pode. O Soma não aceita."*

Então o gerador tem de **conferir antes de gravar** e recusar com o nome de quem
está repetido. Um arquivo que o SomaPay rejeita depois de subir custa a rodada
inteira.

**O desenho que isso pede:** a tela oferece as verbas e as contas, a pessoa marca
o que quer junto, e o sistema:

1. agrupa **sempre por conta** (isso não é escolha — a conta define de onde sai o
   dinheiro);
2. dentro da conta, junta as verbas marcadas **se o destino for BeeVale**;
3. **se for SomaPay, separa por verba automaticamente** e explica na tela por que
   não juntou — em vez de deixar a pessoa marcar e receber um erro;
4. mostra, antes de gerar, **quantos arquivos vão sair e com que total cada um**.

### 7.18.3 O que ainda não existe, e por que

**Nada disso está implementado.** Não existe gerador de arquivo nenhum (§7.17.1).
Para existir, falta a peça que sustenta tudo: **a folha guardada no banco** — a
competência, as pessoas, os valores por verba e a apropriação por obra. Hoje o
cálculo existe em memória (`folha_apropriacao`), sem lugar para ficar.

A ordem, então, é:

1. **guardar a folha** (a migração e a tela de importação do arquivo da
   contabilidade);
2. as telas por verba (alimentação, transporte, diaristas, GM) — que é como ele
   trabalha hoje na planilha, uma aba por verba;
3. o **painel** com os totais por obra e por conta (§7.19);
4. os **geradores** de arquivo, com a escolha de juntar ou separar;
5. os **cards** do Pipefy, como passo separado e opcional (já decidido em 26/09).

## 7.19 O PAINEL QUE ELE PEDIU (27/09/2026)

> *"Tem que ter informação gerencial, né, tipo dashboard, para poder estar vendo
> qual é o total por obra, porque isso já ajuda nessa questão do rateio. (…) eu
> queria poder ter dashboard com os totais da folha, de alimentação e transporte,
> a folha da contabilidade, conseguir em um ambiente visualizar tudo, saber como
> é que está a distribuição por obra, por conta."*

**Isto não é enfeite: é entrada do trabalho dele.** Ele disse por quê — é olhando
o total por obra que ele decide o rateio do mês ("as obras que estão em
evidência"). Então o painel vem ANTES do rateio na ordem de uso, mesmo tendo sido
pedido depois.

O que o painel mostra, pelo que ele descreveu:

- **total geral** da folha do mês, e por **tipo de verba** (contabilidade,
  alimentação, transporte, diaristas, GM, PJ/pró-labore);
- **total por obra**, ordenado do maior para o menor — é a lista que responde
  "quais obras estão em evidência";
- **total por conta corrente** — é o que diz quantos pagamentos vão sair;
- os **somatórios** conferindo com a folha da contabilidade (o "fecha ou não
  fecha" que já existe em `folha_apropriacao`).

**Depende da folha guardada no banco** (§7.18.3, passo 1): não há como somar por
obra o que não está gravado. É o próximo passo, e é o que destrava tanto o painel
quanto as telas por verba.

## 7.20 A FOLHA LIGADA ÀS PESSOAS, E O PAINEL (27/09/2026)

Ele pediu para eu **seguir até o fim**. Isto é o que deu para fazer sem inventar
dado que não existe, e o §7.21 diz, item por item, o que ficou travado e em quê.

### 7.20.1 A ponte que faltava: o ID Fortes

⚠️ **A Folha Sintética não traz CPF.** Ela traz **código do empregado e nome**. E
o ponto, o cadastro, o rateio, os auxílios e o pagamento são **todos por CPF**.
Sem o de/para, a folha importada era uma lista de nomes — não dava para saber de
quem era cada valor.

O de/para mora numa aba própria da planilha "Registro de Colaboradores"
(`ID Fortes`), e agora vem **no mesmo botão "Atualizar cadastro"**: duas
atualizações separadas para a mesma planilha seria pedir para alguém esquecer
uma delas, e a folha passaria a não achar gente que ESTÁ cadastrada.

**Três cuidados que o código tem, e o motivo de cada um:**

1. **NÃO casa por nome.** As planilhas cruzam por nome hoje, e é frágil: dois
   "JOSE DA SILVA", um acento diferente, um nome do meio abreviado — e o salário
   vai para a pessoa errada. Aqui é pelo código; quem não tem código **fica
   pendente, visível**, em vez de casar com um parecido.
2. **O mesmo código para duas pessoas vira crítica**, e o primeiro vale. É o pior
   erro possível aqui: o salário de uma iria para a obra da outra.
3. **Aba vazia ou fora do ar não apaga o que já casou.** Zerar o de/para por
   causa de uma aba renomeada faria a folha inteira virar "pendente" de uma hora
   para outra.

E o campo **não entra na lista que a carga principal grava** — ele vem de outra
aba. Se entrasse, cada atualização do cadastro o sobrescreveria com vazio, e a
folha deixaria de achar as pessoas na atualização seguinte. Tem teste para isso.

### 7.20.2 As três críticas da folha, aplicadas ao arquivo de verdade

Com a folha ligada às pessoas, as regras que já existiam passam a valer **sobre a
folha**:

| Crítica | O que faz |
|---|---|
| **não casou com o cadastro** | fica na lista, marcada, e o valor dela aparece somado à parte. Sem CPF não há apropriação nem pagamento |
| **já saiu** | *"não podemos pagar salário ou diárias pra quem saiu"* — com o valor que está nesta folha |
| **está saindo** | aviso para conferir o que é devido até o último dia; rescisão não sai por aqui |

⚠️ **O período da folha é o que decide "saiu" × "está saindo"**, não hoje: quem
saiu no dia 20 trabalhou a **quinzena** (1 a 15) inteira e recebe; na folha de
**fim de mês** do mesmo mês, não. Tem teste com as duas folhas do mesmo mês
dando respostas diferentes para a mesma pessoa.

E as críticas ficam **antes da lista** na tela: são o que impede pagar, e ninguém
as encontraria rolando 491 linhas.

### 7.20.3 O painel — e por que ele é a primeira subtela

Ele explicou o motivo, e isso mudou a ordem: **é olhando o total por obra que ele
decide o rateio do mês.** Então o painel vem **antes** do rateio na ordem de uso,
mesmo tendo sido pedido depois. Quem entra na área cai nele.

O que ele mostra hoje: total das folhas importadas, pessoas, competências, quantas
pessoas **não casaram** com o cadastro (âmbar quando há alguma), e o **total por
filial, da maior para a menor, com o percentual de cada uma** — que é a lista que
responde "quais obras estão em evidência".

⚠️ **E a tela diz o que ainda não sabe, em cima, antes dos números.** Isso não é
modéstia: um painel que mostrasse "total da folha" sem avisar que faltam
alimentação, transporte, diaristas, gratificações e PJ faria o número parecer o
**custo de pessoal inteiro**. E o total por **obra** depende da apropriação, que
depende do ponto. Número que parece completo e não é vale menos que número nenhum.

## 7.21 O QUE FALTA, E EXATAMENTE EM QUÊ (27/09/2026)

Ele pediu para seguir até o fim. Três peças **não dependem de mim** — elas
dependem de dado que só ele pode trazer. Está tudo aqui para não parecer
esquecimento.

| O que falta | Depende de | O que já está pronto esperando |
|---|---|---|
| **total por OBRA** (no painel e no rateio) | o **ponto** carregado | `folha_apropriacao` inteiro: a apropriação por dia, mão > regra > ponto, a sobra de centavo |
| ~~**carga do ponto (Mobponto)**~~ | ✅ **FEITA em 27/09/2026 — ver §7.22.** Eu havia dito que era palpite escrever isso; **o contrato da API está nos Apps Script que ele mandou**, e eu não os tinha lido até o fim. Falta só criar as duas variáveis no Render | o módulo, a tela e 22 testes |
| ~~**tela de alimentação e de transporte**~~ | ✅ **FEITA em 27/09/2026 — ver §7.25** | — |
| ~~**marca "está de férias"** e **desconto proporcional no transporte**~~ | ✅ **FEITOS — §7.23 e §7.25.** A tela de férias é o lançamento; o relatório em arquivo entra depois, para facilitar | — |
| **tela de diaristas e de CTPS extra** | o **nome dos campos do ponto** | as regras (+20 feriado, +10 sábado, +20 domingo, VIGIA fora, compensação) estão em §7.14.5 e §7.14.12, e `folha_vinculo` já as implementa |
| ~~**gerar o arquivo de pagamento**~~ | ✅ **FEITO — §7.27**, com a trava do CPF repetido conferida antes de gravar | — |
| ~~**cards no Pipefy**~~ | ✅ **FEITOS — §7.29**, com os campos lidos do pipe em vez de escritos no código | — |
| ~~**log do que foi gerado, na aplicação**~~ | ✅ **FEITO — §7.28** (migração 035) | — |
| ~~**o gerencial por obra, conta e verba**~~ | ✅ **FEITO — §7.30**, no alto do Panorama | — |
| **conferir o layout do BeeVale e do SomaPay** | abrir o **primeiro arquivo gerado** antes de subir no portal | duas suposições estão nomeadas em §7.27.5 |
| **o de/para de categoria do card** (`DatabaseBeeVale`, `PlanoFinanceiro`) | são **abas ocultas** da planilha — precisam virar cadastro | o card já sai com descrição, valor e os dois links |

**A ordem que isso impôs, e o que sobrou** (atualizado no fim de 27/09/2026):

1. ~~**O ponto.**~~ Feito — falta só o **nome dos campos de cada dia**, que um
   clique na tela do Ponto revela. É o que ainda trava o total por obra vindo do
   ponto, os diaristas e a CTPS extra.
2. ~~**Feriados e férias.**~~ Feitos (§7.23).
3. ~~**A apropriação guardada.**~~ Feita (§7.26, migração 034).
4. ~~**Os geradores e os cards.**~~ Feitos (§7.27 a §7.29, migração 035).
5. ~~**O gerencial.**~~ Feito (§7.30).

**Sobrou UMA coisa que depende dele, e uma que depende de conferência:** o nome dos
campos do ponto (um clique), e abrir o primeiro arquivo gerado para conferir as duas
suposições de layout (§7.27.5).

⚠️ **CORREÇÃO DESTA SEÇÃO, feita no mesmo dia.** Eu escrevi aqui que não faria a
carga do Mobponto para não adivinhar o formato da API. **O formato não precisava
ser adivinhado: estava nos Apps Script que ele já tinha mandado**, e eu não os
havia lido até o fim antes de declarar a coisa travada (§7.22.1). A carga está
feita.

**O que continua verdade, e agora é preciso:** os **campos de cada dia** são
dinâmicos — o script do dono os descobre em tempo de execução, e ninguém escreveu
quais são. Um deles é a obra. Esse eu **não** adivinhei: o módulo guarda o dia
inteiro e **mostra na tela os campos que vieram de verdade**, com um dia de
exemplo. Com essa lista, ligar o total por obra é uma linha de código.

## 7.22 O PONTO — e a correção de uma coisa que eu disse (27/09/2026)

### 7.22.1 Eu disse que estava travado. Estava errado.

Em §7.21 eu escrevi que não escreveria a carga do Mobponto porque não podia
adivinhar o formato da API. **O formato não precisava ser adivinhado: ele está
nos Apps Script que o dono mandou.** Eu não tinha lido esses dois arquivos com
atenção suficiente antes de declarar a coisa bloqueada.

É a terceira vez na semana que eu erro na mesma direção — afirmar sobre uma fonte
que eu tinha em mãos e não li até o fim. Fica registrado.

### 7.22.2 O contrato da API, lido do script

Dois scripts, três endpoints, e as credenciais nos mesmos três cabeçalhos:

```
GET https://www.mobponto.com.br/ponto/api/endpoint.php
Headers: Authorization: Basic …   api-key: …   api-version: 1.0.0

type_data=FOLHA_BWS_EXCEL&status=false&mes=<M>&ano=<AAAA>&pagina=<N>
  → {"result": {"total_paginas": N,
                "funcionarios": [{"cpf", "nome",
                                  "relatorio": [{"dia", "matricula", …}]}]}}

type_data=REL_PRESENCA_BWS&tp_relatorio=M&dia_inicial=1&dia_final=<D>&mes&ano
  → {"result": {"funcionarios": [{"cpf", "nome",
                                  "mes_anterior": {"desc","qtde","local"},
                                  "mes_atual":    {"desc","qtde","local"},
                                  "mudou"}]}}

type_data=FUNCIONARIOS
  → {"result": [ …um objeto por funcionário, chaves dinâmicas… ]}
```

O primeiro é o ponto por dia, e é o que este módulo traz. Os outros dois ficam
registrados para quando forem necessários — o segundo tem o **`local`** (a obra do
mês) e o **`mudou`**, que respondem "esta pessoa trocou de obra".

### 7.22.3 O que EU não sei, e por que não inventei

⚠️ **Os campos de cada dia são dinâmicos.** O próprio script do dono os descobre
em tempo de execução (`Object.keys` do primeiro dia do primeiro funcionário que
tiver relatório). Ninguém escreveu em lugar nenhum quais são.

Um desses campos é a **obra**, e outros são as **quatro marcações** — é o que
`folha_apropriacao.obra_do_dia` precisa. **Adivinhar o nome deles decidiria em
qual obra cai o salário de 500 pessoas com base num palpite.**

Então o módulo faz o que é honesto:

1. **traz e guarda o dia inteiro como veio** (em JSON, na coluna `campos`);
2. resolve só o que dá sem adivinhar: a **data**, o **CPF** e a **matrícula**;
3. deixa `obra`, `presenca` e `falta` **nulas** — coluna vazia é pergunta aberta;
   coluna preenchida por palpite é resposta errada com cara de certa;
4. **anota quais campos vieram** e mostra a lista na tela, com **um dia de
   exemplo de verdade**.

Com essa lista na mão, ligar o total por obra é uma linha de código e nenhuma
suposição. E não é preciso recarregar nada da API depois: o dia inteiro já está
guardado, então uma migração preenche as colunas resolvidas a partir do que há.

### 7.22.4 O que o módulo cuida

- **Página por página**, gravando cada bloco antes de pedir o seguinte: o pico de
  memória fica em poucos MB, e um mês tem dezenas de milhares de dias.
- **Retentativa com espera crescente** (1s, 2s, 4s), como o script do dono — a
  API cai de vez em quando, e uma página perdida deixa buraco no mês.
- **401 e 403 não são repetidos:** credencial errada não melhora na terceira
  tentativa, e a frase diz o que conferir.
- **Página vazia no meio para a leitura**, em vez de insistir: é o sinal de fim
  que a API dá quando `total_paginas` vem otimista. E a tela marca o mês como
  **"veio pela metade"** — um mês incompleto mostrado como completo faria o total
  por obra sair a menos, sem ninguém saber.
- **Teto de páginas**, para a API dizer um número absurdo não travar o processo
  por horas.
- **Recarregar o mesmo mês substitui**, e o botão avisa antes.
- **Dia sem data legível continua guardado**, com aviso: jogar a linha fora
  esconderia o problema.

### 7.22.5 ⚠️ As credenciais, outra vez em claro

Os dois scripts do Mobponto trazem o `Authorization` e o `api-key` **escritos no
código**. Eu os vi ao ler o contrato — e **não os escrevi no repositório**.

O caminho é o mesmo de sempre: abrir o editor de script, copiar **direto para o
Render** (`MOBPONTO_AUTHORIZATION` e `MOBPONTO_API_KEY`), e **trocar a chave na
origem depois**. São as mesmas credenciais do item 1 de §8 — o que mudou é que
agora existe para onde levá-las.

Enquanto elas não existirem, a tela do ponto **não oferece o botão** e explica o
que fazer: botão que só dá erro é armadilha.
## 7.23 FERIADOS E FÉRIAS (27/09/2026) — e três coisas que eu expliquei mal

### 7.23.1 O que ele decidiu, e a tela que saiu

> *"Feriado nós vamos alimentar o sistema com os feriados (…) quais são os
> feriados que são nacionais, quais são os feriados que é por obra, porque as
> obras são em municípios diferentes."*
>
> *"Férias (…) você já cria uma telazinha onde eu vou inserir as férias de cada
> funcionário. Eu posso buscar pelo nome, pelo CPF e incluo o período. Só isso. E
> depois a gente pensa em numa forma de importar a informação via arquivo."*

Feito: uma subtela **"Feriados e férias"**, com os dois cadastros. **Uma tela para
os dois** porque são dois lançamentos pequenos que servem ao mesmo cálculo —
separá-los daria duas entradas para quem procura a mesma resposta, e ele acabou de
corrigir tela demais.

**As férias vêm primeiro na tela**, porque é o que ele vai usar todo mês.

⚠️ **A pessoa é ESCOLHIDA, não digitada.** O CPF é a chave de tudo: digitado na
mão, um número trocado lança as férias de outra pessoa — e o auxílio da certa sai
errado sem ninguém saber. A tela busca no cadastro por nome ou CPF e guarda o CPF
escondido. A busca devolve **só nome, cargo e se a pessoa saiu**: caixa de sugestão
não é lugar de mostrar salário nem auxílio.

### 7.23.2 Por OBRA, não por município — e por quê

Ele falou dos dois: *"por obra, porque as obras são em municípios diferentes"*.

Guardar por **município** exigiria um de/para obra → município que o sistema não
tem, e inventá-lo agora seria mais uma peça para dar errado. Guardar por **obra**
ele escolhe da lista que já existe (a mesma do rateio e do Ratear), e a conta sai
certa.

Se um dia o município entrar no cadastro da obra, o feriado municipal passa a valer
para todas as obras daquele município — **e esta tabela não muda**, só ganha uma
consulta nova.

### 7.23.3 As travas que o cadastro tem, e o que cada uma evita

| Trava | O que ela evita |
|---|---|
| o mesmo feriado não entra duas vezes | um 7 de setembro em duplicidade descontaria **dois** dias do auxílio de todo mundo |
| o mesmo dia pode ser feriado em duas obras diferentes | cada município tem o seu, e o dia pode coincidir |
| feriado "nacional" ignora a obra que vier | nacional com obra escrita deixaria a consulta ambígua |
| férias com o fim antes do início são recusadas | daria contagem negativa, e o auxílio sairia **a mais** |
| período maior que 400 dias é recusado | é quase sempre o ano digitado errado, e descontaria meses |
| dois períodos da mesma pessoa não podem se **cruzar** | descontariam o mesmo dia duas vezes, e o auxílio sairia a menos |
| dois períodos **encostados** (um acaba dia 15, o outro começa dia 16) podem | férias fracionadas são a regra, não a exceção |

### 7.23.4 Duas sutilezas da contagem que saíram de graça

A conta é **dia a dia** (no máximo 31 iterações), e não aritmética de semanas —
porque o mês começa e termina em dias da semana diferentes, e a conta fechada erra
na borda. Daí saem duas coisas certas sem esforço:

1. **Feriado que cai no fim de semana não desconta nada**, porque aquele dia já não
   contava.
2. **Feriado na sexta não desconta de quem é "Segunda à Quinta"** — a sexta já não
   contava para essa modalidade. É a mesma sutileza que a planilha resolve contando
   os feriados de sexta à parte (§7.14.6); aqui ela é consequência da conta.

E o desconto de férias é em **dias úteis**, não de calendário — como
`NETWORKDAYS.INTL` na planilha. Não se desconta refeição de um domingo que já não
era pago.

## 7.24 TRÊS COISAS QUE EU EXPLIQUEI MAL (27/09/2026)

Ele disse, com razão: *"eu não compreendi bem o que você botou falta"*. As três
respostas, em português:

### 7.24.1 "Total por obra" — a regra eu já tenho; o nome do campo é que falta

Ele explicou a regra achando que era isso que me faltava:

> *"Cada dia, eu determino qual obra mais aparece, já que são quatro marcações, e
> a que mais aparece é a que vai valer para o dia. Se tiver duas de uma e duas de
> outra, vamos considerar a primeira obra."*

**Essa regra está escrita e testada desde 26/09** (`folha_apropriacao.obra_do_dia`,
com o empate resolvido pela primeira marcação, exatamente assim).

**O que falta é outra coisa, e é banal:** quando a API do Mobponto devolve um dia,
ela manda um conjunto de campos com **nomes que ninguém documentou** — o script
antigo dele os descobre na hora de rodar. Eu preciso saber **qual desses nomes
carrega a obra de cada marcação**. Não é a regra: é o rótulo da caixa onde o dado
vem.

**E isso se resolve num clique:** a tela do Ponto mostra, depois do primeiro
carregamento, a lista dos campos que vieram e um dia de exemplo de verdade.

### 7.24.2 "Apropriação guardada" — jargão meu; quer dizer isto

**Apropriar** é dizer para qual obra vai o dinheiro de cada pessoa. Hoje o sistema
sabe **calcular** isso, mas o resultado morre na tela: não fica gravado.

**Guardar** a apropriação é ter isso escrito no banco, e serve para três coisas
que ele pediu:

1. o **relatório** de auditoria — "de onde veio este número" precisa de um lugar
   onde o número esteja;
2. o **arquivo de pagamento**, que sai da apropriação e não do cálculo do momento;
3. **o ajuste dele sobreviver.** Ele disse: *"às vezes eu distribuo em várias
   obras, bota um dia numa obra, um dia em outra (…) eu altero só a planilha
   naquele momento."* Se a apropriação não estiver guardada, esse ajuste se perde
   a cada vez que a tela recalcular — e ele teria de refazer.

### 7.24.3 Diaristas — sim, é o mesmo ponto

Ele está certo: *"é o mesmo ponto, a diferença do que para pagar o diarista é que
ele não é um carteira assinada. E os critérios para isso tem nas planilhas
também."*

**Os critérios já estão lidos e implementados**, das fórmulas:

- `folha_vinculo` traduz a fórmula da coluna AH, que decide **por dia** se aquele
  dia é **CTPS** ou **DIÁRIA** (§7.9);
- a diária sai de `AQ = 'PAGAR DIÁRIA'`, exclui quem está sem valor de diária e
  exclui a função **VIGIA** (§7.14.12);
- a diária extra de quem é CTPS sai de `AQ = 'PAGAR EXTRA'`, com **+20 em feriado,
  +10 no sábado, +20 no domingo**, e não paga dia compensado (§7.14.5).

Ou seja: **a tela de diaristas espera o mesmo mapeamento de campos do ponto que o
total por obra espera.** Um clique destrava as duas.
## 7.25 ALIMENTAÇÃO E TRANSPORTE — a tela, pronta (27/09/2026)

Uma subtela para as duas verbas, com uma aba para cada. Ele descreveu a planilha
tendo *"uma aba que eu trato alimentação, uma que eu trato transporte"* e, no mesmo
pedido, reclamou de tela demais no menu. A conta das duas é 90% a mesma; a
diferença é uma linha, e **fica escrita na tela**:

| | Alimentação | Transporte |
|---|---|---|
| desconta **feriado** | sim | **não** |
| desconta **férias** | sim | sim |
| quem tem **"Cartão"** no cadastro | recebe | **não recebe em dinheiro** |

Se ele preferir separadas, é uma linha em `SUBTELAS_DA_FOLHA`.

### 7.25.1 A conta, passo por passo, à vista

A tela mostra o **caminho inteiro** de cada pessoa: dias da modalidade, feriados
descontados, férias descontadas, o ajuste de mão, os dias que sobraram e o valor.
Não é enfeite — é o que ele pediu: *"saber até de onde é que foi que veio aquela
informação"*. Um total sozinho não se audita.

    Mês               valor fixo, não conta dia
    Mensal            todos os dias do mês − 3
    Segunda à Sexta   dias úteis
    Segunda à Quinta  dias úteis menos as sextas

    dias = base − feriados − férias + ajuste      (nunca negativo)
    valor = valor do dia × dias

### 7.25.2 O ajuste tem TRÊS estados, e não dois

"Segue o cálculo" **não** é "não pagar". Se a tela mostrasse os dois iguais, o
padrão do sistema pareceria decisão dele — e ninguém saberia mais quem decidiu o
quê. Por isso o campo é um seletor de três opções, e o banco guarda `NULL`, `true`
e `false` como coisas diferentes (migração 033).

⚠️ **Marcar "pagar" não vence falta de dado no cadastro.** Duas situações são
diferentes:

- **política** (saiu, está afastada, tem cartão): o valor DÁ para calcular, e a
  última palavra é dele — se marcar, paga;
- **impossível** (o cadastro não diz o valor ou a modalidade): não há valor nenhum
  a pagar, e marcar pagaria **zero em silêncio**, que é pior que não pagar. A tela
  recusa e diz o que consertar, e onde.

### 7.25.3 Quem precisa de mão vem primeiro

A lista é ordenada com os problemas **no topo**. Na primeira versão eu havia
escrito a chave ao contrário e os problemas iam para o fim da lista, onde ninguém
rola até — num cadastro de ~3.500 pessoas isso é o mesmo que esconder.

## 7.26 A APROPRIAÇÃO GUARDADA — o que era, e o que ficou pronto (27/09/2026)

Ele perguntou: *"apropriação guardada. Não entendi o que é isso aqui."* A §7.24
explicou; esta seção registra o que foi construído (migração 034).

São **duas coisas diferentes**, e confundi-las é o erro que a migração existe para
impedir:

| | O que guarda | Quantas linhas | Recalcula? |
|---|---|---|---|
| `apropriacao_ajuste` | **o que ELE mexeu à mão** | poucas por pagamento | não se aplica |
| `apropriacao` + `apropriacao_linha` | **o resultado de um pagamento já feito** | uma por pessoa × obra | **nunca** |

**Por que o ajuste fica separado do cálculo.** Se eu guardasse o resultado com o
ajuste dentro, uma correção no cadastro — ou um ponto que veio pela metade e foi
recarregado — deixaria de aparecer na tela, e ninguém entenderia por quê.

**Por que o resultado é congelado.** Depois que o arquivo foi para o banco, "qual
obra pagou o salário do Fulano em 09/2026" tem **uma** resposta, para sempre.
Recalcular isso faria recarregar o ponto de setembro, em outubro, mudar a história
de um dinheiro que já saiu — e o rateio do mês seguinte, que ele decide olhando o
total por obra, sairia sobre número que **não foi o pago**.

O ajuste guarda três caminhos, e só um por pessoa:

1. **tirar do pagamento** — exige motivo escrito. Não é burocracia: quem abrir o
   relatório três meses depois precisa saber por que faltou gente. "Sumiu" é a
   pior resposta possível num pagamento;
2. **tudo numa obra só** — o caso comum ("o ponto dele está errado, joga tudo na
   CREPEOLINDA");
3. **dividir obra por obra** — "um dia nesta obra, um dia naquela".

Os caminhos 2 e 3 **juntos são recusados**: deixariam o valor ambíguo, e a conta
usaria um enquanto a tela mostrava o outro.

## 7.27 OS ARQUIVOS DE PAGAMENTO — feitos (27/09/2026)

### 7.27.1 A divisão em arquivos, como ele pediu

| | BeeVale | SomaPay |
|---|---|---|
| um arquivo por **conta** | sempre | sempre |
| **juntar verbas** num arquivo | **pode**, e é escolha dele | **não** — separo sozinho |
| mesmo **CPF duas vezes** | pode, se a natureza da verba for outra | **trava** |

A tela **diz por que o SomaPay não junta**, em vez de deixar marcar e devolver erro
depois de ele escolher tudo. E a trava é conferida **antes de gravar o arquivo**,
com o CPF de quem está repetido: um arquivo que o portal recusa depois de subir
custa a rodada inteira — já se gerou, subiu, criou o card e avisou a equipe.

Dentro de cada arquivo, a consolidação é **conta + CPF + natureza da verba**, igual
à do `BeeVale.gs`. É ela que impede o CPF repetido sem perder dinheiro: duas linhas
da mesma pessoa, mesma conta e mesma natureza viram uma, somando.

### 7.27.2 De qual conta sai o dinheiro

**A conta vem da OBRA**, não da pessoa: obra → código → conta. As duas pontas já
existiam no banco — o nome e o código em `referencias_rateio`, a conta em
`contas_diarios` (a aba "C. Diários"). Obra **sem** conta não desaparece da lista:
entra com conta vazia e vira um aviso que diz onde consertar. Omiti-la faria o
recado virar "obra não existe", que é o problema errado.

### 7.27.3 São sempre DOIS arquivos

Pedido dele: *"tem que ter no mínimo o do arquivo de pagamento e um de análise da
folha, com as informações separadas, agrupadas, qual obra, qual funcionário, rateio
de folhas"*. Então:

- **o de pagamento** (um por conta) é para o PORTAL: três colunas no SomaPay, onze
  no BeeVale, nada que explique nada;
- **o de análise** é para GENTE, com quatro abas — Resumo, Por obra, Por
  funcionário e Rateio (percentual com sete casas, fechando 100% exato).

Um arquivo só, servindo aos dois, não serviria a nenhum: ou o portal recusa colunas
a mais, ou a conferência não tem o que olhar.

⚠️ **Os avisos entram no arquivo de análise**, não só na tela. Aviso que só existiu
na tela não explica diferença nenhuma três meses depois.

### 7.27.4 Só se paga apropriação FECHADA

Se o arquivo pudesse sair de um cálculo em memória, recarregar o ponto depois
mudaria a explicação de um dinheiro que já saiu. E verba **sem** fechamento recusa a
geração inteira: gerar pela metade sai com cara de completo, e a pessoa recebe a
menos sem nada avisando.

O botão de gerar só aparece **depois de conferir** — que é o passo que ele pediu:
*"mostrar, antes de gerar, quantos arquivos vão sair e com que total cada um"*.
Conferir não grava nem sobe nada.

### 7.27.5 O que está confirmado no layout, e o que é suposição

Isto importa, porque arquivo recusado pelo portal custa a rodada:

| Peça | De onde veio |
|---|---|
| SomaPay: aba `Valores`, três colunas, **CPF formatado** | do arquivo que o Make anexa ao card **hoje** — o que de fato é enviado e funciona (§2.1) |
| BeeVale: as 11 colunas, `Benefício` = Livre, `Tipo de Recarga` = Mensal, `Dias úteis` = 0, e-mail `<cpf>@bwsconstrucoes.com.br`, `Categoria` = BWS | do `BeeVale.gs` |
| **SUPOSIÇÃO:** o `Centro de Custo` do BeeVale — usei a obra | no fluxo das SPs é o número do card; para a folha a obra é o que faz sentido |
| **SUPOSIÇÃO:** o valor do SomaPay vai como **número** com máscara brasileira | o arquivo enviado mostra "1.126,60"; se o portal exigir TEXTO, é uma linha |

**As duas suposições têm de ser conferidas no primeiro arquivo gerado, abrindo o
.xlsx, antes de subir no portal.**

## 7.28 O LOG NA APLICAÇÃO (27/09/2026)

Pedido dele, com todas as letras: *"que tenha também o log na aplicação que a gente
está construindo aqui, com as informações e o link que a gente quer baixar por
lá"*. É a migração 035.

Hoje o histórico da geração está em três lugares que não conversam: a pasta do
Drive (que diz o nome do arquivo e nada mais), o card do Pipefy (que diz o link e
nada mais) e as abas `HistoricoBeeVale` da planilha. Para responder "este arquivo
foi gerado quando, por quem, com que total, e bateu?" é preciso abrir os três.

Agora é uma linha: competência, pagamento, destino, verbas, conta, pessoas, total,
quem gerou, o link do arquivo, o link do card e **os avisos que havia na hora**.

⚠️ **Gerar duas vezes deixa DUAS linhas**, de propósito. Apagar a primeira
esconderia que houve duas — e é justamente isso que alguém precisa ver quando o
portal recebeu dois arquivos.

## 7.29 OS CARDS DO PIPEFY (27/09/2026)

Lançar no Pipefy é um **segundo botão**, separado de gerar — decisão dele em
26/09/2026 (D14): assim ele gera e confere o arquivo sem sujar nada lá fora.

⚠️ **Os campos do pipe são lidos do Pipefy na hora, não escritos no código.** O
motivo é concreto: o blueprint do Make tem defeito conhecido de campo trocado (o
par 62 grava em `valor_centro_de_custo_63` — §4), e copiar a lista de lá copiaria
o defeito. Um valor de centro de custo caindo no vizinho **só aparece no
fechamento da obra, meses depois**.

Então: a tela tem um botão "conferir os campos do pipe" (só leitura, não cria
nada), que mostra o que eu reconheço e **o que não reconheço**. Campo não
reconhecido fica **vazio e dito**, nunca preenchido por parecença — campo parecido é
pior que campo vazio, porque vazio alguém vê e preenche, e errado ninguém vê.

**Dois defeitos meus, achados por teste durante a construção** — os dois da mesma
família, o campo parecido:

1. eu procurava o campo da planilha de análise por "planilha" + "an", e "an" está
   dentro de "**plan**ilha" — o campo da planilha de PAGAMENTO era reconhecido como
   o da análise, e o link errado iria para o campo errado. Corrigido para "analis".
2. eu procurava o campo do total por conter "valor", e o pipe tem **um** campo
   "Valor" e **setenta e cinco** "Valor Centro de Custo N" — o total da despesa
   poderia ser escrito dentro do valor de um centro de custo, que é **exatamente o
   defeito que o blueprint do Make tem hoje**. Corrigido: o rótulo **exato** ganha
   do parecido, e "centro de custo" desqualifica o campo.

Os dois com teste travando, e o segundo com o teste montando os campos na ordem que
fazia a busca errar.

O card recebe: a descrição inteira (competência, pagamento, verbas, conta, pessoas,
total, os **dois** links), o valor e os dois campos de link, quando existirem no
pipe. O arquivo de análise **não tem card próprio** — ele vai como link dentro do
card do pagamento.

E, seguindo a decisão dele (D15), o sistema **avisa e não impede** o lançamento
repetido: lançar o mesmo arquivo duas vezes é recusado (é distração), mas lançar a
mesma competência de novo é livre, porque o cancelamento do card antigo é feito por
ele, no Pipefy.

## 7.30 O GERENCIAL, FEITO (27/09/2026) — e por que ele sai do congelado

A §7.19 registrou o pedido. Isto registra o que ficou pronto, no alto do
**Panorama**: totais da competência **por obra**, **por conta** e **por verba**, mais
o percentual de rateio já calculado.

**Ele vem primeiro na tela**, antes do resto do painel, porque é a primeira coisa
que ele abre quando vai decidir o mês: *"saber qual é o total por obra, porque isso
já ajuda nessa questão do rateio"*.

⚠️ **E sai do que foi FECHADO, não de um recálculo.** Esta é a decisão de desenho
que importa: o rateio do mês seguinte se decide sobre **o que foi pago**. Se o
número viesse de um recálculo, recarregar o ponto de setembro — em outubro, por
qualquer motivo — mudaria o número embaixo de uma decisão já tomada, sem nada na
tela avisando.

Três detalhes que não são estética:

1. **Obra sem conta aparece marcada**, em "(sem conta)". Esconder faria a surpresa
   aparecer só na hora de pagar — e é justamente isso que trava a geração do
   arquivo.
2. **O percentual é o MESMO que vai no rateio do card** (sete casas, fechando 100%,
   sobra para a maior fatia). Duas contas de percentual em dois lugares divergiriam
   no primeiro arredondamento, e o card deixaria de bater com a tela.
3. **Mês sem nada fechado explica por quê**, em vez de mostrar tabela vazia. "Nada
   aqui" sem motivo faz a pessoa achar que o sistema quebrou.

## 7.31 A PRIMEIRA VEZ QUE ELE USOU AS TELAS (28/09/2026) — e o que estava errado

Ele abriu as telas da folha e a conclusão foi: *"no geral, eu estou achando tudo
muito ruim, muito ruim mesmo."* A lista era longa e toda justa. Está aqui inteira,
com o que mudou, porque cada item virou uma regra que não deve voltar.

### 7.31.1 O que fazia a tela de auxílio não calcular NADA

**A causa era um nome de coluna que eu chutei.** Ele ditou os certos, olhando a
planilha:

| Coluna | Nome de verdade | Eu havia chutado |
|---|---|---|
| BM | **Categoria** Auxílio Alimentação | "Modalidade Auxílio Alimentação" |
| BN | Valor Auxílio Alimentação | (certo) |
| BO | **Categoria** Auxílio Transporte | "Modalidade Auxílio Transporte" |
| BP | Valor Auxílio Transporte | (certo) |
| BQ | Observação | (não existia) |

Sem a categoria, **toda pessoa caía em "o cadastro não diz a modalidade"** — zero
dias, zero valor, tudo com cara de ajuste pendente. Era exatamente o que ele viu:
*"nenhum está dando valor, aí tudo está botando ajuste, aí zero dias, não está
calculando nada isso aqui."*

⚠️ **A lição, e ela já tinha sido paga duas vezes esta semana:** eu chutei o nome e
segui. O aviso "não achei a coluna X" existia e funcionou — mas ficava numa tela
que ele não abriu antes de usar a outra. Coluna que o sistema não acha tem de
aparecer **na tela que depende dela**, não só na de origem.

### 7.31.2 O layout: a barra errada

*"Está aqui desalinhado competência, aí está uns botões gigantescos da largura da
tela toda, o mês, o ano, aí tem o ver e ainda está desalinhado (…) está feio."*

Eu pus os campos direto dentro de `.barra-acoes`, que é a barra das **Solicitações**
— e ela é uma GRADE de três colunas (`1fr auto 1fr`), desenhada para "grupo à
esquerda / total no centro / grupo à direita". Cada campo solto virou uma célula e
esticou até a largura da tela. Agora existe `.barra-mes`, uma fileira compacta.

### 7.31.3 A obra é o CÓDIGO, e não se digita

Duas correções na mesma frase: *"em obra tem que colocar o código da obra e não a
obra por extenso. Todo mundo tem código da obra. Aqui tem vários vazios"* e *"eu
não sei por que você colocou um campo editável. Essa informação vem do cadastro."*

- A carga lia `Objeto Obra [ ]`, que é a obra **por extenso**. O **código** é outra
  coluna, e é ele que casa com a conta de pagamento e com o rateio. Agora são dois
  campos separados (migração 036), e as telas mostram o código.
- Quando o cadastro antigo não tem o código, o sistema chega nele **pelo nome**
  (`resolver_obra`). Não achando, fica vazio — vazio é pergunta aberta, e é melhor
  que um nome por extenso no lugar de um código.
- **O campo editável saiu.** Obra digitada na tela divergiria do cadastro e do
  rateio, e ninguém saberia qual das duas manda.

⚠️ **Um detalhe que quase passou:** o feriado por obra é cadastrado **pelo nome** (é
o que a tela de Feriados oferece). Passar o código no desconto de feriado faria o
feriado municipal deixar de descontar, em silêncio. O cálculo usa o nome para o
feriado e o código para agrupar.

### 7.31.4 O fluxo: selecionar e salvar de uma vez

> *"Fica muito dificultoso trabalhar da forma que está aqui, a gente vai gravando
> um por um. (…) A princípio tudo que está atendendo os critérios se paga. Ela
> exibe tudo que tem coerência, já faz o cálculo, já deixa tudo pronto. O que eu
> faço é só selecionar quem vai e quem não vai ser pago (…) e eu salvar como um
> todo, não linha a linha."*

Saíram: o seletor de três estados (*"que diabo é segue cálculo?"*) e o botão
**Gravar por linha**. Entraram: uma **caixinha por pessoa**, já marcada para quem
atende os critérios, um "marcar todos", e **um** botão Salvar numa barra colada
embaixo que só aparece quando há mudança não salva.

⚠️ **O QUE SE GUARDA É A EXCEÇÃO, NÃO A LISTA.** Se eu gravasse uma linha por
pessoa, o padrão ("paga") viraria uma decisão registrada — e no mês seguinte
ninguém saberia mais o que ele decidiu e o que o sistema calculou. Guardando só
quem ele desmarcou, a tabela tem três linhas em vez de quinhentas, e cada linha é
uma decisão de verdade.

Para isso o cálculo passou a devolver **as duas decisões**: `pagar_calculado` (o que
a conta diz sozinha) e `pagar` (o que vale depois do ajuste). Antes eu estava
adivinhando a diferença comparando textos de motivo — que quebra no dia em que
alguém reescreve uma frase.

⚠️ **A caixinha de quem não tem valor no cadastro fica DESABILITADA.** Marcar
pagaria zero em silêncio, e zero tem cara de resposta.

### 7.31.5 Os filtros, que faltavam em todas as telas

*"Era para ter uma na lateral aqui, filtro, para a gente poder estar filtrando. A
obra tal, pessoa tal. Eu preciso às vezes tratar só uma obra."*

O módulo **já tinha** a lateral de filtros (é o padrão das Solicitações e da
Documentação Fiscal) e eu declarei as telas da folha como `sem-filtros`. Agora
alimentação/transporte, colaboradores e diaristas têm filtro por **quem** (nome ou
CPF), por **obra** e por **situação**.

⚠️ **O filtro recorta a LISTA, nunca a CONTA.** Os totais do alto continuam sendo
os da verba inteira — filtrar a conta faria o total mudar conforme o filtro, e aí
ninguém saberia mais qual é o valor do pagamento.

### 7.31.6 Colaboradores: quem saiu não aparece mais — mas não desaparece

*"O que é colaborador desligado não deveria nem estar sendo exibido. Ele está
desligado, ele não está trabalhando."*

⚠️ **ISTO CONTRARIA UMA INSTRUÇÃO DELE DE 26/09** (*"não pode ficar oculto,
escondido"*, sobre o cadastro pela metade). A leitura que atende as duas: a lista do
dia a dia é de quem está **trabalhando**, e quem está desligado sai dela — mas a
conta aparece no alto da tela e uma caixinha o traz de volta. O que 26/09 proibia
era esconder **e não dizer**.

E a tabela ganhou uma frase dizendo **para que ela serve** (*"ninguém entende nem
para que serve essa tabela"*), a obra, o CPF pontuado e a linha clicável.

### 7.31.7 Clicar na pessoa: a ficha com o ponto do mês

*"Além de associar ao cadastro das pessoas, clicando abre o pipe — eu quero também
poder visualizar o ponto do mês daquela pessoa. Eu clicar e visualizar o ponto da
pessoa no modal, e de lá abrir o card."*

Feito, e a mesma rota serve as duas telas (auxílios e colaboradores): cadastro,
categoria e valor dos dois auxílios, **a observação da coluna BQ**, o ponto do mês
**dia por dia** e o botão do card.

⚠️ **O ponto aparece COMO VEIO**, sem interpretar. Enquanto o nome dos campos de
cada dia não for conhecido, inventar significado para eles poria o salário na obra
errada — e é justamente mostrar o dado cru que permite descobrir o nome certo.

### 7.31.8 O ponto: dizer se está carregando

*"A gente bota aqui trazer o ponto, mas aí se o ponto veio, se o ponto não veio, só
Deus sabe o que está acontecendo aqui em relação a essa API."*

A tela só acompanhava a carga se **você tivesse apertado o botão naquela aba**. Quem
abrisse depois — ou de outro computador — não via nada. Agora o estado vem do banco
junto com a página: ela já abre dizendo "está trazendo dado agora", com o passo, e
se atualiza sozinha quando termina. E se a carga foi **interrompida** (uma
publicação reinicia o serviço), a tela diz isso e diz o que fazer.

### 7.31.9 Os diaristas, que não existiam na tela

*"E cadê os diaristas? Não entrou diaristas."*

A regra existia e estava testada desde 26/09 (`folha_vinculo.py`, tradução fiel da
coluna AH). Faltava **ligá-la**, e faltavam duas colunas no cadastro: **Data de
Início** e **Data de Admissão** — sem elas todo dia caía em "falta data para
decidir".

A tela nova mostra, por pessoa, **quantos dias de diária e quantos de CTPS** no mês.
A regra é **por dia**, e isso fica escrito porque é contraintuitivo: a mesma pessoa
tem dias dos dois tipos no mês em que foi registrada.

⚠️ **O que ela AINDA NÃO faz é dizer quanto pagar**, e diz isso em vez de mostrar
zero: falta o **valor** da diária (não achei a coluna) e o nome dos campos do ponto,
que é o que diz a obra do dia e se foi sábado, domingo ou feriado (+10/+20).

### 7.31.10 A ordem das subtelas

*"Não tem lógica nas telas aqui (…) folha da contabilidade, alimentação, ambos são
PAGAMENTOS. Então acho que era para estar junto. O que é CADASTRO é para estar
junto."*

A ordem anterior era a ordem em que **eu** construí as peças. Agora são três grupos,
com o nome à vista na faixa de abas:

| Grupo | Telas |
|---|---|
| — | Panorama |
| **Pagamentos** | Folha da contabilidade · Alimentação e transporte · Diaristas · Arquivos gerados |
| **Cadastro e base do cálculo** | Colaboradores · Ponto · Feriados e férias · Rateio das obras |

### 7.31.11 Gerar o pagamento de dentro de cada tela

*"Eu não sei por que você botou gerar pagamento separado (…) na folha da
contabilidade eu gero o pagamento de lá, na folha da alimentação eu gero de lá."*

A tela de auxílio ganhou o bloco de geração. **A geração em si continua exigindo a
apropriação fechada** — e isso não é teimosia minha: é o que garante que o arquivo
enviado ao banco tenha uma explicação que não muda depois (§7.26). A tela de
"Arquivos gerados" continua existindo como **log**, que foi o que ele pediu em
27/09, não como a única porta.

### 7.31.12 O que ele pediu e AINDA não está atendido

| O que | Por quê |
|---|---|
| o **valor** do auxílio e da diária saindo certo para todos | depende de ele apertar "Atualizar cadastro" com os nomes de coluna novos — é o que traz categoria, valor, código da obra, observação e as duas datas |
| **total por obra vindo do ponto**, diária em dinheiro | depende do **nome dos campos de cada dia** do ponto |
| gerar o arquivo **de dentro** de cada tela, com um clique | depende da apropriação fechada, que depende do ponto |

## 7.32 SEGUNDA RODADA DE USO (29/09/2026) — e a confusão que eu mesmo criei

Ele voltou às telas e a lista veio maior. No fim: *"a verdade é que tem tanta
coisa, que até desanima."*

### 7.32.1 A confusão que custou a rodada inteira: aviso guardado sem data

Ele leu, na tela de Colaboradores:

> ⚠️ não achei a coluna de **"Modalidade Auxílio Alimentação"** (…)

E concluiu, com razão, que a correção não havia funcionado — *"eu já havia falado
sobre a coluna BM, cujo header é Categoria Auxílio Alimentação"*.

**Mas aquele aviso era de 26/09 às 18:35**, escrito pelo código ANTIGO e **guardado
no banco**. O texto "Modalidade" não pode sair do código de hoje: a lista de nomes
começa por "Categoria", e a mensagem usa o primeiro nome da lista. O que aconteceu é
que **"Atualizar cadastro" não foi apertado desde a correção** — então a leitura
nunca foi refeita, e o aviso velho continuou lá parecendo estado de agora.

⚠️ **A lição, e ela vale para qualquer aviso guardado:** aviso sem a data da carga
que o escreveu **parece o presente**. Agora o aviso sai como *"Na carga de 26/09 às
18:35: …"*, com uma linha dizendo que não é o estado de agora e que basta atualizar.
Sem isso, qualquer correção futura no cadastro vai gerar a mesma conversa.

### 7.32.2 As colunas, de novo — e uma que eu havia encurtado

Ele repetiu as cinco, e uma estava errada no meu lado: a de observação chama-se
**"Observação Ajuda de Custo"**, não "Observação". A planilha tem mais de uma coluna
de observação; o nome curto poderia casar com a errada e a tela mostraria a
observação de outro assunto.

Continua sem nome confirmado: **"Paga por BeeVale"**. Ela não afeta o cálculo do
auxílio — decide por qual portal a pessoa recebe — mas segue avisando.

### 7.32.3 Os desligados continuavam aparecendo, e o motivo era comparação exata

*"Você continua exibindo Colaboradores Desligados na tela de cadastro. Esses devem
aparecer ocultos. Eu já havia dito isso."*

A regra estava escrita, e estava **comparando texto exato**:
`lower(fase) = 'colaboradores desligados'`. Qualquer variação na planilha —
"Desligados", "Colaborador Desligado", um espaço a mais — deixava de casar, e a
pessoa voltava para a lista sem nada avisando.

Agora casa por **pedaço** ("desligad", "afastad"), que cobre plural e singular de uma
vez. E — o detalhe que quase escapou — **os dois predicados moram num lugar só**
(`fase_diz_desligado` / `fase_diz_afastado`), porque o `WHERE` que ESCONDE e a regra
que CLASSIFICA a pessoa tinham de concordar sempre. Eu havia corrigido um e quase
deixado o outro; se divergissem, a tela mostraria como ativo quem ela esconde do
pagamento.

### 7.32.4 A tela de cadastro não tinha informação de trabalho

*"3531 pessoa(s) trazidas da planilha em 26/09 às 18:35. Isso vai aparecer sempre
assim? Não tem nada de KPI essa tela."*

Ele está certo: "quantas linhas vieram e quando" é **registro de carga**, não
informação de trabalho. Entraram seis indicadores, e os três últimos são os que
travam pagamento: quantos estão trabalhando, quantos têm cada auxílio, e quantos
estão **sem código de obra**, **sem ID Fortes** e **sem card no Pipefy**. Os três
ficam âmbar só quando há algum.

E entrou o filtro que ele já havia pedido: a **Fase Atual** (coluna AX), com a lista
vindo do banco e a contagem de cada fase — fase nova no Pipefy aparece sozinha. Mais
o recorte por **data de admissão**, que responde "quem entrou neste mês".

### 7.32.5 O ponto: a terceira vez que ele reclama da mesma coisa

*"Eu já disse e vou dizer novamente: clico em trazer o ponto, sistema diz que vai
trazer e NÃO TRAZ nada. Não sei se ele conseguiu conectar, se tá indo, se não tá,
ninguém sabe de nada."*

⚠️ **O registro da tentativa SEMPRE existiu** — na tabela `execucoes`, com `ok`, com
a mensagem e com o erro da API dentro dela. O que faltava era **a tela mostrar**.
Falha que só aparece no log do serviço é falha que ele não tem como ler, e aí o botão
vira caixa preta.

Agora a tela do Ponto abre com a última tentativa: quando foi, por quem, se deu certo
e **o que o sistema respondeu**. E traduz os três casos que importam: *401/403* é
credencial recusada (trocar no Render); *tempo/conexão* é a API que não respondeu
(tentar de novo resolve); *0 página(s)* é a API respondendo sem dado para o mês. E
quando a carga diz "deu certo" mas não há mês carregado, isso é dito em vez de a tela
ficar muda.

### 7.32.6 A importação olhava o totalizador, não as pessoas

*"Você tá muito preocupado com os totalizadores do arquivo de importação, quando a
preocupação deve ser linha a linha de cada colaborador."*

A lista de folhas importadas mostrava "fecha / não fecha". Agora a coluna que vem
antes é **"Precisam de olho"**: quantas pessoas estão sem cadastro, já saíram ou
estão saindo, com link para a folha pessoa por pessoa. Esse número já era calculado e
só aparecia abrindo a folha. "Não fecha" continua dito — é problema de verdade — mas
não é mais a única coisa que acende.

E o aviso falso `linha que não reconheci: Empregado(s))` sumiu: é rótulo do rodapé do
Fortes. ⚠️ **O lugar dessa verificação importou:** na primeira tentativa eu a pus no
alto do laço e ela **engoliu o "Total: Geral (406 Empregado(s))"** — justamente o
número que a conferência usa. Dois testes pegaram; ela foi para depois dos totais.

### 7.32.7 Padrão visual, campos esticados, e o que eu coloquei sem ele pedir

- *"A tela de Folha de PGT não seguiu o mesmo padrão de Solicitações. Siga o mesmo
  padrão de cabeçalho, de cor da tabela."* Os KPIs da folha usavam `<span>`/`<b>` e
  não tinham a classe `estatico` — então **subiam no hover e mostravam cursor de
  mão**, como se fossem clicáveis. Agora usam o mesmo molde das Solicitações
  (`.kpi.estatico` com `kpi-rotulo` / `kpi-valor` / `kpi-sub`).
- *"Os campos em várias telas estão esticados demais, ocupa de ponta a ponta a tela.
  Fica horrível numa tela grande."* `input` e `select` dentro de `.cartao` não tinham
  largura declarada. Agora há teto **por tipo de campo** (420 px para texto, 170 para
  data, 150 para os curtos), e a caixa de soltar arquivo para em 760 px. Quem precisa
  da linha inteira pede (`.campo-largo`).
- E **cinco barras** ainda usavam `.barra-acoes`, que é a **grade de três colunas** das
  Solicitações: era ela que esticava campo de mês até meia tela e jogava botão e
  recado para pontas opostas. Todas viraram `.barra-mes`.
- *"Campo procurar de férias, bota limpar."* Feito — e o "Limpar" aparece **sempre**:
  botão que só existe depois de filtrar obriga a pessoa a descobrir que ele existe.
- *"Eu não pedi pra colocar Pagamentos e Cadastro e base do cálculo. Era apenas pra
  reorganizar."* Os rótulos saíram. Os grupos continuam definindo a ordem, com um
  risco fino entre eles.

## 8. Segurança — SEIS coisas que já são risco hoje (atualizado 27/09/2026)

Os três primeiros já estavam aqui. Os três últimos apareceram na leitura dos
scripts da planilha de Diaristas/Extras/GM, em 27/09/2026.

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
4. **O `BeeVale.gs` tem uma função que grava as credenciais do Dropbox escritas
   no próprio código** — chave do app, segredo do app e *refresh token*. O
   refresh token é o pior dos três: ele não expira e troca-se por um acesso novo
   quando se quiser. Quem lê o script tem acesso à pasta do Dropbox da BWS.
   A função existe para "rodar uma vez", mas o valor ficou lá. **Tem de ser
   invalidado no painel de apps do Dropbox e gerado de novo** — não basta apagar
   do script, porque quem já viu continua com ele. Existe no mesmo arquivo uma
   função `setDropboxCredentials()` que pede os valores numa caixinha e salva nas
   Script Properties: é esse o caminho certo, e é o que deve ficar.
5. **O token do Z-API também está escrito no código**, junto com o identificador
   da instância, na própria URL. Com esses dois, qualquer pessoa manda WhatsApp
   pela conta da BWS. Vale trocar na origem igualmente.
6. **O script FORÇA o link do Dropbox a ser público.** Ele não só cria o link
   como público (`requested_visibility: public`, `audience: public`): se já
   existir um link restrito à equipe, ele **revoga o restrito e cria um
   público**. Depois converte para link de download direto (`dl=1`) e cola na
   descrição do card e na mensagem de WhatsApp. Resultado: o arquivo com nome,
   CPF e valor de todo mundo baixa **sem login**, para quem tiver a URL — e a URL
   circula por WhatsApp, que é histórico que ninguém apaga. É o mesmo risco do
   item 3, mas agora está confirmado no código, e é deliberado, não acidente de
   configuração.

**O que o sistema novo resolve — e a correção de uma frase minha.** Eu havia
escrito que os itens 3 e 6 "morrem por construção" quando o arquivo passa a sair
de dentro do sistema. **Não é verdade automaticamente:** conferindo em
27/09/2026, descobri que `drive.subir_arquivo` — o caminho que a geração do
BeeVale deste módulo já usa em produção — **libera todo arquivo por link
público** (`{"role": "reader", "type": "anyone"}`), e o link vai para o card do
Pipefy. É o mesmo risco, dentro do nosso código, com arquivos que têm nome, CPF
e valor.

Então os itens 3 e 6 só morrem se **a subida deixar de ser pública**.

✅ **DECIDIDO PELO DONO em 27/09/2026: continua público, como está.** O motivo
dele: *"é um simples arquivo de folha de pagamento, isso vai transitar só dentro
da empresa (…) o que é que alguém vai fazer com isso? Pagar o funcionário?"*

Então os itens **3 e 6 ficam como estão**, por decisão de negócio, e não por
esquecimento. O que fica registrado, uma vez e sem reabrir: o link público **não
depende de ter acesso à pasta** — quem recebe a URL baixa sem conta nenhuma — e o
risco é de exposição de dado pessoal em volume, não de pagamento indevido. Os
itens **1, 4 e 5 (credenciais que já circularam) continuam valendo** e não foram
afetados por esta decisão. Os itens 1, 4 e 5 são credenciais que **já circularam** e
precisam ser trocadas na origem, independentemente do sistema novo.

⚠️ **Estes valores não devem ser colados aqui nem no chat.** O caminho é:
abrir o editor de script, copiar direto de lá para o Render (ou para o painel do
fornecedor, no caso da troca), e trocar na origem em seguida.

---

## 9. O que este documento NÃO cobre

- **As fórmulas das planilhas, célula por célula.** Ver §3.
- **A aba `DC`** (Despesa com Colaboradores) e as outras oito folhas do Make, além
  do que a §4 registra.
- **Qualquer teste em produção.** Nada foi executado; nada foi escrito no Pipefy,
  no Drive, no Dropbox ou no OMIE.
