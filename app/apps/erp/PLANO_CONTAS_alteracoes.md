# Plano de contas — alterações consolidadas

> **Guardado no repositório em 10/09/2026, depois de aplicado.** Este é o
> documento que o dono mandou; ele é a ORIGEM das oito alterações, e fica aqui
> para a próxima sessão saber por que o plano é como é sem depender do chat.
> O que foi feito de cada item, e as decisões que sobraram, está em
> `HISTORICO.md` › "O plano de contas depois das oito alterações".


> Arquivo alvo: `app/apps/erp/core/cadastros/plano_padrao.py`
>
> **Antes de mexer:** verifique quais contas já têm lançamento e avise se
> alguma alteração exigir migração de dados. Contas com movimento não podem
> simplesmente sumir — precisam ser migradas ou aposentadas com o histórico
> preservado.

---

## 1. Devoluções e estornos saem do grupo Receitas

Hoje estão em 1.2 como "outras receitas operacionais". Não são receita: são
**redução de custo**. Do jeito atual a obra aparece com receita a mais e custo
inalterado, e a margem sai errada dos dois lados.

Mover para o grupo 3, como **contas redutoras de custo** (sinal negativo no
relatório):

| Conta atual | Vira |
|---|---|
| 1.2.01 Devolução de material a fornecedor | redutora de custo de obra |
| 1.2.02 Estorno de despesas | redutora de custo |
| 1.2.03 Reembolso de custas e despesas processuais | redutora de custo |

**Permanece como receita:** 1.2.04 (multas e indenizações recebidas) — nesse
caso não houve gasto nosso; é dinheiro que entra por descumprimento de
terceiro.

**Comportamento esperado:** a despesa original continua registrada. Se houve
R$ 10.000 de compra e R$ 500 de devolução, o relatório mostra R$ 9.500 de custo,
mas as duas linhas continuam visíveis no histórico. Não estornar o lançamento
original.

---

## 2. Separar a retenção conjunta (CSRF/PCC)

A conta 2.1.06 junta PIS, COFINS e CSLL num só lugar. Separar em **três contas
de retenção distintas**.

Razões: as alíquotas são distintas; PIS e COFINS incidem sobre faturamento e a
CSLL sobre o lucro; e a retenção **nem sempre acontece** — depende da obra e do
tomador. Juntas, não há como saber o custo real de cada uma.

A guia é uma só (DARF 5952), então o lançamento precisa permitir **ratear o
valor da guia entre as três contas**.

---

## 3. Um tributo, uma conta — aplicar de verdade

A CSLL aparece hoje em dois lugares: retida (grupo 2.1) e sobre o lucro (2.2).
É o **mesmo imposto em momentos diferentes** — a retida é antecipação da devida.

Unificar numa conta só, com a forma (retida na nota ou apurada) como informação
do lançamento, não como conta separada. Mesma coisa para IRRF/IRPJ se estiver
duplicado.

Motivo prático: hoje não se consegue ver o custo total de CSLL de uma obra sem
somar duas linhas de grupos diferentes.

---

## 4. Eliminar o parcelamento tributário como fluxo

**Remover a conta 9.4.03** (pagamento de principal de parcelamento tributário).
Gera confusão sem necessidade.

Regra que fica valendo: o **principal** vai sempre para a conta do tributo
correspondente, no grupo 2. Os **juros** vão para 2.3.02.

---

## 5. Grupo 8 (investimentos): de FLUXO para RESULTADO

Todas as contas do grupo 8 passam a ser de resultado.

**Razão operacional, que prevalece aqui:** uma betoneira de R$ 4.000 comprada
para uma obra em parceria precisa aparecer no custo daquela obra — senão não há
como cobrar a parte do parceiro. Como o rateio é obrigatório em todo título, a
distinção se resolve sozinha: bem de alto valor comprado para a empresa fica na
matriz; bem que serve a obra vai rateado para a obra.

A depreciação fica a cargo da contabilidade externa, no balanço.

> Com esta mudança, **não é mais necessário** o relatório de desembolso por obra
> que havia sido sugerido antes.

---

## 6. Nomenclatura e descrições

| Conta | Ajuste |
|---|---|
| 5.1.03 | "software" → "sistemas" |
| 5.1.05 | tirar "copa": fica "Limpeza da sede" |
| 5.1.01 | descrição deve dizer que casa de apoio à obra vai em **3.3.04**, não aqui |
| 5.2.04 e 3.4.06 | deixar explícito: **anuidade do CREA** é administrativa; **ART/RRT** é da obra |

---

## 7. Descrições a acrescentar (evitar dúvida no lançamento)

Muitas contas são óbvias pelo nome. Estas não são, e a dúvida no momento do
lançamento é o que produz plano de contas sujo. Acrescentar descrição curta
explicando **quando usar** e, quando fizer diferença, **com o que não
confundir**:

### Onde há risco de confundir duas contas parecidas

- **3.1.19 Ferramentas** × **8.1.04 Ferramentas e equipamentos duráveis** —
  ferramenta de consumo, que se gasta ou se perde, contra equipamento que dura
  anos. Definir um **critério de valor** e escrever nas duas descrições, senão a
  escolha vira loteria e o relatório fica torto.
- **2.1.02 INSS sobre serviços e obra** × **4.2.01 INSS patronal sobre folha** —
  um é da obra/serviço (CEI/CNO), o outro é encargo da folha da empresa.
- **2.1.03 IRRF sobre serviços prestados** × **4.2.03 IRRF sobre a folha** — um
  é retido do que nos pagam, o outro é retido do empregado.
- **5.1.04 Conservação e manutenção predial** × **8.2.02 Benfeitorias** — reparo
  que mantém o bem contra reforma que valoriza e aumenta a vida útil.
- **5.1.03 Internet, telefonia e sistemas** × **8.3.01 Software e licenças** —
  assinatura mensal contra aquisição de licença perpétua ou desenvolvimento.
- **3.2.01 Empreiteiros com medição** × **3.2.02 Serviços técnicos PJ** —
  execução com contrato e medição contra projeto, laudo, sondagem, topografia.
- **3.2.03 Serviços de pessoa física (RPA)** — explicar que gera INSS (2.1.02) e
  IRRF (2.1.03) e que exige RPA.
- **3.3.01 Locação de máquinas, veículos e equipamentos** × as demais 3.3 —
  quando usar a genérica e quando usar a específica.
- **6.1.01 Juros sobre empréstimos** — só os juros; o principal é 9.4.02.
- **9.1.02 Valores de terceiros** × **9.1.03 Pagamento por conta errada** — já
  têm boa descrição; conferir se está claro que **a mesma conta registra as duas
  pontas** e que saldo diferente de zero significa pendência.

### Contas sem descrição que merecem uma linha

Varrer o plano e acrescentar descrição onde não houver, priorizando:

- todo o grupo **3.1** (materiais) — em especial as que se sobrepõem:
  3.1.18 (parafusos e ferragens) × 3.1.26 (serralheria), 3.1.04 (vedação) ×
  3.1.05 (pré-moldados), 3.1.21 (argamassas) × 3.1.17 (aditivos e colas);
- **3.1.99 Outros materiais** e **5.3.99 Outras despesas** — deixar explícito
  que são último recurso e devem ser revisadas periodicamente, senão viram
  depósito de tudo;
- grupo **9.3 (aportes)** — a direção (recebido × concedido) é o que confunde;
- **1.4 (venda de ativos)** — o que entra ali é o resultado da venda, e a
  entrada do dinheiro aparece no fluxo.

### Regra geral para as descrições

Frase curta, em português direto, dizendo **quando usar** e, se couber, **com o
que não confundir**. Elas aparecem na hora do lançamento e são o que evita o
plano de contas apodrecer com o tempo.

---

## 8. Depois de alterar

- Rodar a suíte.
- Verificar se o de-para do Omie (`core/cadastros/depara.py`) continua
  consistente com as contas que mudaram de grupo ou foram eliminadas.
- Verificar se as regras de tipo de título por conta (matriz de documento hábil)
  continuam válidas para as contas alteradas.
- Se houver lançamento nas contas movidas ou eliminadas, propor a migração
  antes de aplicar.
