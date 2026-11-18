# Desligar a planilha e fazer o Análise de SPs ler o ERP

> *"Precisamos fazer de uma forma infalível essa desativação da planilha e a
> leitura do Análise de SPs pela base do ERP. **A gravidade disso é pagar
> funcionários de forma equivocada.**"* — ele, 17/11/2026

Este arquivo é o roteiro. Ele existe porque "infalível" não se consegue
raciocinando sobre o código: consegue-se **calculando as duas coisas e
comparando**, e só virando a chave quando a diferença for zero.

---

## A ideia, em uma frase

Hoje a folha lê a planilha. Amanhã lê o ERP. **Antes de virar, as duas têm de
pagar a mesma coisa para todo mundo** — e isso é verificável numa tela, não
numa conversa.

```
HOJE:     Pipefy → planilha "Registro de Colaboradores" → Análise de SPs → folha
AMANHÃ:                                            ERP → Análise de SPs → folha
```

---

## A ferramenta

**ERP › Importar › "Conferência: o ERP paga igual à planilha?"**

Ela compara, pessoa por pessoa, o que a folha decidiria lendo cada base. Lê os
dois lados de verdade e **não altera nada**.

O que ela responde, do mais grave para o menos:

| # | O que compara | Por que é grave |
|---|---|---|
| 1 | **"Esta pessoa pode receber?"** | É a pergunta de dinheiro. Respondida pela **mesma função** do Análise de SPs (`situacao_no_pagamento`), alimentada com as duas fichas. Divergência = alguém recebe que não devia, ou deixa de receber |
| 2 | **Na planilha e NÃO no ERP** | Desligar com gente aqui faz essas pessoas **desaparecerem da folha** |
| 3 | **Modalidade do auxílio** | Modalidade que a folha não entende faz a pessoa sair com **zero** |
| 4 | **Os valores** | alimentação, transporte, gratificação, diária |
| 5 | **O que decide diarista × CTPS** | tipo, tipo de contrato e as duas datas — erra a forma de pagamento do dia |
| 6 | O resto do cadastro | cargo, obra, convenção, matrícula, telefone |

No alto dela há **uma linha de veredicto**: "dá para desligar" ou "ainda não".
Ela só fica verde quando **não há** divergência de pagamento, **não há** gente
só na planilha e **não há** divergência que mexa em dinheiro.

---

## O roteiro, na ordem

**1. Importe tudo.** ERP › Importar › "Cadastro das pessoas (planilha)", com o
campo de CPF vazio e **"cadastrar quem falta"** marcado.

**2. Zere as pendências da importação.** No relatório, abra
*"Colunas da planilha que eu não leio"* e *"Pendências"*. Cada linha ali é um
campo que não entrou. Me mande a lista; não tente adivinhar.

**3. Rode a conferência.** Preencha "pagando até" com o último dia do período
que você pagaria, e compare.

**4. Resolva na ordem que a tela mostra.** Primeiro os de pagamento, depois os
de dinheiro, por último o cadastro. A tela agrupa "onde a diferença se
concentra" — um campo com muita gente é quase sempre **uma causa só**.

**5. Repita 1 → 3 até o veredicto ficar verde.**

**6. Só então** o Análise de SPs passa a ler o ERP, e a atualização da planilha
pode ser desligada.

---

## ⚠️ As três coisas que podem dar errado, e o que fazer

**1. A planilha pode ter andado depois da importação.** A conferência mostra
*"planilha atualizada em …"*. Se a hora for posterior à sua importação, importe
de novo antes de confiar no verde.

**2. Verde não quer dizer "testado em dinheiro".** A conferência prova que as
duas bases **dizem o mesmo**. Ela não gera folha. O passo que fecha isso é
**gerar a mesma folha duas vezes** no Análise de SPs — uma lendo a planilha,
outra lendo o ERP — e comparar os totais, antes de pagar.

**3. O ponto ainda lê a planilha.** É outro desligamento, e o dono já pediu
separado (ver `app/apps/ponto/HISTORICO.md`). Um não depende do outro, mas **os
dois leem a mesma planilha** — desligar a atualização dela afeta os dois.

---

## O que FICA pendente de decisão dele (17/11/2026)

**1. Quatro categorias de transporte que a folha não entende, nem hoje:**
`Segunda e Sexta` (9 pessoas), `Motocicleta` (9), `Veículo`, `Bicicleta`.

Não é defeito do ERP — a planilha escreve assim e a folha também não as
reconhece; **essas pessoas já ficam sem cálculo de transporte hoje.** A folha
só sabe: `Mês`, `Mensal`, `Segunda à Sexta`, `Segunda à Quinta`, `Diário`,
`Vale Transporte`, `Diário e Vale Transporte` e `Cartão`.

⚠️ **"Segunda e Sexta" não é "Segunda à Sexta".** A primeira é só a ida e a
volta da semana, de quem fica alojado na obra; a segunda é os cinco dias. A
folha conhece a segunda.

Ou a folha aprende as quatro, ou essas pessoas têm outra categoria.

**2. `Paga por BeeVale`** — existe na planilha, o Análise de SPs importa e
guarda, **nenhuma conta da folha usa**, e o ERP não tem o campo. Se vai servir
para algo, o ERP precisa dele; se não, morre com a planilha.

---

---

# As DUAS desativações — e elas são independentes

> Ele, em 17/11/2026: *"Como é que eu desligo atualização via diários ou via
> planilha, para a gente desconectar essa atualização lá e passar a trabalhar
> só por aqui? E do jeito que tá o ponto, tá pegando os dados de onde? Já é
> essa base? O que preciso fazer para ele ficar aqui?"*

São **duas chaves diferentes**, em dois lugares diferentes. Uma não mexe na
outra, e nenhuma das duas exige programador.

## 1 · O PONTO — hoje ele NÃO lê o ERP

**De onde o ponto está lendo agora:** da **cópia da planilha** que o Análise de
SPs guarda (a tabela `analisesps.colaborador`, alimentada pelo botão
"Atualizar cadastro" de lá). É o padrão do sistema, e foi pedido seu na época.

Na prática, hoje, o ponto pega destas duas fontes ao mesmo tempo:

| O quê | De onde |
|---|---|
| nome, telefone, cargo, obra, situação, datas, quem não bate ponto | **da planilha** (via Análise de SPs) |
| jornada, raio da obra, foto, banco de horas, exceções do DP | do próprio ponto |

Quando a planilha não tem a pessoa, ele cai no cadastro do ERP.

### Como virar a chave

**ERP › Ponto › Gestão › bloco "Base de pessoas"** → troque de
**"Registro de Colaboradores"** para **"ERP"**.

A tela pergunta antes de confirmar e mostra um retrato da base — inclusive
**quantas pessoas estão na planilha e faltam no ERP**, com exemplos. Esse
número tem de estar **zero** antes de virar: quem falta no ERP simplesmente
para de bater ponto.

### O que muda no mesmo instante

- **PJ e pró-labore param de bater ponto.** Era regra que só existia no caminho
  da planilha; agora ela existe nos dois (decisão sua de 16/11). Quem bate:
  CLT (os dois), temporário, autônomo RPA, autônomo mensalista, jovem aprendiz
  e estágio.
- **A obra ESCRUSSAS continua fora**, como já era.
- **A exceção marcada pelo DP continua valendo por cima** — quem for marcado
  bate, mesmo sendo PJ.
- **O início do contrato continua sendo a MENOR** entre "data de início" e
  "data de admissão", como na planilha. O diarista começa antes da carteira, e
  mudar a data do papel não pode apagar o direito de registrar o dia.

### O que conferir antes

1. **"Faltam no ERP" tem de ser zero.** A própria tela tem um botão para
   cadastrar os que faltam.
2. **Obra preenchida em todo mundo que bate ponto.** Sem obra, não há cerca
   nem conferência de local.
3. **Tipo de contrato preenchido**, porque agora é ele que decide quem bate.

**Dá para voltar atrás:** a mesma chave volta para "Registro de Colaboradores"
a qualquer momento.

## 2 · O ANÁLISE DE SPs — a folha

Essa é a do roteiro acima: ela só se desliga quando a **conferência** ficar
verde e a folha for gerada duas vezes com o mesmo resultado.

**A atualização da planilha em si** (o robô do Make que copia o Pipefy para
ela) é desligada **por você, no Make** — o ERP não a controla. Por isso a ordem
importa: primeiro o ERP vira a fonte das duas pontas (ponto e folha), e **só
então** o robô é desligado. Ao contrário, as duas pontas ficam sem base.

## A ordem que eu faria

1. **Limpar e importar** o cadastro no ERP (com a sua lista).
2. **Zerar "faltam no ERP"** na tela do ponto.
3. **Virar a chave do PONTO** para ERP. Acompanhar um dia de batidas.
4. **Rodar a conferência da folha** até ficar verde.
5. **Gerar a folha duas vezes** no Análise de SPs e comparar os totais.
6. **Só então desligar o robô do Make.**

Os passos 3 e 4 são independentes: dá para virar o ponto sem mexer na folha.

---

## Onde isto mora no código

| O quê | Onde |
|---|---|
| A conferência | `app/apps/erp/core/conferencia_folha.py` |
| A tela | `app/apps/erp/templates/erp_importar.html` (primeiro cartão) |
| O que o ERP entrega à folha | `pessoas.para_folha()` |
| A palavra que a folha lê (≠ rótulo da tela) | `pessoas.CATEGORIA_*_PARA_FOLHA` |
| O teste que cobra o espelho | `tests/test_erp_espelha_a_folha.py` |
| O teste da conferência | `tests/test_conferencia_folha.py` |
| A chave do ponto | `app/apps/ponto/core/registro.py` (`pessoas.fonte`) |
| O agente de DP, explicado | `app/apps/erp/COMO_O_AGENTE_TRABALHA.md` |

**Os dois testes leem os DOIS lados de verdade** — o `folha_auxilio` do Análise
de SPs e o cadastro do ERP. Não há valor copiado dentro deles: quem mudar um
dos lados cai neles. É o que impede o espelho de rachar em silêncio depois de
alguém mexer num rótulo.
