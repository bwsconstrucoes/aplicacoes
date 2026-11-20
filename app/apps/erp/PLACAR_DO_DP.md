# PLACAR DO DP — o que o Pipefy faz, e onde isso vive no ERP hoje

> **Para que este arquivo existe**, nas palavras dele, em 17/11/2026:
>
> *"Eu estou vendo que eu estou tendo que corrigir minúcia a minúcia, porque
> nada está ficando 100%. E está muito desgastante, cansativo e demorado. Eu
> estou fazendo errado do jeito que a gente está fazendo aqui. Tem um método
> melhor?"*
>
> **Tem, e é este arquivo.** Até agora o ÚNICO detector de vão era ele abrir
> uma tela e sentir falta. A especificação existia (`DE_PARA_PIPEFY_DP.md`, a
> exportação dos dois pipes pelo GraphQL em 04/11/2026), mas **nada comparava
> a especificação com o sistema**. Então cada vão era descoberto um por um, por
> ele, na pior hora possível: operando.
>
> Aqui cada coisa que o Pipefy faz tem UMA linha e UM veredito. Com isso:
>
> · ele dá **uma passada** nesta lista em vez de cinquenta correções soltas;
> · uma sessão nova sabe o que falta **sem perguntar**;
> · `tests/test_placar_do_dp.py` **recusa** item do Pipefy sem linha aqui, e
>   linha sem veredito — então o placar não apodrece em silêncio.

## Como ler o veredito

| Marca | Quer dizer |
|---|---|
| ✅ | **Está no ERP**, no lugar equivalente. Nome do tipo/etapa entre parênteses |
| 🔀 | **Existe, em OUTRO lugar** do ERP — e isso é de propósito, porque o ERP não é um espelho do Pipefy. Diz onde |
| ❌ | **Não existe ainda.** É trabalho a fazer |
| ⚰️ | **Morre com o Pipefy / com a planilha**, de propósito. Não vira nada |
| ❓ | **PRECISA DE VOCÊ.** Eu não sei responder sem saber como é usado na prática |

⚠️ **A regra que vale aqui**, e ela é a razão de o placar não ser só uma
lista de pendências: *"está no Pipefy" NUNCA é resposta para onde um dado deve
morar.* O ERP vai SUBSTITUIR o Pipefy, então 🔀 e ⚰️ são respostas tão boas
quanto ✅ — o que não pode existir é vão que ninguém mapeou.

---

## 1. Os pedidos que a obra faz ao DP

| O que o Pipefy tem | Onde está no ERP |
|---|---|
| Admissão de Prestador de Serviço | ✅ `ADMISSAO_PRESTADOR` |
| Cadastro de Colaborador | ✅ `CADASTRO` — no ERP chama-se **Admissão de Colaborador** desde 19/11/2026 |
| Admissão de Colaborador | ✅ `CADASTRO` — o nome que ele pediu em 19/11/2026: cobre quem nunca trabalhou conosco e quem volta (readmissão) |
| Readmissão de Colaborador | ✅ `READMISSAO` |
| Desligamento de Colaborador | ✅ `DESLIGAMENTO` |
| Desligamento de Prestador de Serviço | ✅ `DESLIGAMENTO_PRESTADOR` |
| Cancelar Solicitação de Desligamento | ✅ `CANCELAR_DESLIGAMENTO` |
| Solicitação de Alteração de Cargo | ✅ `ALTERACAO_CARGO` |
| Atualização de Dados Cadastrais | ✅ `ATUALIZACAO_DADOS` |
| Solicitação de Cadastro de Dependente | ✅ `DEPENDENTE` |
| Solicitação de Ajuda de Custo | ✅ `AJUDA_DE_CUSTO` |
| Solicitação de Vale Transporte | ✅ `VALE_TRANSPORTE` |
| Lançar Desconto em Folha | ✅ `DESCONTO_EM_FOLHA` |
| Solicitações Diversas | ✅ `DIVERSOS` |
| Ajuste de Ponto | 🔀 ponto › ficha da pessoa › **Ajustar o dia (incluir batida)** |
| Ajuste de Ponto e Faltas | 🔀 ponto › **Ajustar o dia** + **Pedir abono**. ⚠️ O campo *Motivo* do Pipe tem **20 opções** (falta não justificada manhã/tarde, doação de sangue, justiça eleitoral, banco de horas…). **Conferir se o ponto tem todas** — ver §4 |
| Ajuste de Pontos e Faltas | 🔀 o mesmo de cima — **o Pipe tem o pedido escrito de duas formas** (singular e plural), e no ERP é um caminho só |
| Encaminhamento de Atestado Médico | 🔀 ponto › ficha da pessoa › **Atestado ou licença** |
| Pedido de Afastamento ou Licença | 🔀 ponto › ficha da pessoa › **Lançar afastamento** |
| Documentação Admissão | 🔀 ERP › DP › Contabilidade, modelo **Assinatura de CTPS** (espera a ficha de registro) |
| Documentação Rescisão | 🔀 ERP › DP › Contabilidade, modelo **Desligamento** |
| Solicitação de Férias | ✅ `FERIAS` — desde 20/11/2026 a obra pede pelo portal; o DP manda à contabilidade (modelo **Férias**) e o pedido fecha quando ela devolve. As férias ficam na tela **Férias** (aguardando → confirmada) |
| Alteração de Férias | ✅ `ALTERACAO_FERIAS` — novo (20/11/2026; o Pipe não tinha). A data só muda com o retorno da contabilidade |
| Cancelamento de Férias | ✅ `CANCELAMENTO_FERIAS` — novo (20/11/2026; o Pipe não tinha). As férias saem da programação com o retorno da contabilidade |
| Relatório de DCs | 🔀 ERP › **DC** (tela própria de despesas em lote) |
| Atualizar Chave Pix | ❓ O Pix é campo do cadastro, e `ATUALIZACAO_DADOS` o alcança. Quer um **tipo próprio** (como no Pipe) ou basta o de dados cadastrais? |
| Atualização de Telefone | ❓ Mesma pergunta do Pix |
| Pré-Cadastro de Colaborador | ❓ O portal da obra tem **Cadastro de Colaborador** (completo). O "pré-cadastro" era um passo antes — ainda faz sentido, ou o cadastro único substituiu? |
| Cartão Somapay 2ª Via | ❌ não existe |
| Solicitação de Cartão Somapay 2ª Via | ❌ não existe (é o mesmo pedido com dois nomes no Pipe) |
| Cartão Vale Transporte 2ª Via | ❌ não existe — ⚠️ `VALE_TRANSPORTE` pede o BENEFÍCIO, não a 2ª via do cartão |
| Solicitação de Cartão Vale Transporte 2ª Via | ❌ não existe (o mesmo, com dois nomes no Pipe) |
| Gerar Crachá (DP) | ❌ não existe |
| Solicitação de Crachá | ❌ não existe (o mesmo, com dois nomes no Pipe) |
| Foto Relógio de Ponto | ❓ O ponto tem **Fotos de cadastro** e a foto de cada batida. Este pedido do Pipe era para quê — mandar a foto do relógio físico? |
| Contrato, Etiqueta, Termos (DP) | ❓ Parece tarefa interna do DP (gerar contrato, etiqueta e termos). Isso é **documento a gerar** ou pedido que alguém abre? |
| Planilhas de Cadastro (DP) | ⚰️ era manter a planilha em dia. Morre com a planilha |

## 2. As fases do cadastro (pipe CADASTRO COLABORADORES)

| Fase no Pipefy | Onde está no ERP |
|---|---|
| Cadastro Inicial (44 campos) | ✅ etapa **Inicial** (`CADASTRO_INICIAL`) |
| Sob Análise | ✅ fundida na **Inicial** em 17/11/2026, por decisão dele |
| Aguardando Documentação | ✅ etapa **Solicitações** + o pedido à contabilidade esperando o retorno |
| Solicitações DP (19 campos) | ✅ etapa **Solicitações** (código `NA_CONTABILIDADE`, rótulo "Solicitações") |
| Cadastro Concluído (18 campos) | ✅ etapa **Concluído** — e agora com **trava**: carteira só conclui com o registro formal feito |
| Cadastro Cancelado | ✅ etapa **Cancelado** |
| Colaboradores Afastados | ✅ etapa **Afastado** |
| Colaboradores Desligados | ✅ etapa **Desligado** |
| Cadastros Adicionais (8 campos) | ❓ Para que servia esta fase? Pelos 8 campos parece complemento de cadastro já concluído |
| Atestado Médico | 🔀 ponto › **Atestado ou licença** (e atestado > 15 dias abre afastamento sozinho) |
| BH/Compensação/Folga | 🔀 ponto › **Compensação**, **Compensação de horas** e **Permuta** |
| MobPonto | ⚰️ era o aplicativo de ponto antigo. O ponto do ERP substituiu |

## 3. As fases do pipe de solicitações

| Fase no Pipefy | Onde está no ERP |
|---|---|
| Caixa de entrada | ✅ situação **Aberta** do pedido |
| Sob Análise | ✅ situação **Em análise** |
| Fazendo | ✅ situação **Em análise** — no Pipe eram duas fases para o mesmo estado |
| Concluído | ✅ situação **Atendida** |
| Cancelar | ✅ situação **Cancelada** (e **Recusada**, que o Pipe não tinha) |

## 4. O que o Pipefy faz e o placar ainda não sabe medir

Estas são as partes da configuração do Pipe que **não entraram na exportação
de 04/11** (ela trouxe campos e fases, não automações), e por isso ninguém
conferiu:

1. ❓ **Os disparos de e-mail automáticos.** Quais fases mandavam e-mail, para
   quem, e com que texto? O ERP tem 7 modelos de pedido à contabilidade — e eu
   não sei se são os mesmos 7 nem se faltam outros.
2. ❓ **As condicionais de campo** (campo que aparece conforme a resposta de
   outro). O ERP tem isso para os tipos que ele mandou, um por um. **Não há
   conferência** contra o que o Pipe fazia.
3. ❓ **As 20 opções de "Motivo" do ajuste de ponto** — o ponto tem quantas
   dessas?
4. ❓ **As transições automáticas** entre fases (o que movia um card sozinho).
5. ❓ **Quem recebia o quê**: no Pipe havia responsáveis por fase. No ERP isso
   é perfil de acesso — e a correspondência nunca foi mapeada.

⚠️ **O jeito de fechar o item 1, 2 e 4 sem depender da memória dele:**
exportar as **automações** do Pipefy (o GraphQL do Pipe entrega), como foi
feito com os campos. É uma coisa que ele pede ao Pipefy uma vez, e cala
dezenas de perguntas.

---

## 5. AS CONDICIONAIS — medidas e decididas por ele em 18/11/2026

O Pipefy **não exporta condicionais** (ele avisou: *"o pipe não exporta
condicionais, por isso minha pergunta sobre mais ajuda na construção e na
ideia"*). Então elas foram **medidas nos 660 cadastros** e ele revisou a lista
inteira. Isto fecha boa parte do item 2 da §4.

### Adotadas

| Regra | Medida em |
|---|---|
| Cargo, Data de início e Empresa de registro obrigatórios **para todos** | quase 100% de todos os cadastros, com vínculo ou sem |
| CTPS: Série da CTPS obrigatória | 408 de 413 (98,8%) |
| CTPS: Contrato de experiência obrigatório | 406 de 413 (98,3%) |
| CTPS: Convenção obrigatória | 403 de 413 (97,6%) |
| CTPS: Data de admissão obrigatória | 413 de 413 |
| Prestador: Convenção obrigatória | 207 de 207 autônomos RPA |
| **Salário passou a ser só de quem tem CTPS** | vazio em 97,3% dos 222 prestadores — prestador recebe por diária ou por mês |
| Prestador: Contrato de experiência não se aplica | vazio em 97,7% |
| **Razão social (PJ) e CNPJ (PJ) só aparecem no contrato PJ** | ⚠️ **decisão DELE** — a medição viu só 10 PJ em 660, abaixo do mínimo, e ficou calada |

### Recusadas por ele, com o motivo — e o motivo vale mais que o "não"

| Proposta recusada | Por quê |
|---|---|
| Não exibir "Valor do mês" para CTPS e para CLT determinado | **mensalista de carteira existe** (4 em 413) |
| Não exibir "Ajuda/Valor da alimentação" para prestador e para RPA | **prestador com ajuda de alimentação existe** (1 a 3 casos) |

⚠️ **A lição, e ela muda a medição de agora em diante: "quase nunca" não é
"nunca".** O corte de 97% acha o campo que não se aplica, mas não o distingue
do campo que se aplica POUCO. Esconder o segundo tiraria da tela o único jeito
de registrar esses casos, e eles voltariam a ser resolvidos por fora do
sistema. Só quem conhece o negócio distingue — foi o que ele fez.

As seis recusas estão em `RECUSADAS`, em `core/cadastros/regras_pelos_dados.py`,
e a tela **não volta a oferecê-las**: elas aparecem numa lista própria, "Já
decididas por você".

### ⚠️ O que a medição mostrou e não é regra: cadastros incompletos

Os discordantes se repetem. **Cinco pessoas aparecem em quatro listas
diferentes** (sem série da CTPS, sem contrato de experiência, uma sem empresa de
registro), e **oito a dez pessoas estão sem convenção**. Não são exceções à
regra: são cadastros pela metade, hoje, em produção — e convenção faltando é o
que a conferência da folha usa.

⚠️ Os nomes e CPFs **não entram neste arquivo**: é dado pessoal, e o
repositório é lido por quem não precisa dele. Estão na tela
(`ERP › Pessoal › Regras do cadastro`), com link para cada ficha.

---

## Como este arquivo se mantém vivo

- **A fonte da verdade do Pipefy** é `DE_PARA_PIPEFY_DP.md` (exportação).
- **O placar é este arquivo**, e `tests/test_placar_do_dp.py` recusa item do
  de-para sem linha aqui, e linha sem veredito.
- **Quando algo sai do ❌ ou do ❓**, a linha muda aqui NA MESMA entrega, e o
  `HISTORICO.md` registra a decisão. Placar que atrasa vira enfeite.
