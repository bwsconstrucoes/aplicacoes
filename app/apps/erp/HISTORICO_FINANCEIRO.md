# HISTÓRICO — ERP · Financeiro

Memória **desta área**. O chat é descartável; isto não.

⚑ **ESTE ARQUIVO NASCEU EM 18/11/2026**, quando o dono dividiu o chat do ERP em
quatro. O pedido dele, com as palavras dele:

> *"Talvez assim separar mais: a parte de pessoal eu trato no chat, a parte de
> suprimento eu trato no outro chat, porque assim uma coisa que eu vejo numa
> área eu jogo lá, e a outra coisa que eu vejo na outra área eu jogo no outro
> chat. Eu acho que tem como evoluir mais rápido dessa forma."*

⚠️ **O PASSADO DESTA ÁREA NÃO ESTÁ AQUI — está em `HISTORICO.md`**, que até
18/11/2026 guardava o ERP inteiro (12 mil linhas). Ele **não foi quebrado**, de
propósito: repartir doze mil linhas de memória por adivinhação perderia
contexto, e perder memória é o pior estrago possível neste repositório. Então:

- **procurar o passado** → `HISTORICO.md`, buscando por "título", "aval", "aprovação", "conciliação", "pagamento", "empreita", "locação", "OMIE"
- **escrever o novo** → aqui

**Leitura desta área, além do `HISTORICO.md`:** `MEDICOES_E_NOTAS.md`,
`NOTAS_FISCAIS.md`, `PLANO_CONTAS_alteracoes.md`,
`AUDITORIA_TRANSACIONAL.md`, `CONTEXTO.md` §2.1.

---

## Onde o trabalho está

Títulos, contas a pagar e a receber, o aval em duas pessoas, a
conciliação do extrato, pagamentos, empreitas, locações e o que ainda
conversa com o OMIE.

**É a parte mais antiga e mais usada do ERP** — contas a pagar está
completo e em operação. Mexer aqui mexe em dinheiro que sai.

---

## ⚑ PENDENTE AGORA — leia isto antes de qualquer coisa

### 27/11/2026 — O FINANCEIRO NO PORTAL E A REGRA DOS CAMPOS (no ramo, NÃO publicado)

Ramo `claude/beautiful-cannon-l0aujd`. **Não foi para a `main`** — espera o
"pode" dele. **Não tem migração.**

**O que espera dele:**
1. Revisar `PLEITOS_FINANCEIRO.md` (o mapa de tipos, subtipos, campos e
   condições). Ele pediu exatamente isso: *"faça um mapeamento dos tipos,
   subtipos e campos e condições para que eu possa revisar a lógica"*.
2. Responder as decisões marcadas **ESCOLHA MINHA** abaixo.
3. Testar no portal como administrador (só ele vê a área por enquanto).

---

## 27/11/2026 — O FINANCEIRO NO PORTAL, E OS CAMPOS COERENTES NAS DUAS PORTAS

### O pedido, com as palavras dele

> *"Pedir Pagamento, Fundo Fixo, Medição de Empreita e o Cadastro dela, Cancelar
> um Pedido e uma forma de listar e visualizar todos esses lançamentos, devem
> estar disponíveis no portal. O Administrativo de Obra fará esses lançamentos.
> É importante revisar toda a lógica de condicionais dos campos (…) Não tem
> sentido por exemplo exibir campo de boleto quando o tipo de pagamento for
> pix."*

E, no meio do trabalho, duas mensagens que mudaram o tamanho dele:
*"a mesma lógica deve ser aplicada ao ERP também nos casos em que se mantenha
lançamentos em ambos lugares"* e *"essa lógica e condições dos campos nunca foi
revisada no ERP"*. Na véspera (26/11): *"vamos deixar ele ativo somente para o
administrador."*

### O desenho: UM catálogo, três leitores

`core/titulos/pleitos.py` é a fonte única dos pleitos do financeiro: as formas
de pagamento (o que cada uma pede), os subtipos de pagamento (tipo interno,
formas que servem, nº e data do documento, campos próprios, documentos
exigidos), o fundo fixo, o contrato e a medição de empreita, e o cancelamento.
Quem lê:

- **o portal** (`templates/erp_portal_financeiro.html`) desenha os formulários
  a partir dele, sem decidir campo nenhum;
- **a tela Lançar do ERP** recebe o catálogo em `/erp/api/lancamento/dados` e
  ganhou o campo **"O que está sendo pago"** — ele filtra as formas, nomeia e
  torna obrigatórios o nº e a data do documento, mostra a referência, a
  justificativa e a chave de acesso só onde servem, filtra as contas do plano
  pelo tipo e lista os documentos exigidos (cada arquivo agora diz o que é);
- **o servidor** confere pelas `conferir_*` nas DUAS portas — campo que não
  aparece para a escolha feita é DESCARTADO, obrigatório em branco é recusado
  com a lista do que falta.

`PLEITOS_FINANCEIRO.md` é **gerado** do catálogo
(`python -m app.apps.erp.core.titulos.pleitos`), e há teste que recusa o
arquivo diferente do código — mapa velho é revisar regra que não existe.

### O que a obra faz no portal (área "Financeiro", antes "Pagamentos")

| Pleito | Como grava |
|---|---|
| Pedir pagamento (7 subtipos) | rota própria `/erp/portal/api/financeiro/pagamento`, que leva os documentos **na mesma gravação** do título e passa por `_gravar_lancamento` — o MESMO caminho da rota do ERP (separado de `api_criar_titulo` para isso) |
| Fundo fixo | a rota do ERP `/erp/api/prestacao` |
| Contrato de empreita | a rota do ERP `/erp/api/empreitas` (nasce esperando o aval) |
| Medir empreita | a rota do ERP `/erp/api/empreitas/<id>/medicoes` + fotos |
| Cancelar | a rota do ERP `/erp/api/titulos/<id>/cancelar` (só o que é dele, sem dinheiro encostado) |
| O que pedi | `/erp/portal/api/financeiro/lancamentos` e `/titulo/<id>`: pagamentos, fundo fixo, contratos e medições numa lista só, com filtro, o detalhe, **por que está parado** e o cancelamento |

**Em teste: só o ADMIN.** `portal_financeiro.EM_TESTE_SO_ADMIN = True`; para
os demais a área continua aparecendo fechada, com o aviso de sempre. Abrir para
a obra é trocar para `False` — aí vale a ação `lancar`, que o administrativo de
obra já tem.

### O que mudou no ERP (é o que ele vai sentir)

- **Lançar:** "O que está sendo pago" é obrigatório; as formas e os campos
  seguem o subtipo; a conta do credor passou a ser **obrigatória** em Pix/TED
  (conta pendente serve — o beco de 22/09 continua resolvido pelo "+ Cadastrar
  credor"); os documentos exigidos são conferidos pela etiqueta de cada arquivo.
- **Empreitas › Novo contrato:** "por medição" exige a planilha de serviços,
  com unidade; "em parcelas" esconde a planilha e passa a pedir valor total e
  nº de parcelas (antes o valor era a soma dos serviços nos dois casos).
- **Empreitas › Medir:** o período passou a ser **obrigatório** — é ele que
  pega medir duas vezes o mesmo serviço (crítica M3).
- **Fundo fixo:** na prestação de **adiantamento** o período é obrigatório.

### ⚠️ Buracos de alcance fechados de passagem (não foram pedidos, mas são defeito)

- `_gravar_lancamento`: quem é da obra (`PERFIS_SO_PORTAL`) só lança em obra
  dele. O rateio não era conferido contra o alcance.
- `api_criar_prestacao`: a obra de cada despesa, idem.
- `api_empreitas` (POST): o contrato não conferia a obra contra o alcance.
- `empreita.listar`: para quem é da obra usava `obras_do_usuario`, que devolve
  "todas" para quem enxerga por autoria — o administrativo de obra via os
  contratos da empresa inteira. Passou a usar `obras_de_registro_sem_autor`.
- `/erp/api/lancamento/dados` continua devolvendo **todas** as obras e todos
  os credores (não toquei; quem é da obra não abre a tela, mas a API responde).

### Decisões — ESCOLHA MINHA, para ele confirmar

1. **Os subtipos do portal:** nota de material, nota de serviço, frete,
   pessoa física (RPA), conta de consumo, guia, despesa sem nota. **Só no ERP:**
   folha/encargos, financiamento, adiantamento a fornecedor. Locação fica no
   caminho da Locação (o enquadramento já recusa lançar por fora).
2. **Formas por subtipo** (o mapa tem a tabela): pessoa física só Pix/TED;
   guia só Guia; conta de consumo Boleto, Débito automático ou Pix.
3. **Documentos obrigatórios:** a nota (e o boleto, se a forma for boleto); o
   RPA assinado; a fatura; a guia; na despesa sem nota, recibo OU comprovante.
4. **Comprovante de cada despesa do fundo fixo continua NÃO obrigatório** —
   vira crítica para o financeiro, como já era (decisão dele de 07/09/2026
   trata a falta como problema de prestação de contas). Pergunta aberta.
5. **O portal não lança retenção.** O pedido nasce com o valor do documento;
   retenção (ISS, INSS do RPA) o financeiro acerta. No RPA isso quer dizer que
   o líquido do pedido sai maior que o devido até alguém ajustar.
6. **Cancelar** mantém o mínimo de 5 letras do motivo (regra de 12/09/2026).

### Verificação

- 28 testes novos em `tests/test_pleitos_financeiro.py` (regra, coerência do
  catálogo, mapa, área só do admin).
- Suíte das áreas tocadas com banco de verdade (Postgres local descartável):
  verde. Suíte inteira: ver o commit.
- **Percorrido no navegador**, em tamanho de celular, contra um banco
  descartável: Pix mostra a conta e esconde a linha digitável, boleto o
  contrário; credor sem conta → cadastra a chave na hora; sem a nota anexada →
  "Falta anexar: Nota fiscal"; com ela → pedido criado; aparece em "O que
  pedi", abre o detalhe e cancela. Fundo fixo de adiantamento sem período →
  recusado; com período → criado. Contrato sem unidade → recusado; com → criado
  esperando aval (e quem cadastrou **não** consegue aprovar). Medição sem foto
  e sem período → recusada; com → registrada. No ERP, o Lançar com os seis
  subtipos e o contrato "em parcelas" conferidos.
- **NÃO verificado:** a leitura de documento por IA (não há chave aqui) — o
  preenchimento automático pelo documento lido não foi exercitado de verdade.

**Conserto achado pelo próprio teste:** a conferência da medição pedia o
contrato no formulário, mas ele vem no endereço da rota — teria travado TODA
medição do ERP. Corrigido antes do commit.

⚠️ **Perguntar ao dono** se o que está escrito lá já aconteceu — o estado do
mundo muda entre uma sessão e outra, e ele é a única fonte disso.

---

## Decisões desta área

(vazio — a primeira decisão desta área entra aqui, com a data, o motivo e o que
ficou de fora)

---

## Incidentes

(vazio)
