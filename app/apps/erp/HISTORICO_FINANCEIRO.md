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

### 27/11/2026 (2) e (3) — CONTRATO GERADO E ASSINADO, CONTRATOS DA OBRA, EMPREITEIRO PELO DOCUMENTO — ✔ PUBLICADO

Publicado na `main` em 27/11/2026 com o "pode" dele. **Tem a migração 132** —
avisado para apertar "Aplicar atualizações do banco" no mesmo momento.
**Confirmar com ele que apertou** (sem ela, a assinatura pelo sistema fica
indisponível e a tela avisa; o resto funciona). Pendências que continuam com
ele: advogado no texto do contrato, contador na retenção de INSS de PF, e se
quer limitar quem pode assinar por cada empresa.

### 27/11/2026 — O FINANCEIRO NO PORTAL E A REGRA DOS CAMPOS — ✔ PUBLICADO

**Publicado na `main` em 27/11/2026**, com o "pode" dele (*"não tem nada
rodando, pode publicar"* — a pergunta da carga do painel e do Análise de SPs
já foi respondida neste dia). **Sem migração.** Suíte inteira com banco de
verdade verde (só `test_chave_de_segredos[lixo-no-fim]`, que falha igual sem
esta mudança por falta da chave no ambiente de teste).

**O que espera dele:**
1. Revisar `PLEITOS_FINANCEIRO.md` (o mapa de tipos, subtipos, campos e
   condições). Ele pediu exatamente isso: *"faça um mapeamento dos tipos,
   subtipos e campos e condições para que eu possa revisar a lógica"*.
2. Responder as decisões marcadas **ESCOLHA MINHA** abaixo.
3. Testar no portal como administrador (só ele vê a área por enquanto).

---

## 27/11/2026 (3) — O EMPREITEIRO PELO DOCUMENTO (Cartão CNPJ, RG, CNH)

Ele: *"vamos usar a mesma lógica de todo o resto do ERP, que é a importação de
informações a partir da leitura dos documentos (…) utilizar um cartão CNPJ,
uma identidade, RG."*

- **Bloco "Empreiteiro"** no cadastro do contrato (portal e ERP, a mesma peça em
  `_contrato_gerado.html`): pessoa física ou empresa; procura quem já está no
  cadastro (abre preenchido); joga-se o documento no alto — RG/CNH/comprovante
  pelo MESMO leitor da admissão (`documentos_de_pessoa.ler_varios`), Cartão
  CNPJ pelo MESMO leitor das empresas (`arquivo.leitura`) — e ele preenche
  CPF/CNPJ, nome, RG, endereço, telefone e e-mail, tudo editável.
- **Ao cadastrar o contrato**, o empreiteiro é criado ou COMPLETADO com o que foi
  conferido, e os documentos lidos ficam arquivados no cadastro dele
  (`core/titulos/empreiteiro.py`). Substituiu a caixinha "Prestador" do ERP e a
  busca de credor do portal — **é a mesma porta, agora com o documento dentro.**
- **O RG SAIU no mesmo dia.** Ele: *"quando eu falei RG, na verdade eu me
  referi a documento da pessoa de maneira geral (…) o documento para se fazer
  esse tipo de coisa é o CPF e CNPJ."* Quem identifica é o CPF/CNPJ; qualquer
  documento da pessoa serve para a LEITURA (dele saem CPF, nome, endereço) e
  fica arquivado no cadastro. Não há campo de RG nem RG no contrato.
- O dado do credor é procurado pelo **CPF/CNPJ**, não pelo número interno — a
  vigia de escopo pegou: número em sequência no endereço permitiria varrer o
  cadastro de credores.
- Testes: 5 novos em `tests/test_contrato_assinatura_banco.py`. Percorrido no
  navegador (preencher à mão, escolher do cadastro, RG no texto). **A leitura
  com a IA de verdade não foi exercitada aqui** (sem a chave) — dublada no teste.

## 27/11/2026 (2) — O CONTRATO DE EMPREITA GERADO E ASSINADO À DISTÂNCIA; OS CONTRATOS DA OBRA

**Publicado em 27/11/2026. Tem migração: 132** (`contrato_documentos` e
`contrato_assinaturas` — só tabelas novas, nenhuma coluna em tabela existente:
sem o botão apertado, contrato, aval e medição funcionam como antes; só a
assinatura pelo sistema fica indisponível, e a tela diz isso).

### O pedido e as decisões dele (no mesmo dia)

> *"a gente precisaria coletar uma assinatura (…) do empreiteiro (…) duas
> pessoas assinariam. Seria o de fora e um de dentro."*

| Pergunta | Resposta dele |
|---|---|
| quem assina pela empresa | *"uma pessoa do cadastro de colaboradores (…) só selecionar quem é (…) aproveitar já o telefone que tem lá"* |
| por onde vai | *"Vai ser o WhatsApp mesmo. WhatsApp e e-mail"* — e o WhatsApp do sistema **está ligado** (eu tinha dito que nascia desligado; ele corrigiu) |
| o texto | *"A parte jurídica do contrato você faz aí. Botar alguma coisa selecionável (…) e dê espaço também pra gente fazer a digitação (…) a IA deveria dar uma ajustada"* |
| quem acompanha | *"ele vê todos os contratos. Da obra. É aquela mesma lógica do ponto"* |
| fotos | *"fotos dos serviços, para poder demonstrar que aquele serviço está sendo executado"* |

### Como ficou

- **`core/titulos/contrato_assinatura.py`**: o texto-base (10 cláusulas, a partir
  do rascunho `MODELO_CONTRATO_EMPREITA.md`), 8 cláusulas para marcar (multa por
  atraso com o %, entregar limpo, não repassar, ajudantes identificados,
  equipamento emprestado com a lista, material do empreiteiro, nota a cada
  medição, autoriza fotos), dois prazos editáveis (pagar a medição em até X
  dias, aviso de rescisão), o espaço livre que a IA revisa (ela sugere e aponta
  risco — vínculo de emprego, contradição; a pessoa aceita ou mantém o dela).
  Os dados vêm do cadastro: a empresa é a da OBRA, quem assina é um colaborador.
- **O caminho:** cadastro (o documento nasce junto, na mesma gravação — se não
  puder ser gerado, o contrato também não fica) → aval (os links saem AGORA, e
  o contrato vai para **"Esperando as assinaturas"** em vez de vigente) →
  cada um abre `/erp/assinar/<link>` no celular, sem login, pede o código de 6
  dígitos (WhatsApp ou e-mail), confirma o CPF/CNPJ e assina com o dedo →
  com as duas, o PDF final (texto + registro de cada assinatura: hora, canal,
  IP, lacre SHA-256) vai para o arquivo do contrato e ele fica **vigente**.
  Contrato "já tenho assinado" continua como sempre (aval = vigente); no portal
  ele exige o documento anexado.
- **Defesas da página pública:** token de 24 bytes guardado só como resumo e
  trocado a cada reenvio; código vale 15 min, 5 tentativas, 5 códigos/hora;
  CPF/CNPJ conferido; lacre conferido no momento de assinar (texto mudado =
  recusa); teto por IP. Link inválido e vencido respondem igual.
- **Portal › aba Contratos:** todos os contratos das obras da pessoa (saldo,
  % medido, situação da assinatura com "reenviar o link", o texto do contrato,
  os documentos, e cada medição com os serviços, as observações e as fotos).
- **Medição:** foto e observação **por serviço**; com "exige foto", cada serviço
  medido precisa da sua foto (a foto vai como anexo da medição, com a legenda
  "Serviço: …").
- **ERP › Empreitas:** a mesma peça no "Novo contrato" (é UM arquivo,
  `templates/_contrato_gerado.html`, incluído nas duas telas) e o quadro da
  assinatura na ficha, com "reenviar o link".

### ⚠️ O que está escrito para ele ver

- **Validade jurídica**: é assinatura eletrônica (Lei 14.063/2020, MP
  2.200-2/2001 art. 10 §2º), aceita pelas partes na própria cláusula final — a
  mesma base do ponto e dos termos do DP. **Não passou por advogado.** O texto
  da cláusula de natureza civil não protege se a prática for de empregado.
- **Retenção de INSS** do empreiteiro pessoa física: não tratada — confirmar
  com o contador.
- **A busca de quem assina pela empresa** procura qualquer colaborador ativo
  pelo nome (devolve só nome e cargo). Não há lista de "quem pode assinar por
  qual empresa"; se ele quiser restringir, é uma marcação nova no cadastro.
- **A IA da revisão** usa a mesma chave da leitura de documentos
  (OPENAI_API_KEY) e conta no teto de IA de cada pessoa. Não exercitada aqui.

### Mexi em arquivo de outras áreas (registrado em `HISTORICO.md`)

`app/apps/mensageria/core.py` (tipo novo `erp.contrato_assinatura`, WhatsApp),
`tests/test_mensageria.py` e `tests/test_permissoes_rotas.py` (as listas
fechadas: o tipo novo por WhatsApp e as 5 rotas públicas da assinatura, com o
motivo escrito).

### Verificação

- 8 testes novos com banco (`tests/test_contrato_assinatura_banco.py`): o texto
  puxa o cadastro e as cláusulas, o caminho inteiro até vigente com PDF, código
  errado, CPF errado, texto mudado, reenvio invalida o link antigo, contrato sem
  documento continua vigente no aval.
- Percorrido no navegador (celular) contra banco descartável, com o WhatsApp
  gravando em arquivo: cadastro com o contrato gerado → aba Contratos → aval
  por outra pessoa → os dois links saem → cada um pede o código, confirma o
  documento e assina → "assinado pelas duas partes", PDF baixa → contrato
  vigente → medição recusada sem a foto do serviço, aceita com ela → a foto e a
  observação aparecem na ficha do contrato.

**Achado de passagem, de outra área:** desde as migrações 130/131 (chat do DP),
usuário **sem perfil cadastrado** não entra em lugar nenhum — nem ADMIN. É a
regra nova deles e está certa; anoto porque derrubou o meu teste no navegador
até eu dar perfil aos usuários de teste.

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
