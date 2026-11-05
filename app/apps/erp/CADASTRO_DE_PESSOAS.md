# O cadastro de pessoas vai para dentro do ERP

Levantamento de 04/11/2026, com o **apurado dos dois pipes do Pipefy e de três
cenários do Make** que o dono enviou no mesmo dia. Ler antes de escrever
qualquer linha dessa área.

> *"Essa parte de cadastro de colaboradores, a gente ainda está usando o Pipefy
> (…) isso está gerando muito conflito (…) é importante a gente colocar logo o
> cadastro das pessoas dentro do ERP."*
>
> E, ao mandar os arquivos: *"tem muita gambiarra, tem muita puxadinho, tanto
> nos cenários quanto no pipe (…) tem muita coisa dos cenários do Make que nem
> está mais em uso. (…) Fazer um apurado geral. (…) Antes de construir tudo, eu
> acho que tem muita coisa que poderia melhorar."*

---

## 0. AS DECISÕES DELE — já tomadas, não perguntar de novo

| Assunto | Decisão (04/11/2026) |
|---|---|
| Pipefy e planilha | **Morrem.** Enquanto não morrem, **o ERP passa a ALIMENTAR** as planilhas de transporte, GM e diaristas — o sentido se inverte. |
| Quem aprova | **Todos os pedidos vão para o DP.** Quem tiver perfil de DP aprova. |
| Colaborador x prestador | **Um cadastro só: todo mundo é colaborador.** Mantém-se a distinção **CTPS / Prestador de Serviço** e o **tipo de contrato** (CLT determinado/indeterminado, PJ, Autônomo RPA, etc.) como campos. |
| O que importar | **Só quem está ATIVO**, com a documentação. Os ~2.960 desligados **ficam na planilha como base morta.** Daí em diante, cadastro novo nasce no ERP. |
| Portal | Um ambiente com três áreas: **Colaboradores**, **Solicitações do DP** e **Ponto** — como o Pipefy fazia. |
| Sub-bases que o cadastro usa | cargo, município, naturalidade, obra (já existe no ERP), e a pessoa **responsável pela requisição**. |

---

## 1. O TAMANHO DA COISA, em números

| | Pipe CADASTRO COLABORADORES | Pipe SOLICITAÇÕES DP |
|---|---|---|
| criado em | 22/11/2020 | 06/01/2021 |
| cards | **3.664** | **80.992** |
| campos no formulário de abertura | **51** | **106** |
| campos nas fases | 94 | 9 |
| fases | 7 | 10 |
| etiquetas | 8 | 2 |
| webhooks para o Make | 3 | **6** |

**Cards por fase (cadastro):** Cadastro Inicial 2 · Cadastros Adicionais 100 ·
Cadastro Concluído 462 · Solicitações DP 108 · **Colaboradores Desligados
2.960** · Cadastro Cancelado 20 · Colaboradores Afastados 12.

> **Logo: os ATIVOS são ~462 + 100 + 12 ≈ 575 pessoas**, não 3.664. A decisão de
> importar só os ativos reduz o trabalho por **6**.

**Cards por fase (solicitações):** Caixa de entrada 247 · Aguardando
Documentação 2.030 · Sob Análise 49 · Fazendo 4 · **Concluído 22.010** ·
**MobPonto 54.940** · Cancelar 1.712. As fases *Atestado Médico*,
*BH/Compensação/Folga* e *Ajuste de Pontos e Faltas* têm **0 cards** — já estão
mortas (o ponto do ERP absorveu).

⚠️ **O material está INCOMPLETO, e isto é importante:** os dois pipes têm **9
webhooks** apontando para o Make, e o dono mandou **3 cenários**. Faltam até 6.
Em particular, **nenhum dos três cenários ESCREVE o cadastro inicial na aba
"Dados Documentos"** — eles só leem para detectar duplicado. **Quem escreve a
linha nova da planilha está num cenário que não veio.**

---

## 2. O QUE OS CENÁRIOS DO MAKE FAZEM HOJE

### 2a. `DP - Cadastro de Colaboradores - Novo Card`

1. Card novo no pipe →
2. **procura o CPF na aba "Dados Documentos"**;
3. **se NÃO existe:** atualiza campos no Pipefy, avisa por **WhatsApp (Z-API)**
   ou **Telegram** (dois caminhos paralelos, um por rota), baixa a foto do
   colaborador e **cadastra no MobPonto** — com duas tentativas de desempate
   quando o CPF "está sendo utilizado por outro" ou falta `cpf_responsavel`;
4. **se JÁ existe:** manda e-mail, avisa por WhatsApp/Telegram e **APAGA o card**;
5. grava uma linha de log numa planilha de Log.

→ **É daqui que vem o "o ERP também tem que alertar se o cadastro já existe".**
Hoje o aviso chega **depois** de a pessoa preencher 51 campos, e a punição é o
card ser apagado.

### 2b. `DP - SDP - Novo Card` (o maior: 21 roteadores)

Trata, por tipo de solicitação: atualiza CPF/nome no card, liga o card ao
cadastro, e então — conforme o tipo — **baixa o anexo e sobe para o Dropbox**
(15 módulos de upload), pega arquivo do **Google Drive** (10 módulos), converte
documento (**CloudConvert**), manda documento por **Telegram**, escreve a chave
Pix na planilha (colunas 73 e 74), calcula parcela de desconto em folha numa aba
`RateioDescFolha`, atualiza uma aba `Relatório`, e grava log.

### 2c. `DP - SDP - Card Concluído`

Ao concluir o card: **17 atualizações de campo no Pipefy**, cria/apaga registro
nas **tabelas do Pipefy** (Colaboradores / Colaboradores Desligados), e escreve
na aba "Dados Documentos":

| Colunas | O que grava | Em que pedido |
|---|---|---|
| 3, 23, 53–55 | tipo de readmissão, data de início, limpa 3 colunas | Readmissão |
| 64–69 | categoria e valor do **auxílio-alimentação**, categoria e valor do **auxílio-transporte**, observação da ajuda de custo, obra | Ajuda de custo, VT, admissão de prestador |
| 73–74 | tipo e valor da **chave Pix** | Atualizar Pix |

E acrescenta linha numa planilha separada, **"Alterações de Frequência"**, com
nome, CPF, tipo, motivo, CID, datas e os horários de até **3 dias** de ajuste —
que é o que alimentava o ponto antigo.

---

## 3. O QUE ESTÁ MORTO OU MORRENDO (o "apurado" que ele pediu)

| O que | Situação | Por quê |
|---|---|---|
| **MobPonto** (3 chamadas de API no cenário de cadastro) | **MORTO** | o ponto do ERP substituiu. A fase "MobPonto" do pipe tem 54.940 cards: é arquivo morto. |
| Aba **"Alterações de Frequência"** | **MORTA** | alimentava o MobPonto. O ponto do ERP tem ocorrência de verdade, com aprovação. |
| Fases *Atestado Médico*, *BH/Compensação/Folga*, *Ajuste de Pontos e Faltas* | **MORTAS** | 0 cards. Absorvidas pelo ponto. |
| Campos de **ajuste de ponto em 3 dias fixos** (20 campos: data/entrada/saída/almoço × 3) | **MORTO** | o ponto do ERP ajusta por período, sem teto de 3 dias. |
| **Dropbox** (15 uploads) | **A SUBSTITUIR** | o ERP já arquiva documento com catálogo, validade, sigilo e aviso de vencimento. |
| **CloudConvert**, **Telegram SendDocument** | **A SUBSTITUIR** | o ERP gera PDF e tem bot próprio. |
| Tipos **sem automação nenhuma** no Make: *Foto Relógio de Ponto*, *Solicitações Diversas*, *Cartão Somapay 2ª Via*, *Cartão VT 2ª Via*, *Documentação Admissão* | **MANUAL HOJE** | o card é só um recado; alguém faz à mão. Decidir um por um se vira pedido no ERP ou sai. |
| **Z-API + Telegram em rotas paralelas** para o mesmo aviso | **DUPLICADO** | a mensageria do ERP já decide o canal por tipo de mensagem, num lugar só. |

### 3b. As gambiarras do pipe que o ERP RESOLVE sem esforço

Vale listar porque cada uma delas é trabalho que **não** precisa ser refeito:

1. **Chave Pix em 6 campos** (tipo + um campo por tipo: e-mail, telefone, CPF,
   CNPJ, aleatória) — limitação do Pipefy. O ERP já tem **um** campo de tipo e
   **um** de valor, com máscara pelo nome do campo.
2. **Dependentes em 11 campos fixos** ("Quantidade de Dependentes" com opções
   *Filho 1*…*Filho 5*, mais Nome e Data de cada) — teto de 5 filhos e nenhum
   dependente que não seja filho ou cônjuge. O ERP resolve com **uma tabela**.
3. **Quatro campos para a mesma obra**: *Código da Obra*, *Obra*, *Obra (C.
   Diários)*, *Código da Obra Readmissão*. No ERP a obra é **uma**.
4. **"Cargo" (conector) + "Cargo [ ]" (texto)** — o texto é cópia do conector só
   para a planilha conseguir ler. Some.
5. **"Nome Completo" + "Nome Completo (corrigido)"** — o segundo existe porque o
   primeiro não podia ser editado depois. No ERP, edita-se com trilha.
6. **"Table Record ID", "Table Record ID Desligado", "ID do Card", "Criar Novo
   Card Via Automação"** — identificadores técnicos dentro do formulário, que a
   pessoa vê. Somem.
7. **"Você não possui autorização para esta solicitação"** como *checklist* —
   permissão feita na mão. O ERP tem perfis de verdade.
8. **"Salário Extenso"** digitado à mão — o ERP gera do valor.
9. **Jornada de Trabalho** como texto de duas opções fixas — o ponto do ERP tem
   escalas de verdade (44h, 12x36, com saída diferente na sexta).
10. **"Colaborador 1 - Ausência" / "Colaborador 1 - Ajuste"** — começo de uma
    lista que nunca escalou para 2, 3, 4.

---

## 4. O CADASTRO, CAMPO POR CAMPO — o que o ERP precisa ganhar

O pipe de cadastro coleta isto (× = o ERP **não** tem onde guardar):

**Identificação:** nome completo ×(tem), data de nascimento ×, sexo ×,
naturalidade (município) ×, UF da naturalidade ×, escolaridade ×, estado civil
×, nº identidade ×, órgão emissor ×, data de emissão ×, UF do documento ×, CPF
(tem), nome da mãe ×, **nome do pai** ×, nº CTPS ×, série da CTPS ×, data de
emissão da CTPS ×, UF da CTPS ×, nº PIS ×, tipo de CTPS (física/digital) ×,
primeiro emprego? ×.

**Endereço:** logradouro ×, número ×, bairro ×, município ×, UF ×, CEP ×.

**Contato:** telefone (tem).

**Pagamento:** tipo de chave Pix (tem), valor da chave (tem), pagamento
diarista/mensalista ×, valor da diária (tem), valor do mês ×, salário ×,
salário por extenso ×(gerado).

**Vínculo:** empresa para registro ×, CNPJ para admissão ×, tipo de documentação
(CTPS/Prestador) ×, tipo de contrato ×, matrícula (tem), cargo ×(só `funcoes`),
código CBO ×, convenção ×, jornada ×(escala do ponto), contrato de experiência
×, data de início (tem como admissão), data de admissão (tem), objeto da obra
×(tem na obra), obra (tem).

**Família:** nome/nascimento/CPF do cônjuge ×, dependentes ×, solicitar
salário-família? ×.

**Benefícios:** categoria e valor do auxílio-alimentação ×(só valor), categoria e
valor do auxílio-transporte ×(só valor), meio de transporte para a obra ×,
gratificação mensal (recebe/valor) ×, parcela única ×, recebe via SP ×, cesta
básica extra ×, adicional de transferência ×, vale-refeição ×, observação da
ajuda de custo ×.

**Requisição:** requisição feita por terceiro? ×, requerente ×, responsável pela
solicitação ×.

**Documentos anexos (6 obrigatórios + 2 opcionais):** documento de identificação,
CTPS página da foto, CTPS página das informações, CPF, comprovante de endereço,
foto do colaborador, certidão/RG dos dependentes, documentos diversos, **ASO**.
→ O catálogo do ERP já prevê todos, menos **foto do colaborador** (o ponto tem
foto cadastral — conferir se é a mesma) e **comprovante de endereço**.

**As listas de opções que vêm prontas do pipe** (reusar, não inventar):
escolaridade (8), estado civil (5), órgão emissor (SSP/DETRAN/MTE/ME), UF (27),
tipo de contrato (9: CLT determinado, CLT indeterminado, PJ, Autônomo RPA,
Autônomo Mensalista, Jovem aprendiz, Estágio, Temporário, Pró-labore), contrato
de experiência (5), categoria de alimentação (4: Seg-Qui, Seg-Sex, Mensal, Mês),
categoria de transporte (9: Diário, Mensal, Seg e Sex, Vale Transporte, Diário e
VT, Veículo, Motocicleta, Bicicleta, Cartão), meio de transporte (4), chave Pix
(5), empresa (3 CNPJs), CNPJ de admissão (3).

---

## 5. OS 28 TIPOS DE SOLICITAÇÃO — para onde cada um vai

| Tipo no pipe | Onde fica no ERP |
|---|---|
| Encaminhamento de Atestado Médico | **já existe** (ponto) |
| Pedido de Afastamento ou Licença | **já existe** (ponto) |
| Solicitação de Férias | **já existe** (ponto) |
| Ajuste de Ponto e Faltas | **já existe** (ponto) |
| Foto Relógio de Ponto | **já existe** (o ponto tira foto na batida) |
| Desligamento de Colaborador / de Prestador | **criar** — um só, com forma de desligamento, aviso prévio, último dia, data de saída, faltas a descontar, checklist |
| Cancelar Solicitação de Desligamento | **criar** (cancelar pedido, com motivo) |
| Readmissão de Colaborador | **criar** — achar o desligado e trazer de volta, sem apagar o vínculo anterior |
| Admissão de Prestador de Serviço | **criar** — é a troca de vínculo (prestador → CTPS) |
| Pré-Cadastro de Colaborador | **criar** (cadastro em duas etapas, como o pipe faz) |
| Solicitação de Cadastro de Dependente | **criar** + tabela de dependentes |
| Solicitação de Vale Transporte | **criar** (com as 9 categorias) |
| Solicitação de Ajuda de Custo | **criar** |
| Solicitação de Alteração de Cargo | **criar** (cargo pleiteado, deferido, salário, justificativa) |
| Atualização de Dados Cadastrais / de Telefone / Chave Pix | **criar** — um pedido só, "atualizar dado", com o que muda |
| Lançar Desconto em Folha | **criar** (valor total e parcelas — o cálculo está numa aba de planilha hoje) |
| Documentação Admissão / Rescisão | **criar** (ou resolver com o Arquivo, que já guarda documento por pessoa) |
| Gerar Crachá (DP) | **decidir** — hoje é geração de arquivo |
| Planilhas de Cadastro (DP) / Contrato, Etiqueta, Termos (DP) | **decidir** — hoje gera documento a partir do cadastro |
| Relatório de DCs | **já existe** parecido (despesa de colaborador no ERP) |
| Cartão Somapay 2ª Via / Cartão VT 2ª Via | **decidir** — hoje é recado manual com aviso de custo |
| Solicitações Diversas | **criar** (o "outros", com texto livre) |

---

## 6. ORDEM PROPOSTA

1. **O cadastro completo** — os campos da seção 4, as listas de opções do pipe,
   a tabela de dependentes, o alerta de duplicado **ao digitar o CPF** (com
   oferta de readmissão quando a pessoa está desligada) e as sub-bases (cargo
   com CBO, município/naturalidade).
2. **Importar os ~575 ativos** da planilha, com a documentação. Relatório do que
   não casou. *(Os desligados ficam na planilha, por decisão dele.)*
3. **O portal de solicitações** — a fila de pedidos da seção 5, toda no DP,
   reusando a máquina de aprovação que o ponto já tem.
4. **Vínculo com história** — readmissão, prestador → CTPS, histórico de cargo,
   de salário e de obra.
5. **Inverter o sentido** — o ERP passa a escrever nas planilhas de transporte,
   GM e diaristas (hoje é o Make quem escreve, nas colunas 64–69 e 73–74). Só
   depois disso o Pipefy e os cenários morrem.

---

## 7. O QUE AINDA FALTA SABER

1. **Os outros até 6 cenários do Make** — em especial o que ESCREVE a linha nova
   na aba "Dados Documentos". Sem ele, não sei o caminho completo do cadastro
   novo até a folha.
2. **As tabelas do Pipefy** (*Cargos*, *Colaboradores*, *Colaboradores
   Desligados*) — o conteúdo delas, para o cadastro de cargo nascer preenchido.
3. **Quais abas da planilha a folha realmente lê** hoje, e com que nome de
   coluna — é o contrato que o ERP vai ter de honrar na fase 5. A Análise de SPs
   já lê ~30 colunas e conhece os nomes; as outras 48 continuam sem mapa.
4. **Os cinco tipos manuais** (Foto Relógio, Diversas, 2ª via de cartão,
   Documentação Admissão): viram pedido no ERP ou saem?
