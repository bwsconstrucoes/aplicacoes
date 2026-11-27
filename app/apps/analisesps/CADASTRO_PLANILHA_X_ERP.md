# Cadastro de colaboradores: planilha × ERP (27/11/2026)

O dono: *"a planilha já está hoje desatualizada. Então, o cadastro é o ERP. (…) A gente
precisa saber exatamente quais campos, quais colunas da planilha são observadas para a
montagem das telas de pagamento (…) e quais campos no ERP são correspondentes (…) para
saber se está tudo ok e definitivamente deixar de usar a planilha."*

Levantado lendo o código, não suposto. Fontes: `colaboradores.py` (as colunas que a
cópia guarda e quem lê cada uma), `erp/core/cadastros/pessoas.py › para_folha` (o que o
ERP entrega para a folha) e o modelo `Colaborador` do ERP.

## Como é hoje

- **A cópia da planilha** ("Registro de Colaboradores", aba "Dados Documentos") é lida
  pelo botão "Atualizar cadastro" e guardada em `analisesps.colaborador`. Quase todas as
  telas de pagamento leem SÓ dela.
- **O ERP completa a cópia** (`colaboradores.para_a_folha`) em dois lugares só: os
  **diaristas** (26/11) e a **data de admissão do rateio da folha** (27/11). Completa
  quer dizer: entra quem não está na cópia, e quem voltou pelo ERP (readmissão) usa a
  ficha do ERP.
- Quem foi admitido pelo ERP depois de 25/11 (a admissão não vai mais à planilha) **não
  existe** para a Folha da Contabilidade ("sem cadastro"), alimentação/transporte e
  gratificados.

## As colunas usadas nas telas de pagamento, e o correspondente no ERP

Legenda — **ERP entrega**: já sai no `para_folha` com o mesmo nome. **Existe, não sai**:
o campo existe na ficha do ERP mas o `para_folha` não o entrega (falta uma linha).
**Conferir**: existe, mas o significado ou o formato pode ser outro.

| Coluna da planilha | Campo | Quem usa (pagamento) | No ERP | Situação |
|---|---|---|---|---|
| CPF | `cpf` | todas | `cpf` | ERP entrega |
| Nome Completo | `nome` | todas (nome no arquivo do banco) | `nome` | ERP entrega |
| ID Fortes (col. BU) | `id_fortes` | **Folha da contabilidade** — é o que liga a linha do Fortes ao CPF | `id_fortes` | ERP entrega — **conferir se todos têm** |
| Fase Atual | `fase` | folha (já saiu / saindo / afastado), auxílios, gratificados, diaristas | `situacao` (ATIVO…) | **Conferir**: a planilha usa as fases do Pipefy; o sistema procura "desligad" e "afastad" no texto |
| Cargo | `cargo` | telas da folha (exibição), DC, avulso | `cargo` (tabela de cargos) | ERP entrega |
| Tipo | `tipo` | **diaristas** (Prestador × CTPS decide a diária) | `vinculo` / tipo de contrato | ERP entrega (`_tipo_para_a_folha`) |
| Tipo de Contrato | `tipo_contrato` | diaristas (RPA), DC, avulso, gratificados | `tipo_contrato` | ERP entrega (rótulo) |
| Objeto Obra | `obra_cadastro` | folha (obra do cadastro), auxílios, diaristas, gratificados, DC, avulso | `obra_id` → código | **Conferir**: a planilha traz o NOME da obra; o ERP, o CÓDIGO |
| Código da Obra | `obra_codigo` | DC, calendário, faturamento — e **a "obra do cadastro" da folha** (`resolver_obra` usa este código primeiro) | obra ATUAL (a do ponto) | **⚠️ Mudou de sentido** em 20/11: no ERP é a obra do ponto. Lida do ERP, a coluna "Obra — cadastro" da folha viraria a obra do ponto e o aviso "cadastro ≠ ponto" perderia o sentido — a troca tem de ler `obra_cadastro` (o código da ficha) para isso |
| Data de Início | `data_inicio` | diaristas (dias antes da admissão = diária), folha | `data_inicio` | ERP entrega |
| Data de Admissão | `data_admissao` | folha (rateio desde a admissão), diaristas | `admissao` | ERP entrega |
| Data do Aviso Prévio | `aviso_previo` | folha ("saindo") | `aviso_previo` | ERP entrega |
| Último dia Trabalhado | `ultimo_dia` | folha, auxílios | `ultimo_dia` | ERP entrega |
| Data de Saída | `data_saida` | folha ("já saiu"), auxílios | `demissao` | ERP entrega |
| Categoria Auxílio Alimentação | `modo_alimentacao` | **alimentação** | `categoria_alimentacao` | ERP entrega (traduzido para a palavra da folha) |
| Valor Auxílio Alimentação | `valor_alimentacao` | **alimentação** | `aux_alimentacao` | ERP entrega ("não recebe" zera) |
| Categoria Auxílio Transporte | `modo_transporte` | **transporte** | `categoria_transporte` | ERP entrega (traduzido) |
| Valor Auxílio Transporte | `valor_transporte` | **transporte** | `aux_transporte` | ERP entrega |
| Observação Ajuda de Custo | `observacao_auxilio` | auxílios | `observacao_auxilio` | ERP entrega |
| Valor da Diária | `valor_diaria` | **diaristas**, DC | `valor_diaria` | ERP entrega |
| Valor da Gratificação | `valor_gratificacao` | **gratificados** | `valor_gratificacao` | ERP entrega |
| Recebe Parcela Única | `parcela_unica` | gratificados | `parcela_unica` | ERP entrega |
| Recebe Gratificação Mensal (col. BE) | `recebe_gratificacao` | **gratificados** (quem entra) | `recebe_gratificacao` | **Existe, não sai** |
| Valor do Mês (col. BG) | `valor_mensal` | **gratificados** (mensalistas) | `valor_mes` | **Existe, sai com outro nome** (`valor_mes`) |
| Recebe via SP (col. BI) | `recebe_via_sp` | gratificados / via SP | `recebe_via_sp` | ERP entrega |
| Nome Credor | `nome_credor` | gratificados via SP (PJ) | `razao_social_pj`? | **Conferir** — não sai; o mais próximo é a razão social PJ |
| Pix CNPJ | `pix_cnpj` | gratificados via SP (PJ) | `cnpj_pj`? | **Conferir** — não sai |
| Chave Pix Tipo / Chave Pix | `pix_tipo`, `pix_chave` | gratificados via SP | `pix_tipo`, `pix_chave` | ERP entrega |
| CNPJ Admissão (col. BT) | `cnpj_admissao` | **separar a folha por empresa** (BWS × outras) | `cnpj_admissao` | **Existe, não sai** |
| Nº Registro Pipefy | `card_pipefy` | Arquivos gerados (link do card) | `ref_pipefy` | ERP entrega (morre com o Pipefy) |
| Matrícula, Celular, Data de Nascimento | — | planilha de cadastro BeeVale/SomaPay, ficha | `matricula`, `telefone`, `data_nascimento` | ERP entrega |
| RG (nº, emissão, órgão, UF), Nome da Mãe, Sexo, endereço, e-mail | `documentos` | **planilha de cadastro BeeVale/SomaPay** | `rg`, `rg_emissao`, `rg_orgao`, `rg_uf`, `nome_mae`, `sexo`, `cep`, `logradouro`, `numero_endereco`, `bairro`, `municipio`, `uf`, `email` | **Existe, não sai** |
| Paga por BeeVale | `paga_por_beevale` | só a tela Colaboradores (exibição) | — | não existe no ERP (não pesa no pagamento) |
| Convenção | `convencao` | — (exibição) | `convencao` | ERP entrega |

## O que falta para desligar a planilha

1. **Acrescentar ao `para_folha` do ERP** o que existe e não sai: `cnpj_admissao`,
   `recebe_gratificacao`, `valor_mensal` (o `valor_mes`), os documentos da planilha de
   cadastro do BeeVale/SomaPay e — se o dono confirmar a correspondência —
   `nome_credor`/`pix_cnpj` (razão social e CNPJ PJ). É do ERP (área Pessoal e DP).
2. **Ajustar a leitura da obra do cadastro** (`colaboradores.resolver_obra`) para, vindo
   do ERP, usar `obra_cadastro` (a obra da ficha) e não `obra_codigo` (a do ponto).
3. **Confirmar com o dono:** (a) "Nome Credor" e "Pix CNPJ" da planilha = razão social e
   CNPJ PJ do ERP? (b) as situações do ERP (ATIVO, DESLIGADO, AFASTADO…) cobrem o que as
   fases do Pipefy diziam? (c) todo colaborador ativo tem o **ID Fortes** no ERP?
4. **Comparar antes de trocar:** uma conferência pessoa a pessoa, campo a campo, entre a
   cópia e o ERP (só os ativos) — quantas divergências por campo e quem são. É o que diz
   "está tudo ok" com número, e não por suposição.
5. **Trocar a fonte**, tela por tela, para o ERP; a cópia da planilha fica só como
   consulta até o dono mandar apagar.
