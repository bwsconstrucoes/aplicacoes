# Cadastro de colaboradores: planilha × ERP

O dono (27/11/2026): *"a planilha já está hoje desatualizada. Então, o cadastro é o ERP.
(…) A gente precisa saber exatamente quais campos, quais colunas da planilha são
observadas para a montagem das telas de pagamento (…) e quais campos no ERP são
correspondentes (…) para definitivamente deixar de usar a planilha."*

Levantado no código (as colunas que a cópia guarda e quem lê cada uma) e na tela de
cadastro do ERP (`erp_colaboradores.html`: o RÓTULO que a pessoa vê e o campo em que ele
grava). As correspondências marcadas "dono" foram confirmadas por ele em 27/11/2026.

## Como é hoje

- As telas de pagamento leem a **cópia da planilha** ("Registro de Colaboradores", aba
  "Dados Documentos", botão "Atualizar cadastro"). O ERP só completa a cópia nos
  diaristas (26/11) e na data de admissão do rateio da folha (27/11).
- Quem foi admitido pelo ERP depois de 25/11 não está na planilha → "sem cadastro" na
  Folha da Contabilidade; fora de alimentação/transporte e gratificados.

## O mapa: coluna da planilha → campo no ERP

**Entrega** = o ERP já manda à folha (`erp/core/cadastros/pessoas.py › para_folha`).
**Falta mandar** = o campo existe no ERP, mas não vai à folha — é acrescentar.

| Coluna da planilha | Rótulo no ERP (campo) | Quem usa no pagamento | Situação |
|---|---|---|---|
| CPF | CPF (`cpf`) | todas | Entrega |
| Nome Completo | Nome (`nome`) | todas | Entrega |
| ID Fortes (col. BU) | ID Fortes (`id_fortes`) | Folha da Contabilidade (liga o Fortes ao CPF) | Entrega |
| Matrícula | Matrícula (`matricula`) | ficha, cadastro BeeVale/SomaPay | Entrega |
| Celular | Telefone (`telefone`) | cadastro BeeVale/SomaPay | Entrega |
| Data de Nascimento | Data de nascimento (`data_nascimento`) | ficha, cadastro BeeVale/SomaPay | Entrega |
| Cargo | Cargo (`cargo_id`) | telas da folha | Entrega |
| Tipo | Vínculo (`vinculo`) | **diaristas** | Entrega |
| Tipo de Contrato | Tipo de contrato (`tipo_contrato`) | diaristas, DC, avulso, gratificados | Entrega |
| Fase Atual | Situação (`situacao`) — **a confirmar** (ou Etapa?) | folha (saiu / afastado), auxílios, gratificados | Entrega |
| Objeto Obra | **Obra de cadastro** (`obra_id`) — dono | folha (obra do cadastro), auxílios, diaristas, gratificados, DC, avulso | Entrega como `obra_cadastro`; ⚠️ a folha hoje lê primeiro o `obra_codigo`, que no ERP é a obra do PONTO — ajustar na troca |
| Data de Início | Data de início (`data_inicio`) | diaristas, folha | Entrega |
| Data de Admissão | Data de admissão (`admissao`) | folha (rateio), diaristas | Entrega |
| Data do Aviso Prévio | Data do aviso prévio (`aviso_previo`) | folha ("saindo") | Entrega |
| Último dia Trabalhado | Último dia trabalhado (`ultimo_dia`) | folha, auxílios | Entrega |
| Data de Saída | Data de saída (`demissao`) | folha ("já saiu"), auxílios | Entrega |
| Categoria Auxílio Alimentação | Categoria do auxílio-alimentação (`categoria_alimentacao`) | **alimentação** | Entrega (traduzida para a palavra da folha) |
| Valor Auxílio Alimentação | Valor do auxílio-alimentação (`aux_alimentacao`) | **alimentação** | Entrega ("Não recebe" zera) |
| Categoria Auxílio Transporte | Categoria do auxílio-transporte (`categoria_transporte`) | **transporte** | Entrega (traduzida) |
| Valor Auxílio Transporte | Valor do auxílio-transporte (`aux_transporte`) | **transporte** | Entrega |
| Observação Ajuda de Custo | Observação da ajuda de custo (`observacao_auxilio`) | auxílios | Entrega |
| Valor da Diária | Valor da diária (`valor_diaria`) | **diaristas**, DC | Entrega |
| Recebe Gratificação Mensal (col. BE) | Recebe gratificação mensal? (`recebe_gratificacao`) — dono | **gratificados** | **Falta mandar** |
| Valor da Gratificação | Valor da gratificação (`valor_gratificacao`) — dono | **gratificados** | Entrega |
| Valor do Mês (col. BG) | Valor do mês (`valor_mes`) — dono | **gratificados** (mensalistas) | Sai como `valor_mes`; a folha lê `valor_mensal` — **ligar** |
| Recebe Parcela Única | Recebe em parcela única? (`parcela_unica`) | gratificados | Entrega |
| Recebe via SP (col. BI) | Recebe via SP? (`recebe_via_sp`) | gratificados / via SP | Entrega |
| Nome Credor | Razão social (PJ) (`razao_social_pj`) — dono | gratificados via SP | **Falta mandar** |
| Pix CNPJ | CNPJ (PJ) (`cnpj_pj`) — dono | gratificados via SP | **Falta mandar** |
| Chave Pix Tipo | Tipo da chave Pix (`pix_tipo`) | gratificados via SP | Entrega |
| Chave Pix | Chave Pix (`pix_chave`) | gratificados via SP | Entrega |
| CNPJ Admissão (col. BT) | **Empresa de registro** (`empresa_id` → CNPJ da empresa; sem empresa, o `cnpj_admissao` antigo) — dono | **separar a folha por empresa** | **Falta mandar**. A folha compara só o CNPJ (14 dígitos): o texto é "nome – CNPJ" no ERP e "CNPJ – nome" na planilha; tira-se o CNPJ pelos dígitos |
| RG | Nº da identidade (`rg`) — dono | cadastro BeeVale/SomaPay | **Falta mandar** |
| Data de Emissão (do documento) | Emissão do documento (`rg_emissao`) — dono | idem | **Falta mandar** |
| Órgão Emissor | Órgão emissor (`rg_orgao`) — dono | idem | **Falta mandar** |
| UF (do documento) | UF do documento (`rg_uf`) — dono | idem | **Falta mandar** |
| Nome da Mãe | Nome da mãe (`nome_mae`) — dono | idem | **Falta mandar** |
| Sexo | Sexo (`sexo`) — dono | idem | **Falta mandar** |
| CEP | CEP (`cep`) — dono | idem | **Falta mandar** |
| Logradouro | Logradouro (`logradouro`) — dono | idem | **Falta mandar** |
| Número | Número (`numero_endereco`) — dono | idem | **Falta mandar** |
| Complemento | Complemento (`complemento`) | idem | **Falta mandar** |
| Bairro | Bairro (`bairro`) — dono | idem | **Falta mandar** |
| Cidade | Município (`municipio`) — dono | idem | **Falta mandar** |
| Estado | UF (`uf`) | idem | **Falta mandar** |
| E-mail | E-mail (`email`) — dono | idem | **Falta mandar** |
| Nº Registro Pipefy | (`ref_pipefy`) | link do card em Arquivos gerados | Entrega (morre com o Pipefy) |
| Paga por BeeVale | — | — | **Sai**: desativado, não se usa mais (dono) |
| Convenção | Convenção (`convencao_id`) | — (exibição) | Entrega |

## Em aberto

- **Fase Atual:** a planilha guardava a fase do card do Pipefy. A folha só procura
  nela "desligado" e "afastado" (e o texto aparece na coluna "Fase Atual"). No ERP há
  **Situação** (Ativo, Desligado, Afastado…) e **Etapa** (Inicial, Conferência,
  Concluído…). Proposta: Situação. A confirmar com o dono.

## Os passos da troca

1. ERP (`para_folha`) passa a mandar o que está "Falta mandar" acima — `recebe_gratificacao`,
   `razao_social_pj`→`nome_credor`, `cnpj_pj`→`pix_cnpj`, o CNPJ da empresa de registro
   →`cnpj_admissao`, `valor_mes`→`valor_mensal` e os documentos/endereço. É código do
   ERP (área Pessoal e DP): dizer na resposta ao mexer.
2. A "obra do cadastro" da folha passa a vir do `obra_cadastro` (Obra de cadastro), e
   não do `obra_codigo` (a obra do ponto).
3. Conferência pessoa a pessoa, campo a campo, cópia × ERP (só os ativos), com o número
   de diferenças por campo — antes de trocar.
4. Troca da fonte tela por tela; a cópia da planilha fica só como consulta até o dono
   mandar apagar.
