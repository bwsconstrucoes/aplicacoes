# DE-PARA DO DP — os campos como o Pipefy os tem

> **Fonte:** exportação dos dois pipes pelo GraphQL do Pipefy, em
> 04/11/2026 — `SOLICITAÇÕES DP` (id 301514316, 106 campos no formulário
> inicial, 10 fases) e `CADASTRO COLABORADORES` (id 301487297, 51 campos,
> 7 fases). Só a ESTRUTURA entra aqui: nenhum nome de pessoa, e-mail,
> webhook ou token.

## Por que este arquivo existe

Porque eu inventei nome de campo, e ele cobrou — com razão — em
10/11/2026:

> *"Prestador que passa a ter carteira. Qual foi o termo que eu usei? Por
> que não está usando o termo que está no Pipe? Por que está querendo
> inventar a roda de novo? Uma coisa que já está certa, já está
> funcionando, já está consolidada. Para que inventar?"*

E, no mesmo dia:

> *"Pagar alimentação, aí segunda, quinta, esse termo está errado. Como é
> que estava no Pipe? Para usar igual. É para usar a linguagem do Pipe, o
> pessoal já está acostumado com o que está lá."*

**A regra que nasce daqui:** rótulo de campo do DP não se traduz, não se
melhora e não se inventa. Se está no Pipe, está aqui — e a tela usa este
texto, letra por letra. Campo que o Pipe não tem, o ERP não inventa sem
ele mandar.

⭐ = obrigatório no Pipe. No ERP, campo obrigatório leva asterisco na tela,
como lá — pedido dele no mesmo dia: *"Pipe, todo campo obrigatório, ele
tem um asterisco, facilita. Aqui não, ninguém sabe o que é obrigatório."*

## ⚑ OS DOIS PIPES VIRAM UM SÓ

Decisão dele em 10/11/2026: *"nós temos dois pipes, um de cadastro para
colaboradores e o outro de solicitações DP. Só que a gente está meio que
unificando, né? Vai ser tudo DP, porque tudo é solicitação DP. O cadastro
e as outras solicitações."*

Ou seja: **o cadastro é mais um tipo de solicitação**, na mesma fila, com
a mesma caixa de entrada e a mesma trilha. Não é uma tela à parte.

## ⚑ QUEM PREENCHE O QUÊ — a leitura mais importante deste arquivo

No Pipe, o que está no **formulário inicial** é da OBRA (quem pede); o que
está numa **fase** é do **DP**. Essa separação é o que eu tinha quebrado:

| Campo | Onde está no Pipe | Quem preenche |
|---|---|---|
| Matrícula | fase *Cadastro Inicial* | **DP** |
| Convenção | fase *Cadastro Inicial* | **DP** |
| Categoria e Valor do Auxílio Alimentação | fase *Cadastro Concluído* | **DP** |
| Categoria e Valor do Auxílio Transporte | fase *Cadastro Concluído* | **DP** |
| Forma de Desligamento | formulário inicial | obra |
| Data do Aviso Prévio | formulário inicial | obra |

E o que **não existe em pipe nenhum**, e portanto o ERP não pede a quem
solicita: *último dia trabalhado*, *faltas a descontar*, *data de saída*
(exceto no desligamento de PRESTADOR, onde ela existe), *conta no imposto
de renda*, *tipo de contrato* (o nome certo é **Tipo de Readmissão** na
readmissão, e **Tipo de Documentação para Cadastro** no cadastro).

---

# Pipe SOLICITAÇÕES DP

## Os 32 tipos, e o que cada um mostra

### Admissão de Prestador de Serviço

- **Cadastro Colaboradores Conexão** ⭐ — `cadastro_colaboradores_conex_o` · connector
- **Nº CTPS** ⭐ — `n_ctps` · number
- **Nº PIS** ⭐ — `n_pis` · number
- **CTPS (Página com a Foto do Colaborador)** ⭐ — `ctps_p_gina_com_a_foto_do_colaborador` · attachment
- **CTPS (Página com as Informações do Colaborador)** ⭐ — `ctps_p_gina_com_as_informa_es_do_colaborador` · attachment
- **Solicitar Salário Família?** ⭐ — `solicitar_sal_rio_fam_lia` · radio_horizontal
  - opções: `Sim` · `Não`
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector
- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Código da Obra** ⭐ — `c_digo_da_obra` · connector
- **Anexo (Certidão de Nascimento, RG dos Dependentes ou Certidão de Casamento)** ⭐ — `anexo_certid_o_de_nascimento_rg_dos_dependentes_ou_certid_o_de_casamento` · attachment

### Ajuste de Ponto

- **Ajuste de Ponto** ⭐ — `checklist` · checklist_vertical
  - opções: `Atualizado Registro no Sistema de Ponto`

### Ajuste de Ponto e Faltas

- **Tipo de Ajuste ou Lançamento** ⭐ — `tipo_de_ajuste` · select
  - opções: `Dia Específico (Ponto Padrão)` · `Dia Específico (Informar Horário)` · `Período Específico (Ponto Padrão)`
  - ajuda: Utilizar 'Período Específico (Ponto Padrão)' somente quando não houver nenhuma batida no dia.
- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Código da Obra** ⭐ — `c_digo_da_obra` · connector
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector
- **Motivo** ⭐ — `motivo` · select
  - opções: `Falta não Justificada (Período Integral)` · `Falta não Justificada (Manhã)` · `Falta não Justificada (Tarde)` · `Serviço Externo` · `Falta de Equipamento` · `Esquecimento` · `Problema Técnico` · `Compensação` · `Viagem à Serviço` · `Abono de Falta pela Gestão` · `Ponto Facultativo` · `Declaração de Comparecimento` · `Declaração DETRAN` · `Doação de Sangue` · `Serviços Prestado à Justiça Eleitoral` · `Banco de Horas` · `Folga` · `Falta Justificada` · `Não Trabalha (Manhã)` · `Não Trabalha (Tarde)`
- **Observações** — `observa_es_1` · short_text

### Atualizar Chave Pix

- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector
- **Chave Pix** ⭐ — `chave_pix` · radio_horizontal
  - opções: `E-mail` · `Telefone` · `CPF` · `Aleatória`
  - ajuda: Atualize a Chave Pix do Colaborador para Recebimento de Verbas Rescisórias

### Atualização de Dados Cadastrais

- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Descreva as Informações a serem atualizadas:** ⭐ — `descreva_as_informa_es_a_serem_atualizadas` · long_text
- **Código da Obra** ⭐ — `c_digo_da_obra` · connector

### Atualização de Telefone

- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Telefone** — `telefone` · phone
- **Código da Obra** ⭐ — `c_digo_da_obra` · connector
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector

### Cancelar Solicitação de Desligamento

- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Observações:** — `observa_es` · long_text
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector
- **Cancelar Solicitação de Desligamento?** ⭐ — `cancelar_solicita_o_de_desligamento` · radio_horizontal
  - opções: `Sim` · `Não`

### Cartão Somapay 2ª Via

- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector
- **Código da Obra** ⭐ — `c_digo_da_obra` · connector
- **Observações** — `observa_es_1` · short_text
- **O valor de produção da 2ª via do cartão será descontado do colaborador ao custo de  R$ 9,90.** — `o_valor_do_cart_o_ser_descontado_da_conta_somapay_do_colaborador_custo_r_10_00` · statement

### Cartão Vale Transporte 2ª Via

- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector
- **Código da Obra** ⭐ — `c_digo_da_obra` · connector
- **Observações** — `observa_es_1` · short_text
- **O valor de produção da 2ª via do cartão será descontado do colaborador ao custo de  R$ 10,80.** — `o_valor_do_cart_o_ser_descontado_do_colaborador_ao_custo_de_r_10_80` · statement

### Contrato, Etiqueta, Termos (DP)

- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector
- **Você não possui autorização para esta solicitação** ⭐ — `voc_n_o_possui_autoriza_o_para_esta_solicita_o` · checklist_vertical

### Desligamento de Colaborador

- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Código da Obra** ⭐ — `c_digo_da_obra` · connector
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector
- **Forma de Desligamento** ⭐ — `forma_de_desligamento` · select
  - opções: `À Pedido do Colaborador` · `Encerramento de Contrato de Experiência` · `Demissão com Aviso Trabalhado` · `Demissão com Aviso Indenizado`
- **Observações:** — `observa_es` · long_text
- **Chave Pix** ⭐ — `chave_pix` · radio_horizontal
  - opções: `E-mail` · `Telefone` · `CPF` · `Aleatória`
  - ajuda: Atualize a Chave Pix do Colaborador para Recebimento de Verbas Rescisórias

### Desligamento de Prestador de Serviço

- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Data de Saída** ⭐ — `data_de_sa_da` · date
- **Observações:** — `observa_es` · long_text
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector
- **Código da Obra** ⭐ — `c_digo_da_obra` · connector

### Documentação Admissão

- **CPF** — `cpf` · cpf
- **Nome** — `nome_de_colaborador` · short_text
- **Data** — `data` · date

### Documentação Rescisão

- **CPF** — `cpf` · cpf
- **Nome** — `nome_de_colaborador` · short_text
- **Data** — `data` · date

### Encaminhamento de Atestado Médico

- **Atestado Médico, Licenças e Afastamentos** ⭐ — `atestado_m_dico_licen_as_e_afastamentos` · checklist_vertical
  - opções: `Atualizado o Registro no Sistema de Ponto`
- **Código da Obra** ⭐ — `c_digo_da_obra` · connector
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector
- **Data de Início** ⭐ — `data_de_in_cio` · date
- **Data de Término** ⭐ — `data_de_t_rmino` · date
- **Anexo Atestado ou Declaração** ⭐ — `anexo_atestado` · attachment
- **Código CID ou Descrição da Doença** ⭐ — `c_digo_cid_ou_descri_o_da_doen_a` · short_text
- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Cadastro Colaboradores Conexão** ⭐ — `cadastro_colaboradores_conex_o` · connector

### Foto Relógio de Ponto

- **CTPS (Página com a Foto do Colaborador)** ⭐ — `ctps_p_gina_com_a_foto_do_colaborador` · attachment
- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Código da Obra** ⭐ — `c_digo_da_obra` · connector
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector

### Gerar Crachá (DP)

- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector
- **Você não possui autorização para esta solicitação** ⭐ — `voc_n_o_possui_autoriza_o_para_esta_solicita_o` · checklist_vertical

### Lançar Desconto em Folha

- **Valor da Parcela** — `valor_da_parcela_1` · currency
- **Cadastro Colaboradores Conexão** ⭐ — `cadastro_colaboradores_conex_o` · connector
- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Data** — `data` · date
- **Observações:** — `observa_es` · long_text
- **Código da Obra** ⭐ — `c_digo_da_obra` · connector
- **Valor Total do Desconto** ⭐ — `valor_tota_do_desconto` · currency
- **Quantidade de Parcelas** — `quantidade_de_parcelas` · number
- **Você não possui autorização para esta solicitação** ⭐ — `voc_n_o_possui_autoriza_o_para_esta_solicita_o` · checklist_vertical
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector

### Pedido de Afastamento ou Licença

- **Atestado Médico, Licenças e Afastamentos** ⭐ — `atestado_m_dico_licen_as_e_afastamentos` · checklist_vertical
  - opções: `Atualizado o Registro no Sistema de Ponto`
- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Código da Obra** ⭐ — `c_digo_da_obra` · connector
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector
- **Selecione o Tipo de Licença/Afastamento** ⭐ — `selecione_o_tipo_de_licen_a_afastamento` · select
  - opções: `Licença Paternidade (5 dias úteis)` · `Licença Maternidade (120 dias)` · `Licença Casamento (3 dias)` · `Licença Falecimento (2 dias úteis)` · `Afastamento por Doença` · `Afastamento por Acidente de Trabalho`
- **Anexo Documentação** ⭐ — `anexo_documenta_o` · attachment
- **Cadastro Colaboradores Conexão** ⭐ — `cadastro_colaboradores_conex_o` · connector

### Planilhas de Cadastro (DP)

- **Você não possui autorização para esta solicitação** ⭐ — `voc_n_o_possui_autoriza_o_para_esta_solicita_o` · checklist_vertical
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector

### Pré-Cadastro de Colaborador

- **Data de Início** ⭐ — `data_de_in_cio` · date
- **Cargo Pleiteado** ⭐ — `cargo_pleiteado` · connector
- **Telefone** — `telefone` · phone
- **Requisição Solicitada por um Terceiro?** ⭐ — `requisi_o_solicitada_por_um_terceiro` · radio_horizontal
  - opções: `Sim` · `Não`
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector
- **Anexo Documentação** ⭐ — `anexo_documenta_o` · attachment
- **Observações:** — `observa_es` · long_text
- **Documentos anexados** ⭐ — `documentos_anexados` · checklist_vertical
  - opções: `RG` · `CPF` · `Comprovante de Endereço` · `Foto`
- **Código da Obra** ⭐ — `c_digo_da_obra` · connector
- **Nome do Colaborador** ⭐ — `nome_do_colaborador` · short_text
- **CPF do Colaborador** ⭐ — `cpf_do_colaborador` · cpf

### Readmissão de Colaborador

- **Código da Obra** ⭐ — `c_digo_da_obra` · connector
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector
- **Data de Início** ⭐ — `data_de_in_cio` · date
- **Tipo de Readmissão** ⭐ — `tipo_de_readmiss_o1` · radio_horizontal
  - opções: `Prestador de Serviço` · `CTPS`
- **CPF Colaborador Desligado (somente números)** ⭐ — `cpf_colaborador_desligado_somente_n_meros` · connector
- **Cargo Pleiteado** ⭐ — `cargo_pleiteado` · connector
- **Observações:** — `observa_es` · long_text
- **CNPJ para Admissão** ⭐ — `cnpj_para_admiss_o` · select
  - opções: `00.079.526/0001-09 - MATRIZ/CE` · `50.422.792/0001-09 - INFRADENDE` · `20.198.694/0001-20 - C3`

### Relatório de DCs

- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector
- **Data** — `data` · date

### Solicitação de Ajuda de Custo

- **Solicitação** ⭐ — `solicita_o` · radio_horizontal
  - opções: `Solicitar ou Atualizar` · `Cancelar`
- **Obra** ⭐ — `obra` · connector
- **Categoria Auxílio Alimentação** — `categoria_aux_lio_alimenta_o` · select
  - opções: `Segunda à Quinta` · `Segunda à Sexta` · `Mensal` · `Mês`
- **Categoria Auxílio Transporte** — `categoria_aux_lio_transporte` · select
  - opções: `Diário` · `Mensal` · `Segunda e Sexta` · `Vale Transporte` · `Diário e Vale Transporte` · `Veículo` · `Motocicleta` · `Bicicleta`
- **Observações:** — `observa_es` · long_text
- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector

### Solicitação de Alteração de Cargo

- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Código da Obra** ⭐ — `c_digo_da_obra` · connector
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector
- **Cargo Pleiteado** ⭐ — `cargo_pleiteado` · connector
- **Justificativa de Alteração do Cargo** ⭐ — `jusitificativa_de_altera_o` · long_text
- **Data de Início** ⭐ — `data_de_in_cio` · date
- **Cargo Deferido** ⭐ — `cargo_deferido` · connector

### Solicitação de Cadastro de Dependente

- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector
- **Nome do Dependente** ⭐ — `nome_do_dependente` · short_text
- **Data de Nascimento do Dependente** ⭐ — `data_de_nascimento` · date
- **Documento de Identificação (Certidão de Nascimento ou RG)** ⭐ — `documento_de_identifica_o_certid_o_de_nascimento_ou_rg` · attachment
- **Código da Obra** ⭐ — `c_digo_da_obra` · connector
- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Parentesco** ⭐ — `parentesco` · select
  - opções: `Filho` · `Cônjuge`
- **Cadastro Colaboradores Conexão** ⭐ — `cadastro_colaboradores_conex_o` · connector

### Solicitação de Cartão Somapay 2ª Via

- **Solicitação de 2ª Via de Cartão** ⭐ — `solicita_o_de_2_via_de_cart_o` · checklist_vertical
  - opções: `Solicitação do Cartão` · `Encaminhamento do Cartão`

### Solicitação de Cartão Vale Transporte 2ª Via

- **Solicitação de 2ª Via de Cartão** ⭐ — `solicita_o_de_2_via_de_cart_o` · checklist_vertical
  - opções: `Solicitação do Cartão` · `Encaminhamento do Cartão`

### Solicitação de Crachá

- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector
- **Código da Obra** ⭐ — `c_digo_da_obra` · connector
- **Observações** — `observa_es_1` · short_text

### Solicitação de Férias

- **Data de Início** ⭐ — `data_de_in_cio` · date
- **Data de Término** ⭐ — `data_de_t_rmino` · date
- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Código da Obra** ⭐ — `c_digo_da_obra` · connector
- **Cadastro Colaboradores Conexão** ⭐ — `cadastro_colaboradores_conex_o` · connector

### Solicitação de Vale Transporte

- **Solicitação de Vale Transporte** ⭐ — `solicita_o_de_vale_transporte` · checklist_vertical
  - opções: `Cadastro no Sistema de Transporte` · `Encaminhamento do Cartão`
- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Código da Obra** ⭐ — `c_digo_da_obra` · connector
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector
- **Observações:** — `observa_es` · long_text
- **Conceder Vale Transporte?** ⭐ — `conceder_vale_transporte` · radio_horizontal
  - opções: `Sim` · `Não`

### Solicitações Diversas

- **CPF Colaborador (somente números)** ⭐ — `cpf_somente_n_meros` · connector
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector
- **Código da Obra** ⭐ — `c_digo_da_obra` · connector
- **Observações** — `observa_es_1` · short_text
- **Descreva sua Solicitação:** — `descreca_sua_solicita_o` · long_text

## Campos do formulário inicial que não dependem do tipo

(muitos só aparecem por condição de OUTRO campo — por exemplo as chaves
Pix, que aparecem conforme o tipo de chave escolhido.)

- **Tipo de Solicitação** ⭐ — `tipo_de_solicita_o` · select
  - opções: `Foto Relógio de Ponto` · `Ajuste de Ponto e Faltas` · `Encaminhamento de Atestado Médico` · `Pedido de Afastamento ou Licença` · `Solicitação de Férias` · `Desligamento de Colaborador` · `Desligamento de Prestador de Serviço` · `Admissão de Prestador de Serviço` · `Readmissão de Colaborador` · `Solicitação de Cadastro de Dependente` · `Solicitação de Vale Transporte` · `Solicitação de Ajuda de Custo` · `Solicitação de Alteração de Cargo` · `Solicitações Diversas` · `Cartão Somapay 2ª Via` · `Cartão Vale Transporte 2ª Via` · `Atualização de Dados Cadastrais` · `Atualização de Telefone` · `Atualizar Chave Pix` · `Lançar Desconto em Folha` · `Gerar Crachá (DP)` · `Planilhas de Cadastro (DP)` · `Contrato, Etiqueta, Termos (DP)` · `Cancelar Solicitação de Desligamento` · `Documentação Admissão` · `Documentação Rescisão` · `Pré-Cadastro de Colaborador` · `Relatório de DCs`
- **Parente em 1º Grau** ⭐ — `parente` · select
  - opções: `Pai ou Mãe` · `Filho` · `Avô ou Avó` · `Irmãos` · `Cônjuge`
- **Data não Trabalhada** — `dia_n_o_trabalhado` · date
- **Data de Compensação** — `data_de_compensa_o` · date
- **Data do Aviso Prévio** ⭐ — `data_do_aviso_pr_vio` · date
- **Anexo (Carta de Solicitação)** ⭐ — `anexo_carta_de_solicita_o` · attachment
- **Tipo de Lançamento** ⭐ — `tipo_de_lan_amento` · select
  - opções: `Dia Específico` · `Período Específico`
- **Data Inicial** ⭐ — `data_inicial` · date
- **Data Final** ⭐ — `data_final` · date
- **Data de Ajuste (1º Dia)** — `data_de_ajuste` · date
- **Horário de Entrada (1º Dia)** — `hor_rio_de_entrada` · time
- **Saída Almoço (1º Dia)** — `sa_da_almo_o` · time
- **Retorno Almoço (1º Dia)** — `retorno_almo_o` · time
- **Horário de Saída (1º Dia)** — `hor_rio_de_sa_da` · time
- **Data de Ajuste (2º Dia)** — `data_2_dia` · date
- **Horário de Entrada (2º Dia)** — `hor_rio_de_entrada_2_dia` · time
- **Horário de Saída (2º Dia)** — `hor_rio_de_sa_da_2_dia` · time
- **Saída Almoço (2º Dia)** — `sa_da_almo_o_2_dia` · time
- **Retorno Almoço (2º Dia)** — `retorno_almo_o_2_dia` · time
- **Data de Ajuste (3º Dia)** — `data_de_ajuste_3_dia` · date
- **Horário de Entrada (3º Dia)** — `hor_rio_de_entrada_3_dia` · time
- **Saída Almoço (3º Dia)** — `sa_da_almo_o_3_dia` · time
- **Retorno Almoço (3º Dia)** — `retorno_almo_o_3_dia` · time
- **Horário de Saída (3º Dia)** — `hor_rio_de_sa_da_3_dia` · time
- **Valor (Auxílio Alimentação)** — `valor_aux_lio_alimenta_o` · currency
- **Valor (Auxílio Transporte)** — `valor_aux_lio_transporte` · currency
- **Salário** ⭐ — `sal_rio` · currency
- **Nome do Cônjuge** ⭐ — `nome_do_c_njuge` · short_text
- **Data de Nascimento do Cônjuge** ⭐ — `data_de_nascimento_do_c_njuge` · date
- **CPF do Cônjuge** ⭐ — `cpf_do_c_njuge` · cpf
- **Anexar Certidão de Nascimento ou Declaração de Nascido Vivo e Requerimento do INSS** — `anexar_certid_o_de_nascimento_ou_declara_o_de_nascido_vivo` · statement
- **Anexar Certidão de Casamento** — `anexar_certid_o_de_casamento` · statement
- **Anexar Atestado de Óbito** — `anexar_atestado_de_bito` · statement
- **Anexar Atestado com afastamento superior a 15 dias ou Requerimento do INSS** — `atestado_com_afastamento_superior_a_15_dias_ou_requerimento_do_inss` · statement
- **Anexar CAT, Atestado superior a 15 dias e Laudo Médico (opcional)** — `anexar_cat_atestado_superior_a_15_dias_e_laudo_m_dico_opcional` · statement
- **Anexar Atestado de Óbito e Documento de Identificação do Pai ou Mãe para comprovar o parentesco** — `anexo_atestado_de_bito_e_documento_de_identifica_o_do_pai_ou_m_e_para_comprovar_o_parentesco` · statement
- **Nome para Readmissão** ⭐ — `nome_para_readmiss_o` · short_text
- **Data de Ajuste** ⭐ — `data_da_altera_o` · date
- **CPF para Readmissão** ⭐ — `cpf_para_readmiss_o` · cpf
- **Horário de Entrada** — `hor_rio_de_entrada_1` · time
- **Saída Almoço** — `sa_da_almo_o_1` · time
- **Retorno Almoço** — `retorno_almo_o_1` · time
- **Horário de Saída** — `hor_rio_de_sa_da_1` · time
- **Nº SDP** — `field` · id
- **Chave Pix Email** ⭐ — `chave_pix_email` · email
- **Chave Pix Telefone** ⭐ — `chave_pix_telefone` · phone
- **Chave Pix CPF** ⭐ — `chave_pix_cpf` · cpf
- **Chave Pix Aleatória** ⭐ — `chave_pix_aleat_ria` · short_text
- **Requerente** ⭐ — `requerente` · connector
- **Seleção de etiqueta título** — `sele_o_de_etiqueta_t_tulo` · label_select
- **Criar Novo Card Via Automação** — `criar_novo_card_via_automa_o` · radio_horizontal
  - opções: `Sim`
- **Colaborador 1 - Ausência** — `colaborador_1` · long_text
- **Colaborador 1 - Ajuste** ⭐ — `ajuste_colaborador_1` · radio_horizontal
  - opções: `Inserir Falta` · `Ajustar Ponto` · `Ignorar`

## As fases do pipe de solicitações

### Caixa de entrada — 3 campo(s)

- **ID** — `id` · id
- **Cadastro Colaboradores Conexão** ⭐ — `cadastro_colaboradores_conex_o` · connector
- **Valor da Parcela** — `valor_da_parcela_1` · currency

### Atestado Médico — 0 campo(s)


### BH/Compensação/Folga — 0 campo(s)


### Ajuste de Pontos e Faltas — 0 campo(s)


### Aguardando Documentação — 0 campo(s)


### Sob Análise — 0 campo(s)


### Fazendo — 6 campo(s)

- **A solicitação foi atendida?** ⭐ — `a_solicita_o_foi_atendida` · radio_horizontal
  - opções: `Sim` · `Não`
- **Por quê?** ⭐ — `porque` · short_text
- **Ajuste de Ponto** ⭐ — `checklist` · checklist_vertical
  - opções: `Atualizado Registro no Sistema de Ponto`
- **Atestado Médico, Licenças e Afastamentos** ⭐ — `atestado_m_dico_licen_as_e_afastamentos` · checklist_vertical
  - opções: `Atualizado o Registro no Sistema de Ponto`
- **Solicitação de Vale Transporte** ⭐ — `solicita_o_de_vale_transporte` · checklist_vertical
  - opções: `Cadastro no Sistema de Transporte` · `Encaminhamento do Cartão`
- **Solicitação de 2ª Via de Cartão** ⭐ — `solicita_o_de_2_via_de_cart_o` · checklist_vertical
  - opções: `Solicitação do Cartão` · `Encaminhamento do Cartão`

### Concluído — 0 campo(s)


### MobPonto — 0 campo(s)


### Cancelar — 0 campo(s)


---

# Pipe CADASTRO COLABORADORES

## Formulário inicial — o que a OBRA preenche

- **Tipo de Documentação para Cadastro** ⭐ — `tipo_de_documenta_o_para_cadastro` · radio_horizontal
  - opções: `Prestador de Serviço` · `CTPS`
- **Empresa para Registro** ⭐ — `empresa_para_registro` · select
  - opções: `00.079.526/0001-09 - BWS` · `38.240.209/0001-03 - LC` · `04.895.340/0001-89 - RVV`
- **CNPJ para Admissão** ⭐ — `cnpj_para_admiss_o` · select
  - opções: `00.079.526/0001-09 - MATRIZ/CE` · `50.422.792/0001-09 - INFRADENDE` · `20.198.694/0001-20 - C3`
- **Tipo de CTPS** ⭐ — `tipo_de_ctps` · radio_horizontal
  - opções: `Física` · `Digital`
- **Este é o Primeiro Emprego do Colaborador?** ⭐ — `primeiro_emprego` · radio_horizontal
  - opções: `Sim` · `Não`
- **Nome Completo** ⭐ — `nome_completo` · short_text
- **Data de Nascimento** ⭐ — `data_de_nascimento` · date
- **Sexo** ⭐ — `sexo` · select
  - opções: `Masculino` · `Feminino`
- **Naturalidade** ⭐ — `naturalidade` · connector
- **UF (Naturalidade)** ⭐ — `select_t_tulo` · select
  - opções: `CE` · `AL` · `BA` · `MA` · `PB` · `PE` · `PI` · `RN` · `SE` · `AC` · `AM` · `AP` · `DF` · `ES` · `GO` · `MG` · `MS` · `MT` · `PA` · `PR` · `RJ` · `RO` · `RR` · `RS` · `SC` · `SP` · `TO`
- **Escolaridade** ⭐ — `escolaridade` · select
  - opções: `Não Alfabetizado` · `Educação Infantil` · `Ensino Fundamental` · `Ensino Médio` · `Ensino Superior` · `Pós-graduação` · `Mestrado` · `Doutorado`
- **Estado Civil** ⭐ — `estado_civil` · select
  - opções: `Solteiro (a)` · `Casado (a)` · `Divorciado (a)` · `Viúvo (a)` · `União Estável`
- **Nº Identidade** ⭐ — `n_identidade` · number
- **CPF** ⭐ — `cpf` · cpf
- **Endereço (Logradouro)** ⭐ — `endere_o_rua` · short_text
- **Número** ⭐ — `n_mero2` · number
- **Bairro** ⭐ — `bairro` · short_text
- **Município** ⭐ — `munic_pio` · connector
- **UF (Endereço)** ⭐ — `uf_endere_o` · select
  - opções: `CE` · `AL` · `BA` · `MA` · `PB` · `PE` · `PI` · `RN` · `SE` · `AC` · `AM` · `AP` · `DF` · `ES` · `GO` · `MG` · `MS` · `MT` · `PA` · `PR` · `RJ` · `RO` · `RR` · `RS` · `SC` · `SP` · `TO`
- **CEP** ⭐ — `cep` · short_text
- **Telefone** ⭐ — `phone_t_tulo` · phone
- **Chave Pix** ⭐ — `chave_pix` · radio_horizontal
  - opções: `E-mail` · `Telefone` · `CPF` · `CNPJ` · `Aleatória`
- **Chave Pix Telefone** ⭐ — `chave_pix_telefone` · phone
- **Chave Pix Email** ⭐ — `chave_pix_email` · email
- **Chave Pix CPF** ⭐ — `chave_pix_cpf` · cpf
- **Chave Pix CNPJ** ⭐ — `chave_pix_cnpj` · cnpj
- **Chave Pix Aleatória** ⭐ — `chave_pix_aleat_ria` · short_text
- **Código da Obra** ⭐ — `c_digo_da_obra` · connector
- **Função** ⭐ — `fun_o` · connector
- **Alguma observação em relação à função?** — `obser` · short_text
- **Pagamento** ⭐ — `pagamento` · radio_horizontal
  - opções: `Diarista` · `Mensalista`
- **Valor da Diária** ⭐ — `valor_da_di_ria` · currency
- **Valor do Mês** ⭐ — `valor_do_m_s` · currency
- **Data de Início** ⭐ — `data_de_in_cio` · date
- **Nº CTPS** ⭐ — `ctps` · number
- **Nº PIS** ⭐ — `pis` · number
- **Documento de Identificação (RG, CNH ou CTPS)** ⭐ — `anexo_ctps_rg_cpf_comprovante_de_endere_o` · attachment
- **CTPS (Página com a Foto do Colaborador)** ⭐ — `carteira_de_trabalho` · attachment
- **CTPS (Página com as Informações do Colaborador)** ⭐ — `ctps_p_gina_com_as_informa_es_do_colaborador` · attachment
- **CPF (Cadastro de Pessoa Física)** ⭐ — `attachment_t_tulo` · attachment
- **Comprovante de Endereço ou Declaração** ⭐ — `ctps_1` · attachment
- **Foto do Colaborador** ⭐ — `foto` · attachment
- **Solicitar Salário Família?** ⭐ — `desejar_solicitar_sal_rio_fam_lia` · radio_horizontal
  - opções: `Sim` · `Não`
  - ajuda: Cadastrar dependentes?
- **Anexo (Certidão de Nascimento, RG dos Dependentes ou Certidão de Casamento)** — `certid_o_de_nascimento_dos_dependentes` · attachment
- **Meio de Transporte para chegar à Obra?** ⭐ — `utiliza_meio_de_transporte_para_obra` · select
  - opções: `Nenhum` · `Transporte Coletivo` · `Moto` · `Bicicleta`
- **Documentos Diversos** — `documentos_diversos` · attachment
- **Requisição Solicitada por um Terceiro?** ⭐ — `requisi_o_solicitada_por_um_terceiro` · radio_horizontal
  - opções: `Sim` · `Não`
- **Requerente** ⭐ — `requerente` · connector
- **Responsável pela Solicitação** ⭐ — `respons_vel_pela_solicita_o` · connector
- **.** — `n_registro` · id
- **Etiqueta** — `etiqueta` · label_select

## As fases — o que o DP preenche

### Cadastro Inicial — 44 campo(s)

- **Statement-18b53671-508a-4948-9797-49283ec318dc** — `statement_18b53671_508a_4948_9797_49283ec318dc` · statement
  - ajuda: <p><a href="https://hook.us1.make.com/7yfgj9nxtg1pi2ktrvkywt7x74txfl81?cardid=853990032&amp;acao=cancelarreadmissao" target="_blank" rel="noopener"><strong>Cancelar Readmissão</strong></a></p>
- **Preencher Código da Obra** ⭐ — `preencher_c_digo_da_obra` · checklist_vertical
- **Checklist Readmissão** ⭐ — `checklist_readmiss_o` · checklist_horizontal
  - opções: `Atualizar Data de Admissão` · `Confirmar se campo "Função"="Cargo"` · `Confirmar campo "Código da Obra"` · `Confirmar campo "Data de Início"` · `Atualizar campo "Matrícula"` · `Atualizar campo "Tipo de Contrato"`
- **Data de Admissão** ⭐ — `data_de_admiss_o` · date
- **Tipo de Contrato** ⭐ — `tipo_de_contrato` · select
  - opções: `CLT (tempo determinado)` · `CLT (tempo Indeterminado)` · `PJ` · `Autônomo (RPA)` · `Autônomo Mensalista` · `Jovem aprendiz` · `Estágio` · `Temporário` · `Pró-labore`
- **Razão Social** — `raz_o_social` · short_text
- **CNPJ** — `cnpj` · cnpj
- **Matrícula** ⭐ — `matr_cula` · short_text
- **Nome da Mãe** ⭐ — `nome_da_m_e` · short_text
- **Nome do Pai** ⭐ — `nome_do_pai` · short_text
- **Órgão Emissor (Documento de Identificação)** ⭐ — `rg_o_emissor_documento_de_identifica_o` · select
  - opções: `SSP` · `DETRAN` · `MTE` · `ME`
- **Data de Emissão (Documento de Identificação)** ⭐ — `data_de_emiss_o_documento_de_identifica_o` · date
- **UF (Documento de Identificação)** ⭐ — `uf_documento_de_identifica_o` · select
  - opções: `CE` · `AL` · `BA` · `MA` · `PB` · `PE` · `PI` · `RN` · `SE` · `AC` · `AM` · `AP` · `DF` · `ES` · `GO` · `MG` · `MS` · `MT` · `PA` · `PR` · `RJ` · `RO` · `RR` · `RS` · `SC` · `SP` · `TO`
- **Série (CTPS)** ⭐ — `s_rie_ctps` · number
- **Data de Emissão (CTPS)** ⭐ — `data_de_emiss_o_ctps` · date
- **UF (CTPS)** ⭐ — `uf_ctps` · select
  - opções: `CE` · `AL` · `BA` · `MA` · `PB` · `PE` · `PI` · `RN` · `SE` · `AC` · `AM` · `AP` · `DF` · `ES` · `GO` · `MG` · `MS` · `MT` · `PA` · `PR` · `RJ` · `RO` · `RR` · `RS` · `SC` · `SP` · `TO`
- **Cargo** ⭐ — `cargo` · connector
- **Cargo [ ]** — `cargo_1` · short_text
- **Convenção** — `conven_o` · short_text
- **Jornada de Trabalho** ⭐ — `jornada_de_trabalho` · select
  - opções: `Entr.: 07:00h / Interv. 12:00h às 13:00h / Saída: 17:00h` · `Entr.: 18:00h / Saída 06:00h / Escala 12x36`
- **Contrato de Experiência** ⭐ — `contrato_de_experi_ncia` · select
  - opções: `45 dias, prorrogável por mais 45 dias` · `30 dias, prorrogável por mais 60 dias` · `60 dias, prorrogável por mais 30 dias` · `30 dias, prorrogável por mais 30 dias` · `Sem Contrato de Experiência`
- **Salário Extenso** — `sal_rio_extenso` · short_text
- **Salário** — `sal_rio` · currency
- **Objeto Obra [ ]** ⭐ — `objeto_obra` · long_text
- **Código CBO** — `c_digo_cbo` · short_text
- **Nome do Cônjuge** — `c_njuge` · short_text
- **Data de Nascimento do Cônjuge** — `data_de_nascimento_do_c_njuge` · date
- **CPF do Cônjuge** — `cpf_do_c_njuge` · cpf
- **Quantidade de Dependentes** — `quantidade_de_dependentes` · select
  - opções: `Filho 1` · `Filho 2` · `Filho 3` · `Filho 4` · `Filho 5`
- **Nome Filho 1** ⭐ — `nome_filho_1` · short_text
- **Data de Nascimento Filho 1** ⭐ — `data_de_nascimento_filho_1` · date
- **Nome Filho 2** ⭐ — `nome_filho_2` · short_text
- **Data de Nascimento Filho 2** ⭐ — `data_de_nascimento_filho_2` · date
- **Nome Filho 3** ⭐ — `filho_3` · short_text
- **Data de Nascimento Filho 3** ⭐ — `data_de_nascimento_filho_3` · date
- **Nome Filho 4** ⭐ — `filho_4` · short_text
- **Data de Nascimento Filho 4** ⭐ — `data_de_nascimento_filho_4` · short_text
- **Nome Filho 5** ⭐ — `nome_filho_5` · short_text
- **Data de Nascimento Filho 5** ⭐ — `data_de_nascimento_filho_5` · date
- **Nome Completo (corrigido)** — `nome_completo_corrigido` · short_text
- **ID do Card** — `id_do_card` · id
- **Table Record ID** — `table_record_id` · number
- **Table Record ID Desligado** — `table_record_id_desligado` · number
- **Cancelar Registro** — `cancelar_registro` · checklist_vertical
  - opções: `Cancelar` · `Cancelar Readmissão`

### Cadastros Adicionais — 8 campo(s)

- **ID** — `id_2` · id
- **Statement-9a6c90ec-4534-45ea-a233-472f3e979b8b** — `statement_9a6c90ec_4534_45ea_a233_472f3e979b8b` · statement
  - ajuda: <p><strong><a href="https://hook.us1.make.com/7yfgj9nxtg1pi2ktrvkywt7x74txfl81?cardid={{card.id}}&amp;acao=atualizarcadastro&amp;cpf={{phase309852942.field321873368}}" target="_blank" rel="noopener">Atualizar Cadastro</a></strong></p>
- **Procedimentos Adicionais CTPS** — `cadastro_adicionais` · checklist_horizontal
  - opções: `Cadastro Somapay` · `Cadastro Mobponto` · `Email Contabilidade` · `Impressões (CT, Reg., EPI, Vale, Crachá)` · `Colagem Etiqueta` · `Malote Físico/Digital` · `Criar Pasta Física` · `Malote de Retorno` · `Arquivar Documentos`
- **Procedimentos Adicionais Prestador de Serviço** — `procedimentos_adicionais_prestador_de_servi_o` · checklist_horizontal
  - opções: `Cadastro Somapay` · `Cadasto Mobponto`
- **Readmissão de Colaborador** ⭐ — `readmiss_o_de_colaborador` · checklist_horizontal
  - opções: `Apagar Data de Demissão do Mobponto`
- **Concluir Checklist CTPS** ⭐ — `concluir_checklist_1` · checklist_vertical
- **Concluir Checklist Prestador de Serviço** ⭐ — `concluir_checklist_prestador_de_servi_o_1` · checklist_vertical
- **ASO** — `aso` · attachment

### Cadastro Concluído — 18 campo(s)

- **Statement-40f85a49-9274-46ce-b3c4-ee0dfa08fac5** — `statement_40f85a49_9274_46ce_b3c4_ee0dfa08fac5` · statement
  - ajuda: <p><strong><a href="https://hook.us1.make.com/7yfgj9nxtg1pi2ktrvkywt7x74txfl81?cardid={{card.id}}&amp;acao=atualizarcadastro&amp;cpf={{phase309852942.field321873368}}" target="_blank" rel="noopener">Atualizar Cadastro</a></strong></p>
- **ID** — `id` · id
- **Cesta Básica Extra** — `cesta_b_sica_extra` · checklist_vertical
  - opções: `Sim` · `Não`
- **Recebe Gratificação Mensal** — `recebe_gratifica_o` · checklist_vertical
  - opções: `Sim`
- **Valor da Gratificação** — `valor_da_gratifica_o` · currency
- **Recebe Parcela Única** — `recebe_parcela_nica` · checklist_vertical
  - opções: `Sim`
- **Recebe via SP** — `recebe_via_sp` · checklist_vertical
  - opções: `Sim`
- **Diversos** — `diversos` · checklist_vertical
  - opções: `Adicional de Transferência` · `Vale Refeição`
- **Statement-3fb33d08-3ba9-47cd-abca-d02688083e1c** — `statement_3fb33d08_3ba9_47cd_abca_d02688083e1c` · statement
  - ajuda: <hr />
<p><strong> Ajuda de Custo </strong></p>
- **Obra** — `obra` · connector
- **Categoria Auxílio Alimentação** — `categoria_aux_lio_alimenta_o` · select
  - opções: `Segunda à Quinta` · `Segunda à Sexta` · `Mensal` · `Mês`
- **Valor (Auxílio Alimentação)** — `valor_aux_lio_alimenta_o` · currency
- **Categoria Auxílio Transporte** — `categoria_aux_lio_transporte` · select
  - opções: `Diário` · `Mensal` · `Segunda e Sexta` · `Vale Transporte` · `Diário e Vale Transporte` · `Veículo` · `Motocicleta` · `Bicicleta` · `Cartão`
- **Valor (Auxílio Transporte)** — `valor_aux_lio_transporte` · currency
- **Observação (Ajuda de Custo)** — `observa_o` · long_text
- **Statement-03a63648-e3fd-499d-a88a-ec603e0aa40f** — `statement_03a63648_e3fd_499d_a88a_ec603e0aa40f` · statement
  - ajuda: <hr />
<p> </p>
- **Statement-c5e624df-a7f3-44ee-8db8-189e922dcbd8** — `statement_c5e624df_a7f3_44ee_8db8_189e922dcbd8` · statement
  - ajuda: <p><strong>Responsabilidade de Obras</strong></p>
- **Obra (C. Diários)** — `obra_c_di_rios` · connector

### Solicitações DP — 19 campo(s)

- **Statement-f0a16470-2799-4327-a4a4-465d95feadfd** — `statement_f0a16470_2799_4327_a4a4_465d95feadfd` · statement
  - ajuda: <p><strong><a href="https://hook.us1.make.com/7yfgj9nxtg1pi2ktrvkywt7x74txfl81?cardid={{card.id}}&amp;acao=atualizarcadastro&amp;cpf={{phase309852942.field321873368}}" target="_blank" rel="noopener">Atualizar Cadastro</a></strong></p>
- **Statement-f6a0d486-d89a-4684-a70b-efabddbc0c12** — `statement_f6a0d486_d89a_4684_a70b_efabddbc0c12` · statement
  - ajuda: <p><span style="color: #ff4e4e;"><strong><a style="color: #ff4e4e;" href="https://hook.us1.make.com/7yfgj9nxtg1pi2ktrvkywt7x74txfl81?cardid={{card.id}}&amp;acao=atualizadatasrescisao&amp;datadoaviso={{phase309968339.field322730146}}&amp;datadesaida={{phase309968339.field322730150}}&amp;ultimodia={{phase309968339.field341457274}}" target="_blank" rel="noopener">Atualiza Datas de Rescisão</a></strong></span></p>
- **Statement-d4680fd4-011f-4bdf-ba05-622845aa7946** — `statement_d4680fd4_011f_4bdf_ba05_622845aa7946` · statement
  - ajuda: <p><span style="color: #ff4e4e;"><strong><a style="color: #ff4e4e;" href="https://hook.us1.make.com/7yfgj9nxtg1pi2ktrvkywt7x74txfl81?acao=desligamobponto&amp;cardid={{card.id}}&amp;datadoaviso={{phase309968339.field322730146}}&amp;datadesaida={{phase309968339.field322730150}}&amp;ultimodia={{phase309968339.field341457274}}&amp;cpf={{phase309852942.field321873368}}&amp;tipodesolicitacao={{phase309968339.field322732917}}" target="_blank" rel="noopener">Inativação Mobponto</a></strong></span></p>
- **Tipo de Solicitação** — `tipo_de_desligamento` · select
  - opções: `Desligamento de Colaborador` · `Desligamento de Prestador de Serviço` · `Readmissão de Colaborador` · `Admissão de Prestador de Serviço` · `Novo Cadastro`
- **Descreva as Informações a serem atualizadas** — `descreva_as_informa_es_a_serem_atualizadas` · long_text
- **Forma de Desligamento** — `forma_de_desligamento` · select
  - opções: `À Pedido do Colaborador` · `Encerramento de Contrato de Experiência` · `Demissão com Aviso Trabalhado` · `Demissão com Aviso Indenizado`
- **Data do Aviso Prévio** — `data_do_aviso_pr_vio` · date
- **Último dia Trabalhado** — `ltimo_dia_trabalhado` · date
- **Data de Saída** — `data_de_sa_da` · date
- **Quantidade de Faltas à Descontar** — `quantidade_de_faltas_descontar` · number
- **Observações:** — `observa_es` · long_text
- **Checklist Desligamento de Prestador de Serviço** — `checklist_desligamento_de_prestador_de_servi_o` · checklist_vertical
  - opções: `Desativação no Sistema de Ponto` · `Desativação no Somapay`
- **Código da Obra Readmissão** — `c_digo_da_obra_readmiss_o` · connector
- **Checklist Desligamento de Colaborador CTPS** — `checklist_desligamento_de_colaborador_ctps` · checklist_horizontal
  - opções: `Solic. de Aviso Prévio ou Encer. de CT` · `Lançamento de Faltas` · `Recebimento de Documentação Rescisória` · `Lançar DC do TRCT` · `Lançar SP da GRRF` · `Impressão de TRCT` · `Impressão de Folha de Ponto` · `Malote Físico/Digital` · `Pagamento do TRCT` · `Desativação Somapay (pós-pagamento)` · `Desativação no Sistema de Ponto`
- **Procedimentos Adicionais CTPS** — `procedimentos_adicionais_ctps` · checklist_vertical
- **Procedimentos Adicionais Prestador de Serviço** — `procedimentos_adicionais_prestador_de_servi_o_1` · checklist_vertical
- **Registros de Solicitações DP (Atualização Automática)** — `registros_de_solicita_es` · long_text
- **Concluir Checklist Prestador de Serviço** ⭐ — `concluir_checklist_prestador_de_servi_o` · checklist_vertical
- **Concluir Checklist Desligamento de Colaborador CTPS** ⭐ — `concluir_checklist_desligamento_de_colaborador` · checklist_vertical

### Colaboradores Desligados — 3 campo(s)

- **ID** — `id_1` · id
- **Statement-1ae08cb7-d521-4743-81ff-2863a2b97a2d** — `statement_1ae08cb7_d521_4743_81ff_2863a2b97a2d` · statement
  - ajuda: <p><span style="color: #ff4e4e;"><strong><a style="color: #ff4e4e;" href="https://hook.us1.make.com/7yfgj9nxtg1pi2ktrvkywt7x74txfl81?cardid={{card.id}}&amp;acao=atualizadatasrescisao&amp;datadoaviso={{phase309968339.field322730146}}&amp;datadesaida={{phase309968339.field322730150}}&amp;ultimodia={{phase309968339.field341457274}}" target="_blank" rel="noopener">Atualiza Datas de Rescisão</a></strong></span></p>
- **Statement-49d61b0d-dd3f-447c-930f-7127e3f9461c** — `statement_49d61b0d_dd3f_447c_930f_7127e3f9461c` · statement
  - ajuda: <p><strong><a href="https://hook.us1.make.com/7yfgj9nxtg1pi2ktrvkywt7x74txfl81?cardid={{card.id}}&amp;acao=atualizarcadastro&amp;cpf={{phase309852942.field321873368}}" target="_blank" rel="noopener" data-mce-fragment="1">Atualizar Cadastro</a></strong><br /><strong> </strong></p>

### Cadastro Cancelado — 0 campo(s)


### Colaboradores Afastados — 2 campo(s)

- **Statement-565a87b9-d339-4488-9fc8-c67e5f3ddb7b** — `statement_565a87b9_d339_4488_9fc8_c67e5f3ddb7b` · statement
  - ajuda: <p><strong><a href="https://hook.us1.make.com/7yfgj9nxtg1pi2ktrvkywt7x74txfl81?cardid={{card.id}}&amp;acao=atualizarcadastro&amp;cpf={{phase309852942.field321873368}}" target="_blank" rel="noopener">Atualizar Cadastro</a></strong></p>
- **Statement-ebffb10e-f0a3-4614-af29-82032f13102b** — `statement_ebffb10e_f0a3_4614_af29_82032f13102b` · statement
  - ajuda: <p><strong><a href="https://hook.us1.make.com/7yfgj9nxtg1pi2ktrvkywt7x74txfl81?cardid={{card.id}}&amp;acao=atualizarcadastro&amp;cpf={{phase309852942.field321873368}}" target="_blank" rel="noopener">Atualizar Cadastro</a></strong></p>

