-- ============================================================================
-- 086 — O CADASTRO DA PESSOA FICA COMPLETO
--
-- Pedido do dono, 04 e 05/11/2026: *"é importante a gente colocar logo o
-- cadastro das pessoas dentro do ERP e começar a trabalhar nele (…) no pipe a
-- gente tem toda a documentação das pessoas, e eu preciso que isso seja
-- internalizado dentro do ERP também."*
--
-- DE ONDE VEM CADA COLUNA, e isto não foi adivinhado: o cenário do Make
-- "DP - Cadastro de Colaboradores - Botão" escreve a LINHA INTEIRA da aba
-- "Dados Documentos" (74 colunas) a partir dos campos do pipe. Esse mapeamento
-- é o contrato que a folha de pagamento lê hoje, e está transcrito em
-- `referencia_pipefy/`. As colunas abaixo são ele, campo por campo.
--
-- O QUE NÃO ENTRA, e por quê:
--   · as 6 gambiarras de chave Pix (um campo por tipo) — o ERP já tem
--     `pix_tipo` + `pix_chave`, que é o desenho certo;
--   · os 11 campos de dependente com teto de 5 filhos — vão para a tabela
--     própria, na 087;
--   · "Salário Extenso" — o ERP gera do valor (a planilha tem erro de
--     digitação: "mil quinhetoss e trinta e oito reais");
--   · "Cargo [ ]", "Nome Completo (corrigido)", "Table Record ID" e os outros
--     identificadores técnicos — existiam para contornar o Pipefy;
--   · "Jornada de Trabalho" como texto de duas opções — o ponto do ERP tem
--     escala de verdade, e é ela que manda.
--
-- SIGILO. Este é o momento em que endereço, RG, PIS e filiação de ~575 pessoas
-- entram num sistema com tela na internet. Nada aqui é exibido por padrão: a
-- tela de Colaboradores mostra o cadastro completo só a quem tem
-- `editar_colaboradores` (hoje: ADMIN, diretor financeiro e DP). Quem só tem
-- `ver_pessoal` continua vendo nome, obra, função e situação — o que já via.
-- ============================================================================

ALTER TABLE colaboradores
    -- ---- identificação
    ADD COLUMN IF NOT EXISTS data_nascimento     DATE,
    ADD COLUMN IF NOT EXISTS sexo                TEXT,
    ADD COLUMN IF NOT EXISTS nome_mae            TEXT,
    ADD COLUMN IF NOT EXISTS nome_pai            TEXT,
    ADD COLUMN IF NOT EXISTS estado_civil        TEXT,
    ADD COLUMN IF NOT EXISTS escolaridade        TEXT,
    -- A naturalidade é município + UF. Fica como TEXTO e não como chave para
    -- uma tabela de municípios: o ERP não tem a lista do IBGE, e criar uma
    -- tabela de 5.570 linhas para preencher um campo de cadastro é custo sem
    -- retorno. A tela oferece o que já foi digitado antes (autocompletar), que
    -- é o que evita "Fortaleza", "fortaleza" e "FORTALEZA" na mesma base.
    ADD COLUMN IF NOT EXISTS naturalidade        TEXT,
    ADD COLUMN IF NOT EXISTS naturalidade_uf     TEXT,
    -- ---- documentos (o número; o PAPEL vai para o Arquivo, com validade)
    ADD COLUMN IF NOT EXISTS rg                  TEXT,
    ADD COLUMN IF NOT EXISTS rg_orgao            TEXT,
    ADD COLUMN IF NOT EXISTS rg_uf               TEXT,
    ADD COLUMN IF NOT EXISTS rg_emissao          DATE,
    ADD COLUMN IF NOT EXISTS pis                 TEXT,
    ADD COLUMN IF NOT EXISTS ctps_numero         TEXT,
    ADD COLUMN IF NOT EXISTS ctps_serie          TEXT,
    ADD COLUMN IF NOT EXISTS ctps_uf             TEXT,
    ADD COLUMN IF NOT EXISTS ctps_emissao        DATE,
    ADD COLUMN IF NOT EXISTS ctps_digital        BOOLEAN,
    ADD COLUMN IF NOT EXISTS primeiro_emprego    BOOLEAN,
    -- ---- endereço
    ADD COLUMN IF NOT EXISTS cep                 TEXT,
    ADD COLUMN IF NOT EXISTS logradouro          TEXT,
    ADD COLUMN IF NOT EXISTS numero_endereco     TEXT,
    ADD COLUMN IF NOT EXISTS complemento         TEXT,
    ADD COLUMN IF NOT EXISTS bairro              TEXT,
    ADD COLUMN IF NOT EXISTS municipio           TEXT,
    ADD COLUMN IF NOT EXISTS uf                  TEXT,
    ADD COLUMN IF NOT EXISTS email               TEXT,
    -- ---- vínculo
    -- "Tipo de Documentação para Cadastro" no pipe: CTPS ou Prestador de
    -- Serviço. Decisão do dono em 04/11: *"colaborador e prestador tudo é no
    -- mesmo local, ou seja, todo mundo é colaborador"* — logo é um CAMPO, não
    -- dois cadastros. E em 05/11: *"vamos manter assim o CTPS e o prestador de
    -- serviço. E o tipo de contrato"* — os dois ficam, porque respondem coisas
    -- diferentes: o vínculo diz se há carteira; o contrato, qual é o regime.
    ADD COLUMN IF NOT EXISTS vinculo             TEXT,
    ADD COLUMN IF NOT EXISTS tipo_contrato       TEXT,
    ADD COLUMN IF NOT EXISTS empresa_id          BIGINT REFERENCES empresas(id),
    -- O CNPJ de admissão é OUTRA coisa que a empresa do grupo: o pipe tem as
    -- duas listas, com valores diferentes (a matriz, a INFRADENDE e a C3). É o
    -- CNPJ que assina a carteira.
    ADD COLUMN IF NOT EXISTS cnpj_admissao       TEXT,
    ADD COLUMN IF NOT EXISTS razao_social_pj     TEXT,
    ADD COLUMN IF NOT EXISTS cnpj_pj             TEXT,
    ADD COLUMN IF NOT EXISTS contrato_experiencia TEXT,
    ADD COLUMN IF NOT EXISTS data_inicio         DATE,
    ADD COLUMN IF NOT EXISTS salario             NUMERIC(14,2),
    ADD COLUMN IF NOT EXISTS mensalista          BOOLEAN,
    ADD COLUMN IF NOT EXISTS valor_mes           NUMERIC(14,2),
    -- ---- benefícios: a CATEGORIA é tão importante quanto o valor
    -- "Cartão" na categoria de transporte significa que NÃO se paga em
    -- dinheiro, e era a origem de pagamento em duplicidade.
    ADD COLUMN IF NOT EXISTS categoria_alimentacao TEXT,
    ADD COLUMN IF NOT EXISTS categoria_transporte  TEXT,
    ADD COLUMN IF NOT EXISTS meio_transporte       TEXT,
    ADD COLUMN IF NOT EXISTS recebe_gratificacao   BOOLEAN,
    ADD COLUMN IF NOT EXISTS valor_gratificacao    NUMERIC(14,2),
    ADD COLUMN IF NOT EXISTS parcela_unica         BOOLEAN,
    ADD COLUMN IF NOT EXISTS recebe_via_sp         BOOLEAN,
    ADD COLUMN IF NOT EXISTS cesta_basica_extra    BOOLEAN,
    ADD COLUMN IF NOT EXISTS vale_refeicao         BOOLEAN,
    ADD COLUMN IF NOT EXISTS adicional_transferencia BOOLEAN,
    ADD COLUMN IF NOT EXISTS observacao_auxilio    TEXT,
    ADD COLUMN IF NOT EXISTS solicitar_salario_familia BOOLEAN,
    -- ---- quem pediu o cadastro (o pipe pergunta, e serve para cobrar)
    ADD COLUMN IF NOT EXISTS requerente          TEXT,
    ADD COLUMN IF NOT EXISTS responsavel_id      BIGINT REFERENCES usuarios(id),
    -- ---- folha
    ADD COLUMN IF NOT EXISTS id_fortes           TEXT,
    ADD COLUMN IF NOT EXISTS convencao_categoria TEXT,
    -- ---- desligamento (o pipe guarda as três datas, e a folha usa as três)
    ADD COLUMN IF NOT EXISTS aviso_previo        DATE,
    ADD COLUMN IF NOT EXISTS ultimo_dia          DATE;

-- O PIS é único quando existe: dois cadastros com o mesmo PIS é a mesma pessoa
-- duas vezes, e a folha paga duas. O índice parcial deixa o campo vazio em paz
-- (a maioria das pessoas de hoje entra sem PIS, vindo da planilha).
CREATE UNIQUE INDEX IF NOT EXISTS ux_colaboradores_pis
    ON colaboradores (regexp_replace(pis, '\D', '', 'g'))
 WHERE pis IS NOT NULL AND regexp_replace(pis, '\D', '', 'g') <> '';

-- A busca da tela é por nome, e a base vai para ~575 linhas ativas (e mantém
-- histórico): índice que ignora caixa, como o do Registro de Colaboradores.
CREATE INDEX IF NOT EXISTS ix_colaboradores_nome ON colaboradores (upper(nome));
CREATE INDEX IF NOT EXISTS ix_colaboradores_situacao ON colaboradores (situacao);
