-- ===========================================================================
-- 093 — O DIÁRIO PARA OS MODELOS DOS ÓRGÃOS
--
-- Ele mandou dois modelos em 09/11/2026: o que a BWS usa hoje e o que a
-- Prefeitura de Abreu e Lima exige em contrato. A decisão dele: *"como ele é
-- mais amplo, a gente pode tentar adaptar logo o nosso para esse aí (…) se os
-- dados que a gente estiver coletando estiverem mais amplos, vamos incorporar
-- também. Acho que quanto mais amplo, melhor."*
--
-- O QUE O MODELO DO ÓRGÃO TEM E O NOSSO NÃO TINHA:
--
--   · HORÁRIO DE TRABALHO ("7:00 às 16:00") — e ele pediu padrão por dia da
--     semana: *"o normal é 7 às 17, ou 7 às 16 na sexta-feira (…) como se
--     fosse pré-preenchido, a depender do dia da semana. E se necessário, a
--     pessoa que estiver preenchendo o diário muda."*
--   · EQUIPAMENTOS em obra ("01 Caçamba e 01 placa vibratória");
--   · HISTÓRICO DOS SERVIÇOS/OCORRÊNCIAS — um bloco à parte, acima do
--     quadro técnico, separado dos SERVIÇOS EXECUTADOS.
--
-- ⚑ POR QUE TRÊS BLOCOS DE TEXTO E NÃO UM. O modelo do órgão separa, e a
-- separação tem sentido: SERVIÇOS EXECUTADOS é o que foi feito; OCORRÊNCIAS é
-- *"situações não comuns (…) reuniões, acidentes, visitas técnicas,
-- intercorrências, sinistros"*; HISTÓRICO é o resumo do andamento. Juntar os
-- três num campo só e depois tentar separá-los na impressão seria adivinhar.
--
-- E AS LOGOS. Ele: *"dentro do cadastro da empresa (…) colocar logo da
-- empresa para ela ser colocada dentro do diário também. Tanto a logo da
-- empresa, quanto outras logos. No caso, nós temos a logo da prefeitura."*
-- A logo da EMPRESA já existia (`empresas.logo`, desde o cadastro de
-- empresas); o que faltava era a do CONTRATANTE, que é por OBRA — cada
-- contrato tem o seu órgão, e a mesma empresa atende prefeituras diferentes.
-- ===========================================================================

ALTER TABLE diarios_obra
    -- O horário praticado no dia. Texto e não hora-início/hora-fim porque o
    -- modelo do órgão imprime a FRASE ("7:00 às 16:00"), e obra com turno
    -- partido escreve "7:00 às 11:00 e 13:00 às 17:00" — que não cabe em dois
    -- campos de hora sem inventar um terceiro e um quarto.
    ADD COLUMN IF NOT EXISTS horario_trabalho TEXT,
    -- Os equipamentos em obra no dia. Texto livre: o modelo do órgão pede
    -- exatamente isso ("01 Caçamba e 01 placa vibratória"), e transformar em
    -- cadastro de equipamento seria construir um controle de frota que
    -- ninguém pediu.
    ADD COLUMN IF NOT EXISTS equipamentos TEXT,
    -- O bloco "HISTÓRICO DOS SERVIÇOS/OCORRÊNCIAS" do modelo do órgão.
    ADD COLUMN IF NOT EXISTS historico TEXT;

-- ---------------------------------------------------------------------------
-- A LOGO DO CONTRATANTE, por OBRA
--
-- Por que na OBRA e não numa tabela de órgãos: a mesma prefeitura pode ter
-- dois contratos com modelos diferentes, e um cadastro de órgão só para
-- guardar uma imagem seria tabela nova para um campo. Se um dia houver muitos
-- contratos do mesmo órgão, isso vira cadastro — e aí a migração é trivial.
--
-- Guardada como BYTEA na própria linha, igual à logo da empresa, e pelo mesmo
-- motivo: ela é pequena, é lida junto com a obra e não vale uma ida ao Drive
-- a cada folha de diário impressa.
-- ---------------------------------------------------------------------------
ALTER TABLE obras
    ADD COLUMN IF NOT EXISTS contratante_logo BYTEA,
    ADD COLUMN IF NOT EXISTS contratante_logo_mime TEXT,
    -- A linha abaixo do nome do órgão: "SECRETARIA DE OBRAS E DEFESA CIVIL".
    -- `orgao_resumido` já existe e é o órgão; isto é a unidade dentro dele.
    ADD COLUMN IF NOT EXISTS contratante_unidade TEXT,
    -- Qual modelo de diário este contrato exige. NULL = o da BWS.
    ADD COLUMN IF NOT EXISTS diario_modelo TEXT;
