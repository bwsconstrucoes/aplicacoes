-- ===========================================================================
-- O AFASTAMENTO PASSA A TER TIPO, DATAS E RETORNO CONFERIDO
--
-- Pergunta dele em 18/11/2026: *"Colaborador estava afastado e vai retornar ao
-- trabalho. O que deve ser exigido dele e o que o sistema deve pedir pra travar
-- o simples retorno?"*
--
-- O QUE ACONTECIA ANTES: "Afastado" era uma etapa do quadro e NADA MAIS — sem
-- tipo, sem data de início, sem previsão de volta, sem documento. Voltar era
-- arrastar o cartão para "Concluído": a situação voltava para ATIVO sozinha e a
-- pessoa voltava a bater ponto. Ficar 10 dias por atestado e 8 meses por
-- acidente de trabalho eram a MESMA COISA para o sistema.
--
-- E o tipo não é detalhe de cadastro: é ele que decide tudo o que vem depois —
-- se a volta exige exame médico, se sobra estabilidade no emprego, se o período
-- de férias reinicia. Sem o tipo, não havia como exigir nada.
--
-- AS RESPOSTAS DELE, no mesmo dia, que estão implementadas aqui:
--   · os tipos que acontecem na BWS: atestado longo, auxílio-doença do INSS,
--     acidente de trabalho e licença-maternidade — *"sim, temos esses"*;
--   · estabilidade: *"a contabilidade conta, mas é interessante termos a
--     informação também"* — então o sistema GUARDA e MOSTRA, e não se arvora a
--     ser a fonte oficial;
--   · ASO de retorno: *"acho que tem que travar"*. Trava. ⚠️ É uma decisão
--     DIFERENTE da de 10/11 sobre o ASO de ADMISSÃO (*"não vamos colocar ele
--     como obrigatório ainda"*), e não é contradição: no retorno o exame é
--     exigido por lei ANTES de a pessoa trabalhar, e ele pode voltar "apto com
--     restrição" — em obra, isso é a diferença entre trabalho e acidente;
--   · quem move: *"o DP vai fazer isso depois que alimentar com as informações
--     pertinentes para o retorno."*
--
-- ⚑ POR QUE OS TIPOS NÃO SÃO TABELA, e as licenças do ponto são: as regras de
-- cada tipo aqui (exige exame, quantos meses de estabilidade, a partir de quê)
-- são LEI, não preferência da empresa — não há o que configurar na tela. Os
-- tipos vivem em `core/cadastros/afastamentos.py`, do mesmo jeito que os
-- templates da contabilidade. As licenças do ponto são tabela porque a
-- convenção coletiva pode dar mais dias que a lei.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS colaborador_afastamentos (
    id              SERIAL PRIMARY KEY,
    colaborador_id  INTEGER NOT NULL REFERENCES colaboradores(id) ON DELETE CASCADE,
    tipo            TEXT    NOT NULL,
    data_inicio     DATE    NOT NULL,
    -- Quando a pessoa é ESPERADA de volta. Serve para o vigia avisar ANTES,
    -- para o exame ser marcado antes de ela aparecer na obra.
    previsao_retorno DATE,
    -- Quando ela VOLTOU de verdade. NULL = afastamento em curso.
    data_retorno    DATE,
    motivo          TEXT,
    observacao      TEXT,
    -- ⚑ A ESTABILIDADE, calculada no RETORNO e guardada aqui. Não se recalcula
    -- depois: se a lei mudar, o que valia para quem voltou em 2026 continua
    -- sendo o que valia em 2026 — e é a mesma razão por que a categoria da
    -- convenção fica guardada no cadastro.
    estabilidade_ate DATE,
    estabilidade_motivo TEXT,
    aberto_por      INTEGER REFERENCES usuarios(id),
    encerrado_por   INTEGER REFERENCES usuarios(id),
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    encerrado_em    TIMESTAMPTZ
);

-- ⚑ UM AFASTAMENTO ABERTO POR PESSOA. Dois ao mesmo tempo não existem no mundo
-- e, no sistema, fariam o retorno fechar o errado — e a estabilidade sair do
-- afastamento errado, que é o pior dos dois estragos.
CREATE UNIQUE INDEX IF NOT EXISTS ux_afastamento_aberto
    ON colaborador_afastamentos (colaborador_id) WHERE data_retorno IS NULL;

CREATE INDEX IF NOT EXISTS ix_afastamentos_por_pessoa
    ON colaborador_afastamentos (colaborador_id, data_inicio DESC);

-- A estabilidade é consultada de fora (a tela do desligamento pergunta "esta
-- pessoa está estável?"), e aí o que interessa é quem AINDA está no prazo.
CREATE INDEX IF NOT EXISTS ix_afastamentos_estabilidade
    ON colaborador_afastamentos (estabilidade_ate)
    WHERE estabilidade_ate IS NOT NULL;

-- ---------------------------------------------------------------------------
-- QUEM JÁ ESTÁ AFASTADO HOJE não pode virar refém da regra nova.
--
-- Há gente na etapa "Afastado" agora, posta lá quando a etapa não tinha tipo
-- nem data. Se a trava do retorno exigisse um afastamento registrado, essas
-- pessoas não conseguiriam voltar de jeito nenhum — a trava nasceria
-- impedindo o trabalho em vez de organizá-lo.
--
-- Então o passado entra com o que se sabe: tipo NAO_INFORMADO e a data da
-- última mudança para "Afastado" (ou a admissão, se nem isso houver). O DP
-- corrige o tipo na tela quando for tratar a volta, e o tipo NAO_INFORMADO é
-- o único que NÃO exige exame — porque cobrar o exame de um afastamento cuja
-- natureza ninguém registrou seria cobrar no escuro.
-- ---------------------------------------------------------------------------
INSERT INTO colaborador_afastamentos
       (colaborador_id, tipo, data_inicio, motivo)
SELECT c.id, 'NAO_INFORMADO',
       COALESCE((SELECT max(e.criado_em)::date FROM colaborador_etapas e
                  WHERE e.colaborador_id = c.id AND e.para = 'AFASTADO'),
                c.admissao, c.data_inicio, CURRENT_DATE),
       'afastamento que já existia quando o tipo passou a ser pedido '
       '(18/11/2026); a natureza não foi registrada na época'
  FROM colaboradores c
 WHERE c.etapa = 'AFASTADO'
   AND NOT EXISTS (SELECT 1 FROM colaborador_afastamentos a
                    WHERE a.colaborador_id = c.id AND a.data_retorno IS NULL);
