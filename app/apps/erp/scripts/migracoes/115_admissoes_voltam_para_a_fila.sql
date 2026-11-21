-- ===========================================================================
-- AS ADMISSÕES "ATENDIDAS" COM O CADASTRO AINDA POR CONCLUIR VOLTAM À FILA
--
-- Decisão dele em 19/11/2026: a solicitação de admissão só sai da fila quando
-- o cadastro chega a "Concluído" (`core/admissao_processo.py`). Até ontem,
-- "Atender" fechava o pedido na hora — e o trabalho que faltava (o registro da
-- carteira, o contrato do PJ) seguia só no quadro de etapas, longe do pedido.
--
-- ⚑ DE ONDE SAEM: pedido de admissão (tipo CADASTRO) marcado "Atendida", com
-- o cadastro ligado (colaborador_id) e esse cadastro em etapa que NÃO é
-- Concluído, Afastado, Desligado nem Cancelado — ou seja, Inicial, Assinar ou
-- Contabilidade. Pedido sem cadastro ligado, ou de pessoa que já saiu, fica
-- como está.
--
-- Cada um ganha a marca `processo_admissao` (é ela que faz a tela mostrar os
-- passos) e um evento na trilha do pedido dizendo por que voltou.
-- ===========================================================================
WITH voltam AS (
    SELECT d.id
      FROM dp_solicitacoes d
      JOIN colaboradores c ON c.id = d.colaborador_id
     WHERE d.tipo = 'CADASTRO'
       AND d.situacao = 'ATENDIDA'
       AND UPPER(COALESCE(c.etapa, 'CONCLUIDO')) NOT IN
           ('CONCLUIDO', 'AFASTADO', 'DESLIGADO', 'CANCELADO')
), mexeu AS (
    UPDATE dp_solicitacoes d
       SET situacao = 'EM_ANDAMENTO',
           dados = COALESCE(d.dados, '{}'::jsonb)
                   || '{"processo_admissao": "1"}'::jsonb,
           atualizado_em = now()
      FROM voltam v
     WHERE d.id = v.id
    RETURNING d.id
)
INSERT INTO dp_solicitacao_eventos (solicitacao_id, acao, detalhe)
SELECT id, 'VOLTOU_PARA_A_FILA',
       '{"motivo": "a admissão só se encerra com o cadastro concluído (19/11/2026)"}'::jsonb
  FROM mexeu;
