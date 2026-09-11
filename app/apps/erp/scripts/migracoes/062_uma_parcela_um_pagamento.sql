-- ============================================================================
-- Migração 062 — uma parcela, um pagamento
--
-- Não havia nada impedindo dois pagamentos para a mesma parcela. A única
-- proteção era uma conferência em Python, que LÊ antes de gravar: duas
-- pessoas clicando "baixar" no mesmo instante — ou o mesmo clique repetido
-- numa conexão ruim — passavam as duas e o ERP registrava a saída em dobro.
--
-- O código ganhou trava de linha (FOR UPDATE) na baixa. Esta restrição é a
-- segunda linha de defesa, no próprio banco, para o caso de um caminho novo
-- esquecer a trava. Dinheiro pago duas vezes não tem desfazer bonito.
--
-- ⚠️ ESTA É A ÚNICA MIGRAÇÃO QUE PODE FALHAR POR CAUSA DO DADO QUE JÁ EXISTE.
-- Se falhar, a mensagem diz quantas parcelas têm mais de um pagamento
-- registrado: é dinheiro para conferir no financeiro, não defeito da
-- migração. Ela está separada da 061 de propósito — um problema aqui não pode
-- impedir a correção da conciliação de entrar.
-- ============================================================================

DO $$
DECLARE
    repetidas integer;
BEGIN
    SELECT count(*) INTO repetidas
      FROM (SELECT parcela_id FROM pagamentos
             WHERE estorna_pagamento_id IS NULL
             GROUP BY parcela_id HAVING count(*) > 1) x;
    IF repetidas > 0 THEN
        RAISE EXCEPTION
            'Existem % parcela(s) com mais de um pagamento registrado. '
            'Confira essas baixas no financeiro e desfaça a repetida antes de '
            'aplicar esta atualização.', repetidas;
    END IF;
END $$;

-- Parcial pelo estorno: hoje nenhum caminho cria pagamento de estorno (desfazer
-- a baixa APAGA o pagamento e deixa o registro na trilha), mas se um dia criar,
-- o estorno não pode esbarrar nesta trava.
CREATE UNIQUE INDEX IF NOT EXISTS uq_pagamento_por_parcela
    ON pagamentos (parcela_id) WHERE estorna_pagamento_id IS NULL;
