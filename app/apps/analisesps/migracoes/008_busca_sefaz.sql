-- ---------------------------------------------------------------------------
-- 008 — A BUSCA AUTOMATICA DE NOTAS NA RECEITA
--
-- Pedido do dono, e com todas as letras: *"um dos coracoes dessa atualizacao e
-- essa busca automatica por novas notas"*. Ate aqui as notas so entravam pelo
-- relatorio do FSist, colado a mao numa aba.
--
-- COMO O SERVICO DA RECEITA FUNCIONA, e e isso que explica esta tabela: ele
-- nao responde "me da tudo de setembro". Ele responde "me da o que veio DEPOIS
-- do numero N" — um contador sequencial por CNPJ, o NSU. Cada resposta traz um
-- lote e diz qual foi o ultimo numero entregue; a proxima consulta comeca dali.
--
-- POR ISSO O PONTEIRO PRECISA DE TABELA. Se ele se perdesse, a busca
-- recomecaria do zero a cada rodada — e a Receita LIMITA consultas: quem
-- rebobina toda hora bate no limite e para de receber. Guardar onde parou nao
-- e otimizacao, e o que faz a busca funcionar.
--
-- UM PONTEIRO POR CNPJ E POR TIPO DE DOCUMENTO. A BWS tem mais de um CNPJ, e
-- NF-e e CT-e sao servicos SEPARADOS na Receita, cada um com a sua contagem.
-- Misturar os dois ponteiros faria um sobrescrever o outro e perder notas em
-- silencio.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analisesps.sefaz_ponteiro (
    cnpj          TEXT NOT NULL,          -- so digitos
    tipo          TEXT NOT NULL,          -- NFE | CTE
    -- Ate onde ja se leu. O formato da Receita e numerico de 15 posicoes, mas
    -- fica como TEXTO: e um contador opaco, nao um numero para somar, e o
    -- zero a esquerda faz parte dele.
    ultimo_nsu    TEXT NOT NULL DEFAULT '0',
    -- O maior NSU que a Receita disse existir. Comparado com o de cima, diz se
    -- ainda ha lote para buscar - e e o que evita bater no limite pedindo o
    -- que ja acabou.
    maior_nsu     TEXT NOT NULL DEFAULT '0',
    consultado_em TIMESTAMPTZ,
    -- O ultimo recado da Receita, em portugues. Quando ela recusa, o motivo
    -- tem de ficar visivel: "consumo indevido" e "certificado vencido" pedem
    -- acoes completamente diferentes, e as duas chegam como falha.
    ultimo_recado TEXT NOT NULL DEFAULT '',
    documentos    INTEGER NOT NULL DEFAULT 0,   -- quantos ja vieram por aqui
    PRIMARY KEY (cnpj, tipo)
);
