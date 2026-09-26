-- ===========================================================================
-- 028 — O ESPELHO DO CADASTRO DE COLABORADORES
--
-- Pedido do dono em 27/09/2026:
--
--   "A planilha de cadastros dos colaboradores, tudo vem do Pipefy, por isso
--    que é importante esse direcionamento para o cadastro do colaborador,
--    porque às vezes é preciso fazer a alteração do auxílio de alimentação, do
--    valor de um auxílio de transporte, ou um valor da gratificação. (…) eu
--    preciso poder atualizar as informações que estão na análise SP que
--    espelham o que está na planilha (…) um botão fácil para poder atualizar
--    imediatamente os dados, puxar os dados dessa planilha."
--
-- O CAMINHO DO DADO, de ponta a ponta:
--
--   Pipefy (o card da pessoa)  →  [automação]  →  planilha "Registro de
--   Colaboradores", aba "Dados Documentos"  →  [este botão]  →  esta tabela.
--
-- Ou seja: NADA aqui é digitado. Quem corrige o valor do auxílio corrige no
-- card do Pipefy; a automação leva para a planilha; o botão traz para cá. É por
-- isso que a tela tem o link para o card — o lugar de editar é lá, não aqui.
--
-- ⚠️ ESTA TABELA É CÓPIA, NÃO É VERDADE. Ela pode estar velha, e por isso
-- guarda `atualizado_em`: a tela mostra de quando é o dado. Quem decidir
-- dinheiro por um valor daqui tem de poder ver se ele é de antes ou de depois
-- da última mexida no Pipefy.
--
-- POR QUE SÓ 30 COLUNAS DE UMA PLANILHA DE 78. A própria planilha responde:
-- a aba oculta "CadastroColaboradores", que é quem alimenta as folhas de
-- alimentação, transporte, GM e diaristas, importa exatamente estas colunas da
-- aba "Dados Documentos" (lido das fórmulas em 27/09/2026):
--
--   Col1, Col5, Col23, Col24, Col25, Col26, Col47, Col49 … Col70, Col2
--
-- Trazer as 78 seria ler mais que o dobro de células a cada botão apertado,
-- numa instância de 2 GB dividida com 17 módulos. A lista do que é necessário
-- mora em `colaboradores.py`, num lugar só.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS analisesps.colaborador (
    cpf              TEXT PRIMARY KEY,           -- só dígitos; é a chave em tudo
    nome             TEXT NOT NULL DEFAULT '',
    -- O número do card no Pipefy ("Nº Registro Pipefy", coluna B da planilha).
    -- É o que monta https://app.pipefy.com/open-cards/<numero>. Fica TEXT de
    -- propósito: é identificador, não quantidade — ninguém soma card.
    card_pipefy      TEXT NOT NULL DEFAULT '',
    matricula        TEXT NOT NULL DEFAULT '',
    celular          TEXT NOT NULL DEFAULT '',
    cargo            TEXT NOT NULL DEFAULT '',
    tipo             TEXT NOT NULL DEFAULT '',   -- "Tipo" (coluna AU)
    tipo_contrato    TEXT NOT NULL DEFAULT '',   -- CLT, PJ, Pró-labore, Estágio…
    fase             TEXT NOT NULL DEFAULT '',   -- "Fase Atual" no Pipefy
    convencao        TEXT NOT NULL DEFAULT '',
    obra_cadastro    TEXT NOT NULL DEFAULT '',   -- o centro de custo do cadastro
    -- AS TRÊS COISAS QUE ELE DISSE QUE MAIS MUDAM. Ficam NUMERIC porque são
    -- dinheiro: NUMERIC não tem o arredondamento binário do float, e folha
    -- errada em centavo é folha errada.
    valor_alimentacao   NUMERIC(14,2),
    modo_alimentacao    TEXT NOT NULL DEFAULT '',  -- Mês / Mensal / Seg-Qui / Seg-Sex
    valor_transporte    NUMERIC(14,2),
    modo_transporte     TEXT NOT NULL DEFAULT '',  -- idem, e "Cartão" = não paga
    valor_gratificacao  NUMERIC(14,2),
    -- Marcações que decidem COMO se paga (lidas das fórmulas da GM):
    parcela_unica    TEXT NOT NULL DEFAULT '',   -- "Sim" = valor inteiro no fim do mês
    paga_por_beevale TEXT NOT NULL DEFAULT '',   -- "Sim" = BeeVale; senão SomaPay
    -- Desligamento: quem saiu não entra em pagamento novo.
    aviso_previo     DATE,
    ultimo_dia       DATE,
    data_saida       DATE,
    atualizado_em    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A busca da tela é por nome ou por CPF. O CPF já é a chave primária; o nome
-- precisa de índice porque a lista tem ~3.500 linhas e a tela busca a cada
-- tecla.
CREATE INDEX IF NOT EXISTS ix_analisesps_colaborador_nome
    ON analisesps.colaborador (lower(nome));

-- Quem está desligado sai das listas de pagamento. O índice parcial serve à
-- pergunta que a folha faz sempre: "quem está ativo?".
CREATE INDEX IF NOT EXISTS ix_analisesps_colaborador_ativos
    ON analisesps.colaborador (lower(nome))
 WHERE data_saida IS NULL;
