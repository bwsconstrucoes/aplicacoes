-- 020 — Os passos de cada atualização, para a tela contar a história inteira
--
-- 06/10/2026, o dono: "essa tela de atualizar os dados era para ter mais
-- informativo (…) o que é que atualizou, até onde atualizou, onde é que
-- interrompeu, foram quantas páginas, quantas linhas, o que é que ficou
-- pendente, qual foi a última tentativa (…) e qual ação eu preciso fazer".
--
-- A `execucoes` só guardava a etapa ATUAL: quando a atualização morria, sabia-se
-- onde parou, mas não o que já tinha feito. Aqui fica um registro por etapa —
-- quando começou, o último sinal e o último andamento ("página 37 de 420").
--
-- O código tolera a tabela ainda não existir (sobe para o Render antes do
-- botão "Aplicar atualizações do banco" ser apertado): só não grava os passos.

CREATE TABLE IF NOT EXISTS painel.execucao_passos (
    -- sem chave estrangeira de propósito: limpar `execucoes` (os testes
    -- fazem TRUNCATE) não pode depender desta tabela existir
    execucao_id  BIGINT      NOT NULL,
    etapa        TEXT        NOT NULL,
    ordem        INTEGER     NOT NULL,
    inicio       TIMESTAMPTZ NOT NULL DEFAULT now(),
    visto_em     TIMESTAMPTZ NOT NULL DEFAULT now(),
    detalhe      TEXT,
    PRIMARY KEY (execucao_id, etapa)
);
