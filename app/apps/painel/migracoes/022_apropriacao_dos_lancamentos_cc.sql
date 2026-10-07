-- 022 — A apropriação dos lançamentos de conta corrente, lida no OMIE
--
-- 07/10/2026. O movimento financeiro do OMIE (a listagem de onde o painel lê os
-- pagamentos) NÃO traz a apropriação: o arquivo cru de 09/01/2026 que o dono
-- mandou tem só "detalhes" e "resumo" nos 193 movimentos do dia, nenhum com
-- departamento. O lançamento da Sicredi (100.000,01, metade CRECHESUAPE,
-- metade ESCPE18 na tela do OMIE) chegava sem obra.
--
-- A apropriação mora na consulta do LANÇAMENTO DE CONTA CORRENTE, uma chamada
-- por lançamento. Esta tabela guarda a resposta INTEIRA, como o OMIE mandou,
-- uma linha por lançamento — e não é apagada quando a janela de pagamentos é
-- relida, para não perguntar de novo o que já foi respondido.
--
-- `erro` guarda o motivo quando o OMIE recusou; `tentativas` impede perguntar
-- para sempre pelo mesmo lançamento.

CREATE TABLE IF NOT EXISTS painel.apropriacao_lancamentos_cc (
    ncodmovcc   BIGINT PRIMARY KEY,
    bruto       TEXT,
    erro        TEXT,
    tentativas  INTEGER NOT NULL DEFAULT 0,
    lido_em     TIMESTAMP NOT NULL DEFAULT now()
);
