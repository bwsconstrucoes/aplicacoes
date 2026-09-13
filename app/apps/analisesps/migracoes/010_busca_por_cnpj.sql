-- ---------------------------------------------------------------------------
-- 010 — O INDICE QUE FAZ A TELA "POR NOTA" ABRIR
--
-- Relatado pelo dono em 13/09/2026: "a tela por nota nao abre".
--
-- A causa tinha duas metades. A primeira era uma consulta por nota (200 por
-- pagina), corrigida no codigo. A segunda e esta: a busca da SP pelo CNPJ do
-- emitente compara os OITO PRIMEIROS DIGITOS do documento -- a raiz do CNPJ,
-- que e o que agrupa matriz e filiais. Isso e uma EXPRESSAO, e o indice comum
-- de coluna nao serve para expressao: o banco varre as 59 mil linhas toda vez.
--
-- MEDIDO antes da correcao, com 59.000 SPs e 4.000 notas: 28 segundos so para
-- montar as candidatas de uma pagina -- e numa maquina muito mais rapida que o
-- banco do Render, que tem um decimo de um nucleo.
--
-- O indice e sobre a MESMA expressao que o codigo usa, caractere por
-- caractere. Se ela mudar la e nao mudar aqui, o indice para de ser usado em
-- silencio e a lentidao volta sem ninguem entender por que.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ix_sps_raiz_documento
    ON analisesps.sps (left(regexp_replace(coalesce(documento, ''), '\D', '', 'g'), 8));

-- O desempate por valor exato acompanha a raiz na mesma consulta.
CREATE INDEX IF NOT EXISTS ix_sps_valor_num ON analisesps.sps (valor_num);
