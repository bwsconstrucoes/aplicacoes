-- ---------------------------------------------------------------------------
-- 007 — O NOME QUE VALE PARA CADA CPF/CNPJ
--
-- Pedido do dono em 11/09/2026: *"o Pipefy e frouxo no campo credor. Um lanca
-- 'Aco Cearense Limitada', outro bota so 'Aco Cearense', o outro escreve
-- errado. Queria que a planilha fosse analisada comparando credor e CPF/CNPJ,
-- e equalizasse para o melhor nome."*
--
-- POR QUE UMA TABELA, e nao so uma regra rodando toda vez. A medicao na
-- planilha de verdade mostrou que 6 dos 8 CNPJs divergentes NAO tem resposta
-- automatica: CELPE virou NEOENERGIA, MAFEMA aparece como razao social e como
-- nome de fantasia, e em MAGNA o nome mais longo e justamente o digitado
-- errado. Isso e escolha de gente.
--
-- Esta tabela e a MEMORIA dessa escolha. O dono decide UMA VEZ por CNPJ, e da
-- segunda vez em diante aquele fornecedor e automatico — senao a mesma lista
-- voltaria a perguntar a mesma coisa toda semana, que e exatamente o que ele
-- pediu para nao acontecer ("minimizar a interacao do humano").
--
-- O DOCUMENTO E A CHAVE, so com os digitos. Na planilha ele vem
-- "29.066.773/0001-52" numa linha e "29066773000152" noutra; guardar como veio
-- faria o mesmo fornecedor ter duas linhas aqui e nenhuma das duas valer.
--
-- CNPJ INTEIRO, e nao a raiz de oito digitos. Matriz e filial sao
-- estabelecimentos diferentes e podem ter nomes legitimamente diferentes;
-- juntar pela raiz escreveria o nome da matriz nas SPs da filial.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analisesps.credor_nome (
    documento    TEXT PRIMARY KEY,              -- so digitos: 11 (CPF) ou 14 (CNPJ)
    nome         TEXT NOT NULL,
    -- IGUAL  = mesmo nome escrito diferente (acento, espaco) — o sistema resolveu
    -- COMECO = um era abreviacao do outro ("TRI" -> "TRIBUNAL DE JUSTICA...")
    -- DECIDIR = nomes de verdade diferentes: foi o dono quem escolheu
    tipo         TEXT NOT NULL DEFAULT 'DECIDIR',
    automatico   BOOLEAN NOT NULL DEFAULT FALSE,
    decidido_por TEXT NOT NULL DEFAULT '',
    decidido_em  TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Quantas SPs foram reescritas quando a escolha foi aplicada. Serve para
    -- responder "o que essa decisao mexeu?" meses depois.
    sps_mudadas  INTEGER NOT NULL DEFAULT 0
);

-- A tela separa o que o dono decidiu do que o sistema resolveu sozinho.
CREATE INDEX IF NOT EXISTS ix_credor_nome_tipo ON analisesps.credor_nome (tipo);
