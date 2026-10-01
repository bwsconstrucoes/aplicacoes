-- ===========================================================================
-- 035 — O LOG DOS ARQUIVOS DE PAGAMENTO GERADOS
--
-- Pedido do dono, em 27/09/2026, com todas as letras:
--
--   "O importante é que tenha o arquivo salvo, e que tenha link no card, e que
--    tenha também o LOG NA APLICAÇÃO que a gente está construindo aqui, com as
--    informações e o link que a gente quer baixar por lá."
--
-- ⚠️ POR QUE O LOG NÃO PODE VIVER SÓ NO DRIVE E NO PIPEFY. Hoje o histórico da
-- geração está espalhado em três lugares que não conversam: a pasta do Drive (que
-- diz o nome do arquivo e nada mais), o card do Pipefy (que diz o link e nada
-- mais) e as abas `HistoricoBeeVale` da planilha. Para responder "este arquivo foi
-- gerado quando, por quem, com que total, e bateu?" é preciso abrir os três.
--
-- Aqui fica a resposta inteira, numa linha: competência, pagamento, destino,
-- verbas, conta, quantas pessoas, total, quem gerou, o link do arquivo e o link do
-- card. E os avisos que havia na hora — porque aviso que só existiu na tela não
-- explica diferença nenhuma três meses depois.
--
-- ⚠️ ESTA TABELA NÃO É A VERDADE DO PAGAMENTO, é o registro de uma geração. A
-- verdade é a apropriação fechada (migração 034). Gerar o arquivo duas vezes
-- (porque o primeiro subiu errado) deixa DUAS linhas aqui, de propósito: apagar a
-- primeira esconderia que houve duas, e é justamente isso que alguém precisa ver
-- quando o portal recebeu dois arquivos.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS analisesps.folha_arquivo_gerado (
    id      SERIAL PRIMARY KEY,
    ano     INTEGER NOT NULL,
    mes     INTEGER NOT NULL CHECK (mes BETWEEN 1 AND 12),
    tipo    TEXT NOT NULL DEFAULT '',
    -- "beevale" ou "somapay"; "analise" para o arquivo de conferência.
    destino TEXT NOT NULL DEFAULT '',
    -- As verbas que entraram, separadas por "+" ("alimentacao+transporte"). Texto
    -- porque é para ler, não para procurar dentro.
    verbas  TEXT NOT NULL DEFAULT '',
    conta   TEXT NOT NULL DEFAULT '',

    nome_arquivo TEXT NOT NULL DEFAULT '',
    pessoas      INTEGER NOT NULL DEFAULT 0,
    total        NUMERIC(14,2) NOT NULL DEFAULT 0,

    -- O que ele baixa por aqui. `drive_id` fica para conseguir achar o arquivo
    -- mesmo se o link mudar de forma.
    drive_id  TEXT NOT NULL DEFAULT '',
    link      TEXT NOT NULL DEFAULT '',
    -- O card que recebeu o link, quando houver. Vazio enquanto o card não existe:
    -- criar card é passo separado e opcional, decidido em 26/09/2026.
    card_pipefy TEXT NOT NULL DEFAULT '',
    link_card   TEXT NOT NULL DEFAULT '',

    avisos    TEXT NOT NULL DEFAULT '',
    criado_em  TIMESTAMPTZ NOT NULL DEFAULT now(),
    criado_por TEXT NOT NULL DEFAULT ''
);

-- A pergunta da tela: "o que foi gerado, do mais novo para o mais antigo".
CREATE INDEX IF NOT EXISTS ix_analisesps_folha_arquivo_gerado_quando
    ON analisesps.folha_arquivo_gerado (criado_em DESC);
-- E a da competência: "o que já saiu de 09/2026?".
CREATE INDEX IF NOT EXISTS ix_analisesps_folha_arquivo_gerado_competencia
    ON analisesps.folha_arquivo_gerado (ano, mes, tipo);
