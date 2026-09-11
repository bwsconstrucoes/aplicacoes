-- ===========================================================================
-- Migração 054 — a SAÚDE DO SISTEMA: quanto tempo cada tela leva.
--
-- POR QUE MEDIR, E POR QUE AGORA
--
-- O dono perguntou, em 08/09/2026, se o sistema ia aguentar o crescimento. A
-- resposta honesta na época foi "o gargalo é a máquina compartilhada, não o
-- tamanho da base" — mas foi uma resposta de raciocínio, não de medição. E a
-- próxima pergunta dele vai ser sobre GASTAR: trocar de plano no Render, subir
-- o banco. Decisão de gastar não pode ser palpite.
--
-- O QUE SE GUARDA, E POR QUE ASSIM
--
-- Uma linha por DIA e por ROTA, com a soma dos tempos, a pior chamada e a
-- contagem. NÃO uma linha por requisição: o ERP divide o processo com treze
-- módulos e uma tabela que cresce por clique viraria, ela mesma, o problema
-- que veio medir. Agregado responde tudo que interessa — a média, o pior caso,
-- o volume — e cabe em alguns milhares de linhas por ano.
--
-- A GRAVAÇÃO É EM LOTE, não a cada requisição. Os números se acumulam na
-- memória do processo (que é um só, por decisão de projeto) e descem ao banco
-- de tempos em tempos. Medir não pode custar mais que o que se mede.
--
-- E A MEDIÇÃO NUNCA DERRUBA A TELA: qualquer erro no caminho da medida é
-- engolido e registrado. Um sistema que cai por causa do próprio termômetro é
-- pior do que um sistema sem termômetro.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS saude_tempos (
    dia        DATE NOT NULL,
    rota       TEXT NOT NULL,
    chamadas   BIGINT NOT NULL DEFAULT 0,
    ms_total   BIGINT NOT NULL DEFAULT 0,
    ms_maior   INT NOT NULL DEFAULT 0,
    erros      BIGINT NOT NULL DEFAULT 0,
    lentas     BIGINT NOT NULL DEFAULT 0,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dia, rota)
);

ALTER TABLE saude_tempos DROP CONSTRAINT IF EXISTS ck_saude_nao_negativo;
ALTER TABLE saude_tempos ADD CONSTRAINT ck_saude_nao_negativo
    CHECK (chamadas >= 0 AND ms_total >= 0 AND ms_maior >= 0
           AND erros >= 0 AND lentas >= 0);

CREATE INDEX IF NOT EXISTS idx_saude_dia ON saude_tempos (dia DESC);
