-- ===========================================================================
-- 127 — A FOTO SABE ONDE E QUANDO FOI TIRADA; E O CLIMA MEDIDO GANHA DONO
--
-- Pedido dele em 24/11/2026, olhando o diário de obra:
--
--     *"Se eu precisar que as minhas fotos tenham geolocalização e que na
--      legenda dessa foto apareça essa geolocalização e ainda que fique um
--      carimbo da foto com a data e geolocalização que ela foi retirada."*
--
-- ⚠️ O DEFEITO QUE ISTO FECHA, e ele é de base: para a foto caber no PDF, o
-- sistema a reduzia e REGRAVAVA — e regravar apaga os dados internos da
-- imagem (EXIF), inclusive o GPS e a hora que o celular tinha gravado. Ou
-- seja: a informação chegava e era destruída no envio. Estas colunas existem
-- para que ela seja LIDA ANTES e guardada onde não se perde.
--
-- ⚑ POR QUE EM COLUNA, e não só dentro da imagem: dado que vive só no arquivo
-- não se consulta ("quais fotos desta obra têm local confirmado?"), não
-- sobrevive a uma recompressão futura e não dá para mostrar na legenda sem
-- abrir cada foto.
-- ===========================================================================

-- A FOTO ---------------------------------------------------------------
ALTER TABLE anexos
    -- Quando a imagem foi TIRADA (do EXIF), que é diferente de quando foi
    -- enviada (`criado_em`). Foto de sábado enviada na segunda é o caso
    -- comum, e o diário é da data do serviço, não do upload.
    ADD COLUMN IF NOT EXISTS tirada_em      TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS latitude       NUMERIC(10, 7),
    ADD COLUMN IF NOT EXISTS longitude      NUMERIC(10, 7),
    -- Precisão em metros, quando o aparelho informa. Coordenada sem precisão
    -- é coordenada sem qualificação: ±8 m prova; ±2.000 m não prova nada.
    ADD COLUMN IF NOT EXISTS precisao_m     INTEGER,
    -- DE ONDE veio o local, e esta coluna é a mais importante das cinco:
    --   'EXIF'     — estava gravado na própria foto (o mais forte);
    --   'APARELHO' — o navegador informou no momento do envio;
    --   NULL       — não há local, e aí NÃO SE CARIMBA.
    -- Carimbar uma foto sem local comprovado seria o sistema AFIRMANDO uma
    -- coisa que não sabe, num documento que a fiscalização assina.
    ADD COLUMN IF NOT EXISTS origem_local   TEXT;

COMMENT ON COLUMN anexos.origem_local IS
    'EXIF (gravado na foto), APARELHO (navegador no envio) ou NULL (sem local; não se carimba)';

-- Quais fotos de uma obra têm local confirmado — a pergunta que a tela faz.
CREATE INDEX IF NOT EXISTS idx_anexos_com_local
    ON anexos (entidade_tipo, entidade_id)
 WHERE latitude IS NOT NULL;

-- O CLIMA ---------------------------------------------------------------
-- `diarios_obra.clima_medido` (jsonb) existe desde a migração 092 e NUNCA
-- foi preenchida: a coluna nasceu junto com a decisão dele em 08/11/2026
-- (*"eu acho que é interessante sim colocar"*) e o serviço nunca foi ligado.
-- Agora vai ser. Só falta saber QUANDO cada medição foi buscada, para não
-- perguntar de novo ao serviço o que já se sabe.
ALTER TABLE diarios_obra
    ADD COLUMN IF NOT EXISTS clima_medido_em TIMESTAMPTZ;

COMMENT ON COLUMN diarios_obra.clima_medido_em IS
    'quando o clima foi buscado no serviço; vazio = nunca foi buscado';
