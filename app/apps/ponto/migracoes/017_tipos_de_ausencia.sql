-- ===========================================================================
-- 017 — OS TIPOS DE AUSÊNCIA NUMA TELA SÓ: ver, cadastrar, excluir, e se abonam as horas
--
-- Pedido do dono, 29/10/2026: "a gente tem a parte de licenças e afastamentos, que são por lei, e
-- tem as demais (…) mas não tem nenhum canto no sistema que a gente possa visualizar e até cadastrar
-- novas (…) eu quero adicionar mais outra, aí eu tenho que sempre estar pedindo por aqui (…) e mesmo
-- excluir alguma. E lembrando que a gente tem que poder configurar se abona ou se não abona horas."
--
-- As licenças da lei já tinham tabela (006); ganham "abona". Os abonos da empresa (viagem a serviço,
-- dispensa) eram fixos no código; ganham a tabela deles, com os dois de sempre já cadastrados.
-- Tipo que NÃO abona: o dia fica justificado (aparece no espelho, com o nome), mas as horas são
-- descontadas — como a declaração de comparecimento com "nada".
-- ===========================================================================

ALTER TABLE ponto.tipos_licenca ADD COLUMN IF NOT EXISTS abona BOOLEAN NOT NULL DEFAULT TRUE;

CREATE TABLE IF NOT EXISTS ponto.tipos_abono (
    codigo            TEXT        PRIMARY KEY,
    nome              TEXT        NOT NULL,
    abona             BOOLEAN     NOT NULL DEFAULT TRUE,
    exige_documento   BOOLEAN     NOT NULL DEFAULT FALSE,
    ativo             BOOLEAN     NOT NULL DEFAULT TRUE,
    ordem             INTEGER     NOT NULL DEFAULT 100,
    atualizado_por    TEXT,
    atualizado_em     TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO ponto.tipos_abono (codigo, nome, abona, ordem) VALUES
  ('VIAGEM',   'Viagem a serviço',       TRUE, 10),
  ('DISPENSA', 'Dispensa pela empresa',  TRUE, 20)
ON CONFLICT (codigo) DO NOTHING;
