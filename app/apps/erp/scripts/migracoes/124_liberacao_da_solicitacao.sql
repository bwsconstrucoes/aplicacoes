-- 124 — A SOLICITAÇÃO DE MATERIAL PRECISA SER LIBERADA PELA ENGENHARIA
--
-- O dono, em 22/11/2026: "quando o administrativo de obra fizer esse pedido,
-- alguém lá da parte da engenharia tem que dar um ok, para liberar aquele
-- insumo para compra. Ou um supervisor de obras (...) O compras tem que
-- visualizar, mas não consegue gerar nem mapa de cotação, nem fazer pedido
-- de compra."
--
-- Por que existe: a obra pedia direto ao suprimento e a engenharia não ficava
-- sabendo — o que deixa passar quantidade inflada e necessidade de aditivo.
--
-- `liberacao` vazio = item antigo, ou regularização de compra já feita: vale
-- como liberado. Nada do que já existe fica preso.
ALTER TABLE suprimento_itens
    ADD COLUMN IF NOT EXISTS liberacao    TEXT
        CHECK (liberacao IS NULL OR liberacao IN ('PENDENTE', 'LIBERADO')),
    ADD COLUMN IF NOT EXISTS liberado_por BIGINT REFERENCES usuarios(id),
    ADD COLUMN IF NOT EXISTS liberado_em  TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS ix_suprimento_itens_a_liberar
    ON suprimento_itens (obra_id) WHERE liberacao = 'PENDENTE';

-- Quem libera: os perfis prontos de engenharia e direção. O administrativo de
-- obra fica de fora de propósito — é o pedido dele que precisa do ok.
INSERT INTO perfil_secoes (perfil_id, secao, nivel)
SELECT p.id, v.secao, v.nivel
  FROM perfis p
  JOIN (VALUES
    ('Administrador',       'sup_liberar', 'EDITAR'),
    ('Diretor financeiro',  'sup_liberar', 'EDITAR'),
    ('Gestor de obras',     'sup_liberar', 'EDITAR'),
    ('Supervisor de obras', 'sup_liberar', 'EDITAR')
  ) AS v(perfil, secao, nivel) ON v.perfil = p.nome
ON CONFLICT (perfil_id, secao) DO NOTHING;

-- A COMPRA TARDIA EM DOIS TEMPOS (22/11/2026). Quem lançou informa os
-- materiais; o pedido só nasce quando alguém com alçada dá a ciência, no nome
-- dele. Os materiais informados esperam aqui.
ALTER TABLE compras_fora_do_fluxo ADD COLUMN IF NOT EXISTS itens JSONB;
