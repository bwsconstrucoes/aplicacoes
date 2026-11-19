-- ===========================================================================
-- O DESLIGAMENTO VIRA UM PROCESSO QUE SÓ FECHA NO FIM
--
-- Pedido dele em 19/11/2026, com as palavras dele:
--
--   *"É feita a solicitação, o DP analisa, o DP encaminha o pedido para
--    contabilidade (…) A contabilidade retorna a documentação rescisória, aí o
--    DP vai encaminhar para quem solicitou, essa pessoa que solicitou vai
--    apresentar para o colaborador a documentação, colher assinaturas e
--    devolver para o DP. E o DP, além disso, ainda gera o lançamento
--    financeiro (…) da rescisão, como também (…) a multa rescisória. Vai ser
--    feito esses dois lançamentos pagos, e só então a solicitação se encerra."*
--
-- O QUE ACONTECIA ANTES: "Atender" o pedido de desligamento gravava as datas
-- no cadastro e FECHAVA o pedido na mesma hora. Tudo o que vem depois — o
-- pedido à contabilidade, a documentação que volta, as assinaturas, os dois
-- pagamentos — ficava solto, num checklist que ninguém era obrigado a marcar.
-- O pedido dizia "atendido" com a rescisão ainda por pagar.
--
-- O QUE MUDA: atender passa a ABRIR o processo (situação "Em andamento"), e o
-- pedido só se encerra quando todos os passos estiverem cumpridos. A maior
-- parte dos passos o sistema enxerga sozinho (o pedido à contabilidade, a
-- volta da documentação, o pagamento do lançamento) — esta tabela guarda só o
-- que ele NÃO enxerga: o que o DP marcou à mão, o lançamento que o DP ligou ao
-- passo, e o que foi dispensado com motivo.
--
-- ⚑ PASSO QUE O SISTEMA ENXERGA NÃO PRECISA DE LINHA AQUI. A linha só existe
-- quando alguém agiu. Assim a lista de passos pode crescer sem migração, e o
-- pedido antigo não precisa ser "preenchido" por ninguém.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS dp_desligamento_passos (
    id              BIGSERIAL PRIMARY KEY,
    solicitacao_id  BIGINT NOT NULL REFERENCES dp_solicitacoes(id) ON DELETE CASCADE,
    -- O código do passo, como está em `core/desligamento_processo.py`.
    passo           TEXT   NOT NULL,
    -- FEITO          — cumprido (marcado pelo DP, ou a decisão da continuidade);
    -- NAO_SE_APLICA  — dispensado, com o motivo escrito;
    -- LANCADO        — o lançamento financeiro foi ligado ao passo; ele só
    --                  conta como cumprido quando o lançamento estiver PAGO,
    --                  e isso o sistema lê do próprio lançamento.
    situacao        TEXT   NOT NULL,
    titulo_id       BIGINT REFERENCES titulos(id),
    observacao      TEXT,
    detalhe         JSONB  NOT NULL DEFAULT '{}'::jsonb,
    feito_por       BIGINT REFERENCES usuarios(id),
    feito_em        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_dp_deslig_passo_situacao
        CHECK (situacao IN ('FEITO', 'NAO_SE_APLICA', 'LANCADO')),
    -- Um registro por passo por pedido: marcar de novo SUBSTITUI, e a trilha
    -- do pedido (dp_solicitacao_eventos) guarda cada mudança.
    CONSTRAINT ux_dp_deslig_passo UNIQUE (solicitacao_id, passo)
);

CREATE INDEX IF NOT EXISTS ix_dp_deslig_passo_titulo
    ON dp_desligamento_passos (titulo_id) WHERE titulo_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- A DOCUMENTAÇÃO RESCISÓRIA ASSINADA tem tipo próprio (19/11/2026).
--
-- Ele: *"o arquivo precisa ser cobrado pelo Agente e recebido (…) o agente
-- analisa e o DP valida o recebimento da documentação."* A obra devolve pelo
-- portal, e o tipo é o que separa o termo que a contabilidade mandou (RESCISAO)
-- do mesmo termo depois de assinado — sem isso, os dois ficariam no cadastro
-- com o mesmo nome e ninguém saberia qual tem assinatura. Sigilo PESSOAL:
-- rescisão traz salário e verbas.
-- ---------------------------------------------------------------------------
INSERT INTO documento_tipos (codigo, nome, grupo, dono, sigilo)
VALUES ('RESCISAO-ASSINADA', 'Documentação rescisória assinada', 'FISCAL',
        'PESSOA', 'PESSOAL')
ON CONFLICT (codigo) DO NOTHING;
