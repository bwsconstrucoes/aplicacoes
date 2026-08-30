-- ============================================================================
-- Migração 011 — controle de avisos enviados
-- Garante que a pessoa não receba duas vezes o mesmo comprovante quando a
-- baixa é refeita (correção). A chave é o par (evento, referência): reenvio
-- só acontece se for pedido explicitamente.
-- ============================================================================
CREATE TABLE IF NOT EXISTS notificacoes (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    evento          TEXT        NOT NULL,          -- BAIXA | RECEBIMENTO | DEVOLUCAO...
    referencia      TEXT        NOT NULL,          -- chave idempotente do envio
    titulo_id       BIGINT      REFERENCES titulos(id),
    pagamento_id    BIGINT      REFERENCES pagamentos(id),
    destinatario_id BIGINT      REFERENCES usuarios(id),
    destino         TEXT,                          -- telefone/chat usado
    canal           TEXT        NOT NULL DEFAULT 'TELEGRAM',
    situacao        TEXT        NOT NULL DEFAULT 'PENDENTE',  -- ENVIADO|FALHA|IGNORADO
    mensagem        TEXT,
    erro            TEXT,
    com_anexo       BOOLEAN     NOT NULL DEFAULT FALSE,
    tentativas      SMALLINT    NOT NULL DEFAULT 0,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    enviado_em      TIMESTAMPTZ,
    CONSTRAINT uq_notificacao UNIQUE (evento, referencia)
);

CREATE INDEX IF NOT EXISTS idx_notificacoes_titulo ON notificacoes (titulo_id);
