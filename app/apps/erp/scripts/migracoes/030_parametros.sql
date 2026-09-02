-- ============================================================================
-- Migração 030 — parâmetros do sistema
-- Uma tabela chave/valor para o que o ADMIN ajusta pela tela e não merece
-- coluna própria: o primeiro uso é o teto mensal de gasto com IA (em US$) e a
-- marca de "aviso já enviado neste mês", que impede o aviso de repetir.
-- Credencial NÃO entra aqui — continua na Environment do Render.
-- ============================================================================
CREATE TABLE IF NOT EXISTS parametros (
    chave           TEXT        PRIMARY KEY,
    valor           TEXT        NOT NULL DEFAULT '',
    atualizado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    atualizado_por  BIGINT      REFERENCES usuarios(id)
);

COMMENT ON TABLE parametros IS
    'Configurações ajustáveis pela tela (ex.: ia_teto_mensal_usd). Nunca credencial.';
