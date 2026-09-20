-- ===========================================================================
-- 012 — GUARDAR O MOVIMENTO QUE NAO TEM TITULO, EM VEZ DE JOGAR FORA
--
-- O painel e montado a partir dos TITULOS (contas a pagar e a receber). O
-- extrato de movimentos serve para saber quando cada titulo foi baixado e por
-- quanto — por isso a carga so guardava movimento que apontasse para um titulo,
-- e descartava o resto na hora, contando quantos foram apenas no log.
--
-- So que existe movimento lancado DIRETO na conta corrente, sem titulo nenhum
-- por tras. Esse dinheiro entrou ou saiu da conta de verdade e NUNCA apareceu
-- em lugar nenhum do painel. O dono percebeu o sintoma em 13/09/2026 ("era pra
-- aparecer todos os lancamentos igual o relatorio de conta corrente do OMIE") e
-- em 20/09 perguntou o essencial: *quais sao e como me afetam?*
--
-- Nao dava para responder, porque a carga descartava antes de gravar.
--
-- ESTA TABELA NAO ENTRA EM NENHUM NUMERO DE TELA. Ela nao vira linha do `fato`,
-- nao soma no DRE, nao soma na Visao Geral. Ela existe para PODER SER OLHADA:
-- quanto e, de quais contas, de quais categorias, de quais anos. Trazer esse
-- dinheiro para dentro dos relatorios e outra decisao, do dono, e tem risco
-- real de contar o mesmo valor duas vezes — quem olhar primeiro decide melhor.
--
-- Ela e preenchida junto com `movimentos`: zerada na carga completa, e na
-- atualizacao do dia apagada e reinserida pela mesma janela de datas. Ou seja,
-- so tera conteudo depois da proxima rebaixa de movimentos.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS painel.movimentos_sem_titulo (
    id             BIGSERIAL PRIMARY KEY,
    cnatureza      TEXT,
    cgrupo         TEXT,
    cstatus        TEXT,
    ccodcateg      TEXT,
    ncodcc         BIGINT,
    ncodcliente    BIGINT,
    ddtpagamento   TEXT,
    ddtvenc        TEXT,
    ddtemissao     TEXT,
    ddtregistro    TEXT,
    cliquidado     TEXT,
    nvalortitulo   NUMERIC(16,2),
    nvalpago       NUMERIC(16,2),
    nvalliquido    NUMERIC(16,2),
    nvalaberto     NUMERIC(16,2),
    njuros         NUMERIC(16,2),
    nmulta         NUMERIC(16,2),
    ndesconto      NUMERIC(16,2),
    sync_em        TEXT
);
CREATE INDEX IF NOT EXISTS ix_mst_pgto  ON painel.movimentos_sem_titulo(ddtpagamento);
CREATE INDEX IF NOT EXISTS ix_mst_categ ON painel.movimentos_sem_titulo(ccodcateg);
CREATE INDEX IF NOT EXISTS ix_mst_cc    ON painel.movimentos_sem_titulo(ncodcc);
