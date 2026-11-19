-- ===========================================================================
-- PRIMEIRO ACESSO PELO PRÓPRIO PORTAL — a pessoa prova quem é, e alguém aprova
--
-- Desenho dele em 18/11/2026, depois de encontrar o problema na prática ao
-- tentar orientar a equipe:
--
--   *"Então tá errado esse acesso ser liberado pelo aplicativo de ponto. Qual
--    o sentido que tem de uma pessoa que vai lançar um diário ter que ir lá no
--    aplicativo do ponto? Não tem sentido nenhum. Não tem nada a ver com
--    ponto. O cara tem que ir pro portal e criar o acesso dele por lá."*
--
-- Ele estava certo, e o defeito era meu: eu pus o pedido no ponto porque lá a
-- pessoa JÁ estava identificada — resolvi o meu problema, não o dela. Quem só
-- vai lançar diário tinha de criar PIN de ponto, uma coisa que talvez nunca
-- use, para pedir acesso a outra coisa.
--
-- O DESENHO, com as palavras dele: *"o cara vai colocar o CPF dele, pode
-- colocar o telefone, e quando os dados baterem, ele receber algum código no
-- telefone, via WhatsApp (…) digitaria esse código, digitaria o e-mail dele."*
--
-- ⚑ O TELEFONE CONFERE, NÃO ESCOLHE O DESTINO, e ele confirmou: *"o CPF e
-- telefone já cadastrado em sistema. Não vai ser para um telefone que não
-- existe no sistema."* O código vai para o número DO CADASTRO. Se fosse para o
-- digitado, quem soubesse o CPF de alguém poria o próprio telefone e entraria
-- no lugar dela.
--
-- ⚑ E AINDA PASSA POR ALGUÉM. Decisão dele no mesmo dia: *"ainda assim precisa
-- ser validado por alguém no sistema. Depois dele pedir ele ainda vai pedir a
-- aprovação, que aí aumenta o nível de segurança."*
--
-- São duas perguntas diferentes, e o código prova só a primeira:
--   · QUEM é esta pessoa? → o CPF, o telefone e o código respondem;
--   · ela DEVE ter o portal? → quem responde é gente, na fila de sempre.
--
-- Sem isso, qualquer um dos ~660 cadastrados se liberaria sozinho — e o acesso
-- nasce podendo LANÇAR DIÁRIO, que ele decidiu em 08/11 ser do administrativo
-- da obra: *"quem lança realmente é o administrativo da obra, não é
-- encarregado, não."*
--
-- ⚑ A FILA É A MESMA (`portal_pedidos_de_acesso`, migração 108), e isso é o
-- ponto: não nasce uma segunda fila nem um segundo botão de liberar. O que
-- muda é de ONDE o pedido vem e que ele chega com a identidade já conferida.
-- ===========================================================================

-- O código de seis dígitos, guardado só como resumo (nunca em claro).
CREATE TABLE IF NOT EXISTS portal_codigos_de_acesso (
    id             SERIAL PRIMARY KEY,
    colaborador_id INTEGER NOT NULL REFERENCES colaboradores(id) ON DELETE CASCADE,
    codigo_hash    TEXT    NOT NULL,
    expira_em      TIMESTAMPTZ NOT NULL,
    usado_em       TIMESTAMPTZ,
    tentativas     INTEGER NOT NULL DEFAULT 0,
    ip             TEXT,
    criado_em      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- O teto por hora é contado por pessoa: quem pede dez códigos em dez minutos
-- ou errou o telefone, ou está tentando a sorte. A consulta é sempre
-- "quantos desta pessoa na última hora".
CREATE INDEX IF NOT EXISTS ix_portal_codigos_pessoa
    ON portal_codigos_de_acesso (colaborador_id, criado_em DESC);

-- ---------------------------------------------------------------------------
-- A IDENTIDADE CONFERIDA FICA MARCADA NO PEDIDO.
--
-- Quem aprova precisa saber a diferença: um pedido que chegou com código
-- confirmado por WhatsApp no número do cadastro vale mais que um que chegou
-- sem. Sem a marca, a fila trata os dois igual e a confirmação não serve de
-- nada para quem decide.
-- ---------------------------------------------------------------------------
ALTER TABLE portal_pedidos_de_acesso
    ADD COLUMN IF NOT EXISTS identidade_confirmada_em TIMESTAMPTZ;

COMMENT ON COLUMN portal_pedidos_de_acesso.identidade_confirmada_em IS
    'Quando a pessoa provou quem é com o código do WhatsApp (primeiro acesso '
    'pelo portal). NULL = pedido aberto sem essa prova.';
