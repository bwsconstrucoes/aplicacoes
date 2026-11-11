-- ===========================================================================
-- 095 — MAIS DE UM PERFIL DE ACESSO POR PESSOA
--
-- Pedido dele em 09/11/2026, depois de eu ter recomendado o contrário:
--
--   *"Pode ser que eu queira que uma pessoa utilize mais de um perfil de
--   acesso. Digamos que uma pessoa é do financeiro, mas eu quero que ele
--   acesse diário de obras. O sistema permite que eu vincule mais de um
--   perfil de acesso? Caso não, seria interessante. Isso facilita."*
--
-- Eu disse que a marcação ação a ação já resolvia e que dois perfis trariam
-- ambiguidade. Ele ouviu e respondeu *"ainda assim prefiro"*. É decisão dele,
-- e ela é boa para o uso real: dizer "esta pessoa é Financeiro + Diário de
-- obra" é uma frase que ele entende e consegue conferir depois; duas dezenas
-- de caixinhas marcadas à mão, não.
--
-- ⚑ A AMBIGUIDADE QUE EU TEMIA FOI RESOLVIDA NA REGRA, NÃO ADIADA:
-- **perfil SÓ SOMA acesso, nunca tira.** Quem tem dois perfis fica com o
-- MAIOR nível de cada seção — se um diz "só olhar" e o outro "olhar e mexer",
-- vale "olhar e mexer". Assim não existe a pergunta "qual deles ganha", que é
-- o que faria ninguém mais entender o cadastro seis meses depois.
--
-- Quem TIRA continua sendo a marcação por pessoa (migração 032), que vence os
-- perfis. A ordem final é: perfis somam → a marcação da pessoa corrige → o
-- ADMIN não se tranca para fora. Essa ordem já existia; só o primeiro passo
-- mudou de "um perfil" para "a soma dos perfis".
--
-- ⚑ POR QUE `usuarios.perfil_id` CONTINUA EXISTINDO, e não virou lixo: ele é
-- o perfil PRINCIPAL, o que a lista de operadores mostra numa coluna só. Esta
-- tabela guarda os ACRÉSCIMOS. Fosse tudo para cá, toda tela que hoje mostra
-- "o perfil da pessoa" teria de passar a mostrar uma lista — e a mudança, que
-- é de acesso, encostaria em meia dúzia de telas sem necessidade.
--
-- ⚑ E A JANELA ENTRE PUBLICAR E APERTAR O BOTÃO: a leitura dos acréscimos é
-- uma consulta SEPARADA e protegida. Enquanto esta tabela não existir, ela
-- devolve "nenhum acréscimo" e o sistema segue decidindo pelo perfil
-- principal, exatamente como antes. Se ela estivesse na mesma consulta do
-- perfil principal, TODA a empresa cairia no cargo antigo nessa janela — que
-- é a armadilha conhecida deste repositório.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS usuario_perfis (
    usuario_id  BIGINT      NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    perfil_id   BIGINT      NOT NULL REFERENCES perfis(id)   ON DELETE CASCADE,
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    criado_por  BIGINT      REFERENCES usuarios(id),
    PRIMARY KEY (usuario_id, perfil_id)
);

-- Para a leitura de toda requisição não varrer a tabela.
CREATE INDEX IF NOT EXISTS ix_usuario_perfis_usuario
    ON usuario_perfis (usuario_id);
