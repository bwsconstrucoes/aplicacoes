-- ===========================================================================
-- 030 — O ID FORTES NO CADASTRO: a ponte entre a folha e as pessoas
--
-- ⚠️ SEM ISTO A FOLHA NÃO CONVERSA COM NINGUÉM, e é a razão desta migração
-- existir agora: a Folha Sintética da contabilidade traz **código do empregado e
-- nome** — não traz CPF. O ponto, o cadastro, o rateio e o pagamento são todos
-- por **CPF**. O que liga os dois mundos é o ID Fortes.
--
-- A planilha "Registro de Colaboradores" tem uma aba própria para isso ("ID
-- Fortes"), e é de lá que este campo vem — pelo mesmo botão "Atualizar cadastro".
--
-- POR QUE COLUNA NO CADASTRO, E NÃO TABELA NOVA: é um atributo da pessoa, não
-- uma relação. Uma tabela de/para separada abriria a porta para dois CPFs
-- reivindicarem o mesmo ID (ou o contrário) e para as duas ficarem fora de
-- sincronia com o cadastro. Aqui o ID mora onde a pessoa mora.
--
-- ⚠️ NÃO É `UNIQUE`, e é decisão consciente. O ID Fortes DEVERIA ser único, mas
-- ele vem de uma planilha que pessoas editam à mão. Um `UNIQUE` faria a carga
-- inteira do cadastro falhar por causa de uma linha duplicada — e o cadastro
-- inteiro é mais importante que a duplicidade. A duplicidade vira **crítica na
-- tela**, que é onde alguém pode consertar.
-- ===========================================================================

ALTER TABLE analisesps.colaborador
    ADD COLUMN IF NOT EXISTS id_fortes TEXT NOT NULL DEFAULT '';

-- A pergunta que a folha faz para cada linha: "de quem é este código?".
CREATE INDEX IF NOT EXISTS ix_analisesps_colaborador_fortes
    ON analisesps.colaborador (id_fortes) WHERE id_fortes <> '';

-- E a folha guarda o CPF resolvido na própria linha, para a tela não ter de
-- refazer o cruzamento a cada visita. A coluna já existe (migração 029); o que
-- falta é o caminho de preenchê-la, que é código.
