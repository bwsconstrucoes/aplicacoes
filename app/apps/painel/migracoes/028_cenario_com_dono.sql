-- Cada cenário tem DONO (13/11/2026).
--
-- O dono do painel: "eu queria que cada usuário que tem acesso à prestação
-- possa fazer suas edições, do jeito que ele quiser. Todavia, que ele pudesse
-- aproveitar o que já está editado (…) eu já criei um cenário e tudo, aí eu
-- queria que o próximo usuário fizesse a partir dali."
--
-- `dono_id` vazio = cenário do administrador (todos os que existiam). Com
-- número = cenário de uma pessoa do cadastro de acesso: é a cópia dela, que
-- só ela altera. Apagar o acesso da pessoa apaga os cenários dela junto.
ALTER TABLE painel.cenario
  ADD COLUMN IF NOT EXISTS dono_id BIGINT REFERENCES painel.usuarios(id) ON DELETE CASCADE;

-- O nome era único na tabela inteira; passa a ser único POR DONO — senão a
-- cópia de uma pessoa brigaria com a de outra, e com o original.
ALTER TABLE painel.cenario DROP CONSTRAINT IF EXISTS cenario_nome_key;
CREATE UNIQUE INDEX IF NOT EXISTS ux_cenario_nome_por_dono
    ON painel.cenario (COALESCE(dono_id, 0), nome);
