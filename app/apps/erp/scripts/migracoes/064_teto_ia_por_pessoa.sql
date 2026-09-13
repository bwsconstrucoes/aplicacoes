-- ============================================================================
-- Migração 064 — teto de gasto com IA POR PESSOA
--
-- Decisão do dono em 12/09/2026: *"pra gente não ter surpresa, vamos limitar
-- aí. Deve ficar no cadastro da pessoa, com o valor estimado já de cinco
-- dólares. E se eu quiser colocar diferente pra outras pessoas (…) que seja
-- editável. Se eu quiser colocar alguém sem limite, eu coloco, ou botar dez
-- dólares"*.
--
-- O raciocínio dele, que vale registrar: *"isso é mais é gestão que vai usar,
-- pessoal de obra eu não acredito que vai usar muito"*. Por isso o padrão é
-- baixo — cinco dólares seguram a curiosidade de quem experimenta, e quem
-- precisa de mais recebe mais, um a um.
--
-- NULO = SEM LIMITE, e é escolha explícita de quem edita o cadastro. Todo
-- mundo que já existe recebe os cinco dólares, então nulo só aparece quando
-- alguém apagar o campo de propósito.
--
-- ⚠️ COLUNA NOVA EM `usuarios` — a armadilha conhecida do ERP (ver CLAUDE.md).
-- O código sobe para o Render ANTES de este botão ser apertado, e nessa janela
-- qualquer carga do objeto Usuario pelo ORM quebra. Por isso a leitura do teto
-- no caminho quente é por SQL direto, com "coluna ainda não existe" valendo
-- como "sem teto configurado" — o ERP continua de pé e o aviso global segue
-- valendo. Apertar o botão no mesmo momento da publicação fecha a janela.
-- ============================================================================

ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS teto_ia_usd NUMERIC(10,2);

UPDATE usuarios SET teto_ia_usd = 5.00 WHERE teto_ia_usd IS NULL;

ALTER TABLE usuarios ALTER COLUMN teto_ia_usd SET DEFAULT 5.00;

COMMENT ON COLUMN usuarios.teto_ia_usd IS
    'Teto mensal de gasto com IA desta pessoa, em US$. NULO = sem limite.';
