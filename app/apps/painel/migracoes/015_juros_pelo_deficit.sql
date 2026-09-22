-- 015 — Os juros de empréstimo passam a ter régua própria na Prestação de Contas
--
-- Até aqui o juro de empréstimo ia junto com o resto do custo da matriz e era
-- repartido entre as obras pelo CUSTO DE PESSOAL. Isso cobra juro de obra que
-- se paga sozinha só por ela ter gente — e não cobra de quem viveu de dinheiro
-- emprestado.
--
-- A régua nova é a necessidade de caixa: o juro de cada mês é dividido entre
-- as obras que estavam com o caixa acumulado negativo naquele mês, na
-- proporção do buraco de cada uma. Quem fez faltar dinheiro é quem paga o
-- preço de ter faltado.
--
-- Três chaves novas, todas com padrão seguro:
--   categoria_juros     qual categoria do OMIE é juro de empréstimo;
--   juros_por_deficit   '1' liga a régua nova; '0' volta ao comportamento
--                       antigo (juro dentro do bolo da estrutura);
--   juros_sem_deficit   o que fazer com o juro de um mês em que ninguém
--                       estava no vermelho: 'sobra' (fica visível como custo
--                       sem dono) ou 'estrutura' (segue a régua da estrutura).

INSERT INTO painel.config (chave, valor) VALUES
    ('categoria_juros',   'Juros sobre Empréstimos'),
    ('juros_por_deficit', '1'),
    ('juros_sem_deficit', 'sobra')
ON CONFLICT (chave) DO NOTHING;
