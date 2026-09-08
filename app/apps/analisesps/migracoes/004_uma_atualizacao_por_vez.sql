-- ---------------------------------------------------------------------------
-- 004 — UMA atualizacao por vez, garantida pelo BANCO
--
-- A regra "uma de cada vez" existia desde a migracao 001, mas era conferida
-- pelo PROGRAMA: `disparar()` perguntava "esta rodando?" e, se nao, abria uma
-- execucao nova. Entre a pergunta e a resposta cabe outra requisicao.
--
-- Com o botao manual isso quase nunca acontecia: uma pessoa clicando uma vez.
-- Passou a importar em 05/09/2026, quando a TELA ABERTA voltou a buscar
-- atualizacoes de 90 em 90 segundos (como no Streamlit): com quatro pessoas,
-- quatro telas perguntam quase ao mesmo tempo, e quatro processos de
-- sincronizacao poderiam nascer juntos — quatro leituras da planilha, quatro
-- vezes a cota do Google, para o mesmo trabalho.
--
-- O indice abaixo faz do banco o juiz: no maximo UMA linha com `fim IS NULL`.
-- A segunda tentativa e recusada pelo proprio Postgres, e o programa traduz a
-- recusa em "ja existe uma atualizacao em andamento".
-- ---------------------------------------------------------------------------


-- Pode haver execucoes abertas de processos que morreram (publicacao no meio
-- de uma carga, por exemplo). Sem fechar antes, o indice nao nasce. Fica a
-- mais recente; as outras sao encerradas com a razao escrita.
UPDATE analisesps.execucoes
   SET fim = now(), ok = FALSE,
       mensagem = 'Encerrada ao aplicar a atualizacao 004: havia mais de uma '
                  'execucao aberta ao mesmo tempo, o que nao deveria ser '
                  'possivel. Nada foi corrompido - e so rodar de novo.'
 WHERE fim IS NULL
   AND id <> (SELECT id FROM analisesps.execucoes
               WHERE fim IS NULL ORDER BY inicio DESC, id DESC LIMIT 1);


-- (true) e uma expressao constante: o indice e sobre "a linha existe", e a
-- condicao parcial limita as vivas. Resultado: no maximo uma viva.
CREATE UNIQUE INDEX IF NOT EXISTS ux_execucao_viva
    ON analisesps.execucoes ((TRUE)) WHERE fim IS NULL;
