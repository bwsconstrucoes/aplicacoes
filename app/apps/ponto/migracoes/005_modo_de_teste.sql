-- ===========================================================================
-- 005 — O MODO DE TESTE
--
-- Pedido do dono, 05/10/2026: *"queria fazer teste comigo mesmo (…) se tivesse
-- algum canto que eu pudesse gravar uma geolocalização e colocar meu nome e
-- CPF"*. Sem isso, testar exigia pôr uma obra falsa na planilha C. Diários (que
-- o painel, a emissão de NFS-e e a Análise de SPs também leem) e a pessoa no
-- Registro de Colaboradores.
--
-- A obra de teste e a pessoa de teste ficam marcadas aqui: a obra vale no ponto
-- mesmo fora da planilha; a pessoa bate como ativa mesmo fora do Registro.
-- ===========================================================================
ALTER TABLE ponto.obra_config ADD COLUMN IF NOT EXISTS ensaio BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE ponto.colaborador_config ADD COLUMN IF NOT EXISTS ensaio BOOLEAN NOT NULL DEFAULT FALSE;
