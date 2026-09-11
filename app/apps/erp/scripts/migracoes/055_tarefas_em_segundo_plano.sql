-- ===========================================================================
-- Migração 055 — a FILA DE TRABALHO EM SEGUNDO PLANO.
--
-- O PEDIDO, do dono em 08/09/2026: *"e quando essa base de dados for
-- crescendo? Como é que é a estratégia de manter isso rápido?"* — e a resposta
-- registrada no ROTEIRO foi que o que mais resolve não é máquina maior, é
-- **parar de fazer trabalho pesado enquanto alguém espera a tela**.
--
-- O QUE É "TRABALHO PESADO", NA PRÁTICA DESTE SISTEMA
--
-- Importar 100 cards do Pipefy (cada um com consulta e anexos para baixar),
-- ler um lote de documentos com IA, emitir nota e ficar esperando a prefeitura
-- responder. Nada disso cabe no tempo de um clique — e hoje tudo isso segura
-- uma das QUATRO linhas de atendimento do serviço, que é o mesmo serviço dos
-- outros treze módulos. Uma importação grande deixa o sistema inteiro lento
-- para todo mundo, e nem quem pediu entende por quê.
--
-- POR QUE A FILA VIVE NO BANCO, e não só na memória
--
-- O serviço se REINICIA sozinho de tempos em tempos (é a faxina de memória do
-- gunicorn). Fila na memória perderia o trabalho no meio, calada. Aqui cada
-- trabalho é uma linha: se o serviço morrer no meio, a linha continua lá,
-- dizendo que estava em execução, e é retomada quando ele volta.
--
-- A BATIDA (`batida_em`) é o que separa "está trabalhando" de "morreu no
-- meio". Um trabalho que não dá sinal de vida por alguns minutos foi
-- interrompido, e volta para a fila. Sem isso ele ficaria "executando" para
-- sempre, e ninguém saberia que precisa refazer.
--
-- TENTATIVAS TÊM TETO. Trabalho que falha três vezes para de tentar e fica
-- registrado como falhado, com o motivo escrito. Repetir para sempre um
-- trabalho que sempre falha é gastar máquina e esconder o defeito.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS tarefas (
    id          BIGSERIAL PRIMARY KEY,
    tipo        TEXT NOT NULL,
    -- O que a PESSOA lê na tela ("Importar 37 cards do Pipefy"). Escrito por
    -- quem enfileira, porque é ele que sabe o que está pedindo.
    rotulo      TEXT NOT NULL,
    parametros  JSONB NOT NULL DEFAULT '{}'::jsonb,
    situacao    TEXT NOT NULL DEFAULT 'PENDENTE',
    tentativas  INT NOT NULL DEFAULT 0,
    -- Andamento, para a tela mostrar "23 de 100" em vez de uma ampulheta.
    passo       INT NOT NULL DEFAULT 0,
    total       INT,
    mensagem    TEXT,
    resultado   JSONB,
    erro        TEXT,
    usuario_id  BIGINT REFERENCES usuarios(id),
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    iniciado_em TIMESTAMPTZ,
    batida_em   TIMESTAMPTZ,
    concluido_em TIMESTAMPTZ
);

ALTER TABLE tarefas DROP CONSTRAINT IF EXISTS ck_tarefas_situacao;
ALTER TABLE tarefas ADD CONSTRAINT ck_tarefas_situacao
    CHECK (situacao IN ('PENDENTE', 'EXECUTANDO', 'CONCLUIDA',
                        'FALHADA', 'CANCELADA'));

ALTER TABLE tarefas DROP CONSTRAINT IF EXISTS ck_tarefas_contagens;
ALTER TABLE tarefas ADD CONSTRAINT ck_tarefas_contagens
    CHECK (tentativas >= 0 AND passo >= 0 AND (total IS NULL OR total >= 0));

-- A consulta que o trabalhador faz o tempo todo: "qual é a próxima da fila?".
-- Parcial de propósito: a tabela guarda o histórico, mas só o que está na fila
-- entra neste índice.
CREATE INDEX IF NOT EXISTS idx_tarefas_fila ON tarefas (id)
    WHERE situacao = 'PENDENTE';

-- A varredura de órfãs: quem estava executando quando o serviço reiniciou.
CREATE INDEX IF NOT EXISTS idx_tarefas_executando ON tarefas (batida_em)
    WHERE situacao = 'EXECUTANDO';

-- A tela: as últimas de quem está olhando, e as últimas de todas.
CREATE INDEX IF NOT EXISTS idx_tarefas_recentes ON tarefas (criado_em DESC);
