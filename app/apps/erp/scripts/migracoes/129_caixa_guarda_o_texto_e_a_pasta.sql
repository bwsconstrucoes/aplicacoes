-- 129 — A caixa de entrada guarda o TEXTO da resposta e para onde a mensagem foi (26/11/2026).
--
-- O dono: "a gente está aqui na parte de pedidos. A gente clica (…) não vê o
-- que é que a contabilidade respondeu, não vê o que é que ela anexou." E sobre
-- a leitura: o que o agente identifica vai para a pasta "Agente" (lido); o que
-- ele não identifica fica na caixa de entrada, NÃO LIDO, para o DP.
--
-- Só acrescenta colunas; nada é apagado nem reescrito.
ALTER TABLE email_entrada_mensagens ADD COLUMN IF NOT EXISTS corpo TEXT;
ALTER TABLE email_entrada_mensagens ADD COLUMN IF NOT EXISTS movida_para TEXT;

-- O DIÁRIO DO AGENTE (26/11/2026). O dono: "um ambiente onde as ações que o
-- agente fez de forma autônoma ficasse todo o histórico (…) ele vai fazendo,
-- aí eu analiso (…) fez correto. (…) essa aqui deu errado. Aí eu vou entrar
-- aqui no chat e dizer." Cada linha é UMA coisa que o agente fez sozinho; a
-- revisão (CERTO/ERRADO + comentário) é de uma pessoa.
CREATE TABLE IF NOT EXISTS dp_agente_acoes (
    id              BIGSERIAL PRIMARY KEY,
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now(),
    acao            TEXT NOT NULL,
    texto           TEXT NOT NULL,
    solicitacao_id  BIGINT REFERENCES dp_solicitacoes(id),
    pedido_id       BIGINT REFERENCES dp_pedidos(id),
    colaborador_id  BIGINT REFERENCES colaboradores(id),
    detalhe         JSONB NOT NULL DEFAULT '{}'::jsonb,
    revisao         TEXT CHECK (revisao IN ('CERTO', 'ERRADO')),
    comentario      TEXT,
    revisado_por    BIGINT REFERENCES usuarios(id),
    revisado_em     TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS dp_agente_acoes_sol ON dp_agente_acoes (solicitacao_id);
CREATE INDEX IF NOT EXISTS dp_agente_acoes_pendentes ON dp_agente_acoes (criado_em) WHERE revisao IS NULL;

-- A CONFERÊNCIA DO RETORNO LIGADA (26/11/2026). As rotinas nascem desligadas a
-- pedido dele; esta ele mandou ligar: "A conclusão (…) pode. Mas é bom dar
-- destaque tudo que for feito pelo agente." Desliga-se na tela do agente.
INSERT INTO dp_agente_rotinas (chave, modo) VALUES ('conferencia', 'FAZ')
ON CONFLICT (chave) DO UPDATE SET modo = 'FAZ'
 WHERE dp_agente_rotinas.modo = 'DESLIGADA';
