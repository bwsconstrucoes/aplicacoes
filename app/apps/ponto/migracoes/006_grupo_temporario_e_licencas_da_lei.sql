-- ===========================================================================
-- 006 — O APARELHO DE GRUPO É TEMPORÁRIO, E AS LICENÇAS DA LEI NUMA LISTA
--
-- Pedidos do dono, 06/10/2026:
--   · "Essa solução de bater ponto de algumas pessoas deveria ter caráter
--     precário. O sistema deveria detectar que aquela situação já não está
--     acontecendo mais e já sugerir o cancelamento da permissão (…) uma equipe
--     que voltou a uma obra para resolver algo por alguns dias."
--   · "Precisa ter uma base de dados de afastamentos permitidos por lei para
--     facilitar a inclusão."
-- ===========================================================================

-- A permissão de grupo vence. Aparelho de grupo já aprovado ganha 15 dias a
-- partir de hoje, para ninguém ficar sem bater no dia da atualização.
ALTER TABLE ponto.dispositivos ADD COLUMN IF NOT EXISTS valido_ate DATE;
UPDATE ponto.dispositivos SET valido_ate = (now() AT TIME ZONE 'America/Fortaleza')::date + 15
 WHERE perfil = 'LISTA' AND valido_ate IS NULL;

-- O tipo de licença do pedido (casamento, luto…), da lista abaixo.
ALTER TABLE ponto.ocorrencias ADD COLUMN IF NOT EXISTS subtipo TEXT;

-- ---------------------------------------------------------------------------
-- As licenças que a lei garante (CLT art. 473 e leis próprias). A lista é a
-- da LEI; a convenção coletiva da construção pode dar mais — por isso os dias
-- e o "ativo" se ajustam na Configuração, e se acrescenta tipo novo lá.
--   dias        máximo por pedido (NULL = o que o documento disser)
--   limite_ano  quantas vezes em 12 meses (NULL = sem limite)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ponto.tipos_licenca (
    codigo            TEXT        PRIMARY KEY,
    nome              TEXT        NOT NULL,
    base_legal        TEXT        NOT NULL DEFAULT '',
    dias              INTEGER     CHECK (dias IS NULL OR dias BETWEEN 1 AND 365),
    limite_ano        INTEGER     CHECK (limite_ano IS NULL OR limite_ano >= 1),
    exige_documento   BOOLEAN     NOT NULL DEFAULT TRUE,
    documento         TEXT        NOT NULL DEFAULT '',
    ativo             BOOLEAN     NOT NULL DEFAULT TRUE,
    ordem             INTEGER     NOT NULL DEFAULT 100,
    atualizado_por    TEXT,
    atualizado_em     TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO ponto.tipos_licenca (codigo, nome, base_legal, dias, limite_ano, exige_documento, documento, ordem) VALUES
  ('CASAMENTO',  'Casamento (licença gala)',                                  'CLT art. 473, II',           3, NULL, TRUE,  'certidão de casamento', 10),
  ('LUTO',       'Falecimento de cônjuge, pai, mãe, filho, irmão ou dependente (luto)',
                                                                              'CLT art. 473, I',            2, NULL, TRUE,  'certidão de óbito', 20),
  ('PATERNIDADE','Nascimento de filho (licença-paternidade)',                 'CF/ADCT art. 10, § 1º',      5, NULL, TRUE,  'certidão de nascimento', 30),
  ('DOACAO_SANGUE','Doação voluntária de sangue',                             'CLT art. 473, IV',           1, 1,    TRUE,  'comprovante do hemocentro', 40),
  ('ALISTAMENTO_ELEITORAL','Alistamento eleitoral (tirar o título)',          'CLT art. 473, V',            2, NULL, TRUE,  'comprovante do cartório eleitoral', 50),
  ('MESARIO',    'Trabalho nas eleições (mesário) — 2 dias por dia convocado','Lei 9.504/1997, art. 98',    NULL, NULL, TRUE, 'declaração do TRE', 60),
  ('SERVICO_MILITAR','Exigências do serviço militar',                         'CLT art. 473, VI',           NULL, NULL, TRUE, 'documento do órgão militar', 70),
  ('VESTIBULAR', 'Prova de vestibular ou ENEM',                               'CLT art. 473, VII',          NULL, NULL, TRUE, 'comprovante de inscrição e presença', 80),
  ('JUIZO',      'Comparecimento em juízo (testemunha, jurado, parte)',       'CLT art. 473, VIII e 822',   NULL, NULL, TRUE, 'intimação ou declaração do fórum', 90),
  ('PRE_NATAL',  'Acompanhar a esposa ou companheira em consulta da gravidez','CLT art. 473, X',            1, 2,    TRUE,  'declaração da consulta', 100),
  ('FILHO_MEDICO','Levar filho de até 6 anos ao médico',                      'CLT art. 473, XI',           1, 1,    TRUE,  'declaração da consulta', 110),
  ('EXAME_CANCER','Exames preventivos de câncer',                             'CLT art. 473, XII',          1, 3,    TRUE,  'comprovante do exame', 120),
  ('OUTRA',      'Outra licença (convenção coletiva ou acordo)',              '',                           NULL, NULL, FALSE, 'o documento que houver', 900)
ON CONFLICT (codigo) DO NOTHING;

-- ---------------------------------------------------------------------------
-- QUEM FAZ PEDIDO (pedido do dono, 06/10/2026: "só quem pode anexar esses
-- documentos são os responsáveis da obra, os mesmos que batem o ponto da obra,
-- ou o celular da obra (…) o ajuste só pode ser solicitado por quem tem
-- permissão de bater ponto"). O pedido feito no APARELHO DA OBRA (ou no de
-- grupo) ganha origem própria.
-- ---------------------------------------------------------------------------
ALTER TABLE ponto.ocorrencias DROP CONSTRAINT IF EXISTS ocorrencias_origem_check;
ALTER TABLE ponto.ocorrencias ADD CONSTRAINT ocorrencias_origem_check
    CHECK (origem IN ('APP', 'GESTAO', 'API', 'APARELHO'));
