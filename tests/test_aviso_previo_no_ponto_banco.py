# -*- coding: utf-8 -*-
"""
QUEM CUMPRE AVISO PRÉVIO CONTINUA BATENDO PONTO — contra um Postgres de verdade.

⚑ POR QUE ESTE ARQUIVO EXISTE: em 17/11/2026, horas depois de ligar o cadastro
do ERP como base de pessoas do ponto, ele viu na tela:

    *"Pessoas que estavam com último dia trabalhado mesmo depois de hoje e
    estavam na fase Solicitações DP, quando ativei o cadastro do ERP foram
    considerados desligados."*

**Gente cumprindo aviso prévio parou de conseguir bater ponto.** Essa gente
trabalha, e a jornada dela é registro legal do REP-P — um dia sem batida é um
buraco na prova da jornada, e não se conserta depois.

**A causa tinha duas metades, e as duas estão presas aqui:**

1. O ERP gravava "DESLIGADO" ao ver que EXISTIA data de saída, sem olhar se ela
   havia chegado (`pessoas.ja_saiu`, testado em
   `test_saida_agendada_nao_desliga.py`).
2. **O ponto acreditava na caixinha.** Com a planilha como base ele PERGUNTAVA
   a data (`r.data_saida < hoje`); com o ERP, apenas lia `c.situacao`. Duas
   bases respondendo diferente à mesma pergunta.

Nada disso é alcançável pelo dublê de sessão do `conftest.py`: a regra vive
dentro de um CASE no SELECT. Por isso aqui se pergunta ao Postgres.

⚠️ A régua é `<` hoje nas DUAS bases: **a pessoa trabalha o último dia**.
"""
from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.banco

HOJE = dt.date.today()
ONTEM = HOJE - dt.timedelta(days=1)
SEMANA_QUE_VEM = HOJE + dt.timedelta(days=7)


@pytest.fixture(scope="session")
def _schema_ponto(banco):
    from app.apps.ponto import migracoes_runner
    resultado = migracoes_runner.aplicar_pendentes()
    assert not resultado["erro"], resultado
    return True


@pytest.fixture
def base_erp(_schema_ponto, sessao_real):
    """A base de pessoas do ponto NO CADASTRO DO ERP, que é o cenário dele.

    ⚠️ Não é enfeite: `registro.estado` guarda a resposta por 30 s e o padrão é
    a PLANILHA. Basta outro arquivo da suíte criar a tabela
    `analisesps.colaborador` (a fixture `_schema_analisesps`) para a base virar
    a planilha no meio do caminho — e aí este arquivo testaria a régua errada.
    Foi exatamente o que aconteceu na primeira rodada com `-n auto`.
    """
    from app.apps.ponto.core import parametros, registro
    conn = sessao_real.connection()
    antes = parametros.ler(conn, registro.PARAMETRO_FONTE, registro.FONTE_PADRAO)
    parametros.gravar(conn, registro.PARAMETRO_FONTE, registro.FONTE_ERP, "teste")
    sessao_real.commit()
    registro.esquecer()
    yield
    parametros.gravar(sessao_real.connection(), registro.PARAMETRO_FONTE,
                      antes or registro.FONTE_PADRAO, "teste")
    sessao_real.commit()
    registro.esquecer()


@pytest.fixture
def gente(base_erp, _schema_ponto, sessao_real):
    """Quatro casos que o ponto tem de distinguir, com a caixinha ERRADA de
    propósito nos dois primeiros — é como o banco dele estava."""
    s = sessao_real
    obra = s.execute(text("""
        INSERT INTO obras (codigo, nome, status)
        VALUES ('AVPREV1', 'Obra do aviso previo', 'ATIVA') RETURNING id""")).scalar()
    casos = {
        # nome                    cpf             situacao       demissao
        "aviso_previo":    ("52998224725", "DESLIGADO", SEMANA_QUE_VEM),
        "ultimo_dia_hoje": ("11144477735", "DESLIGADO", HOJE),
        "saiu_mesmo":      ("12345678909", "DESLIGADO", ONTEM),
        "trabalhando":     ("98765432100", "ATIVO",     None),
    }
    ids = {}
    for chave, (cpf, situacao, demissao) in casos.items():
        ids[chave] = s.execute(text("""
            INSERT INTO colaboradores (nome, cpf, situacao, obra_id, demissao,
                                       admissao, vinculo, tipo_contrato)
            VALUES (:n, :c, :s, :o, :d, '2026-01-05', 'CTPS', 'CLT_INDETERMINADO')
            RETURNING id"""),
            {"n": f"Pessoa {chave}", "c": cpf, "s": situacao, "o": obra,
             "d": demissao}).scalar()
    s.commit()
    return {"obra": obra, "ids": ids}


def _situacao_no_ponto(sessao_real, colaborador_id: int) -> str:
    from app.apps.ponto.core import cadastros
    p = cadastros.colaborador_por_id(sessao_real.connection(), colaborador_id)
    assert p is not None, "a pessoa não apareceu para o ponto"
    return p["situacao"]


def test_quem_tem_saida_marcada_para_depois_bate_ponto(sessao_real, gente):
    """⚠️ O TESTE PRINCIPAL. A caixinha do cadastro diz DESLIGADO (é como o
    banco dele ficou depois da importação) e o ponto responde ATIVO, porque a
    data manda."""
    assert _situacao_no_ponto(sessao_real, gente["ids"]["aviso_previo"]) == "ATIVO"


def test_quem_sai_HOJE_ainda_bate_hoje(sessao_real, gente):
    """A pessoa trabalha o último dia — e a batida desse dia é o que a rescisão
    usa. Trocar a régua para `<=` tiraria justamente esse dia."""
    assert _situacao_no_ponto(sessao_real, gente["ids"]["ultimo_dia_hoje"]) == "ATIVO"


def test_quem_saiu_ontem_nao_bate_mais(sessao_real, gente):
    """A outra metade: o conserto não pode deixar entrar quem já saiu."""
    assert _situacao_no_ponto(sessao_real, gente["ids"]["saiu_mesmo"]) == "DESLIGADO"


def test_quem_nao_tem_data_nenhuma_segue_pela_caixinha(sessao_real, gente):
    """Sem data não há o que comparar: vale o que o cadastro diz."""
    assert _situacao_no_ponto(sessao_real, gente["ids"]["trabalhando"]) == "ATIVO"


def test_a_lista_de_quem_bate_ponto_traz_quem_esta_em_aviso_previo(sessao_real, gente):
    """É esta lista que a tela do ponto e o aplicativo usam."""
    from app.apps.ponto.core import cadastros
    ativos = {int(p["id"]) for p in cadastros.listar_colaboradores(
        sessao_real.connection(), so_ativos=True, obra_id=gente["obra"])}

    assert gente["ids"]["aviso_previo"] in ativos
    assert gente["ids"]["ultimo_dia_hoje"] in ativos
    assert gente["ids"]["trabalhando"] in ativos
    assert gente["ids"]["saiu_mesmo"] not in ativos


def test_a_migracao_109_arruma_a_caixinha_de_quem_ainda_trabalha(sessao_real, gente):
    """A migração conserta o dado; o CASE acima conserta a leitura. As duas
    coisas existem porque o ERP também lê essa coluna — nas listas do Pessoal e
    na folha —, e lá não há CASE nenhum."""
    from pathlib import Path
    sql = (Path(__file__).resolve().parents[1] / "app" / "apps" / "erp"
           / "scripts" / "migracoes"
           / "109_saida_agendada_nao_desliga.sql").read_text(encoding="utf-8")
    sessao_real.execute(text(sql))
    sessao_real.flush()

    def caixinha(chave):
        return sessao_real.execute(
            text("SELECT situacao, etapa FROM colaboradores WHERE id = :i"),
            {"i": gente["ids"][chave]}).mappings().one()

    assert caixinha("aviso_previo")["situacao"] == "ATIVO"
    assert caixinha("ultimo_dia_hoje")["situacao"] == "ATIVO"
    # quem saiu de verdade NÃO é tocado
    assert caixinha("saiu_mesmo")["situacao"] == "DESLIGADO"
    # e ninguém fica na coluna "Desligado" do quadro estando na ativa
    assert caixinha("aviso_previo")["etapa"] != "DESLIGADO"


def test_a_migracao_109_deixa_rastro_de_cada_pessoa_que_mexeu(sessao_real, gente):
    """Sem rastro, uma correção em lote é indistinguível de um acidente."""
    from pathlib import Path
    sql = (Path(__file__).resolve().parents[1] / "app" / "apps" / "erp"
           / "scripts" / "migracoes"
           / "109_saida_agendada_nao_desliga.sql").read_text(encoding="utf-8")
    sessao_real.execute(text(sql))
    sessao_real.flush()

    linhas = sessao_real.execute(text("""
        SELECT entidade_id, detalhe FROM eventos
         WHERE acao = 'SITUACAO_CORRIGIDA' AND entidade_tipo = 'colaborador'
      ORDER BY id""")).mappings().all()
    mexidos = {int(l["entidade_id"]) for l in linhas}

    assert gente["ids"]["aviso_previo"] in mexidos
    assert gente["ids"]["saiu_mesmo"] not in mexidos
    for l in linhas:
        assert l["detalhe"]["de"] == "DESLIGADO" and l["detalhe"]["para"] == "ATIVO"
        assert l["detalhe"]["data_saida"]            # a data que motivou
