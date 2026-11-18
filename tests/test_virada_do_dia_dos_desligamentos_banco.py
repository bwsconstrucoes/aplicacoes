# -*- coding: utf-8 -*-
"""
NO DIA DA SAÍDA, TUDO CAI SOZINHO — contra um Postgres de verdade.

⚑ POR QUE ESTE ARQUIVO EXISTE. Em 17/11/2026, depois de o conserto do aviso
prévio deixar de desligar na hora quem tem saída marcada para o futuro, ficou a
pergunta de quem corta o acesso no dia. Ele decidiu:

> *"Não, o ideal é cortar tudo, porque aí corre o risco de a gente esquecer.
> Então, se ela precisar de um contracheque, alguma coisa, ela solicita ao DP e
> o DP manda."*

A metade que faltava: o PONTO se resolve sozinho (compara a data com hoje), mas
o login no ERP e no portal, as mensagens da empresa e as listas do Pessoal leem
a CAIXINHA `situacao` — e nada a virava quando o dia chegava.

⚠️ **ISTO NÃO É O SISTEMA DECIDINDO DESLIGAR NINGUÉM.** Uma pessoa do DP já
registrou o desligamento, com data. A rotina só cumpre, no dia, o que foi
autorizado antes — e é por isso que ela não viola a regra nº 1 do agente de DP
("não desliga").

Nada disto é alcançável pelo dublê de sessão do `conftest.py`: tudo mora em
UPDATE com WHERE de data.
"""
from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import text

from app.apps.erp.core.cadastros import desligamentos_do_dia as svc

pytestmark = pytest.mark.banco

HOJE = dt.date.today()
ONTEM = HOJE - dt.timedelta(days=1)
SEMANA_QUE_VEM = HOJE + dt.timedelta(days=7)


@pytest.fixture(autouse=True)
def _esquecer():
    """A marca em memória não pode atravessar testes."""
    svc.esquecer()
    yield
    svc.esquecer()


@pytest.fixture
def cena(sessao_real):
    """Cinco casos, e os cinco têm de ser tratados diferente."""
    s = sessao_real
    from app.apps.erp.core.auth.service import gerar_hash

    def pessoa(nome, cpf, demissao, situacao="ATIVO", etapa="CONCLUIDO",
               com_login=False, ultimo_dia=None):
        uid = None
        if com_login:
            uid = s.execute(text("""
                INSERT INTO usuarios (nome, email, senha_hash, perfil, ativo)
                VALUES (:n, :e, :h, 'DEPARTAMENTO_PESSOAL', TRUE)
                RETURNING id"""),
                {"n": nome, "e": f"{cpf}@virada.teste",
                 "h": gerar_hash("senha-de-teste-123")}).scalar()
        cid = s.execute(text("""
            INSERT INTO colaboradores (nome, cpf, situacao, etapa, demissao,
                                       ultimo_dia, admissao, usuario_id)
            VALUES (:n, :c, :s, :e, :d, :u, '2026-01-05', :uid) RETURNING id"""),
            {"n": nome, "c": cpf, "s": situacao, "e": etapa, "d": demissao,
             "u": ultimo_dia, "uid": uid}).scalar()
        return {"id": cid, "usuario_id": uid}

    cena = {
        "saiu_ontem":     pessoa("Saiu Ontem", "52998224725", ONTEM, com_login=True),
        "sai_hoje":       pessoa("Sai Hoje", "11144477735", HOJE, com_login=True),
        "aviso_previo":   pessoa("Em Aviso Previo", "12345678909", SEMANA_QUE_VEM),
        "cancelado":      pessoa("Cadastro Cancelado", "98765432100", ONTEM,
                                 etapa="CANCELADO"),
        "so_ultimo_dia":  pessoa("So Ultimo Dia", "15350946056", None,
                                 ultimo_dia=ONTEM),
    }
    s.flush()
    return cena


def _caixinha(sessao_real, cid):
    return sessao_real.execute(
        text("SELECT situacao, etapa FROM colaboradores WHERE id = :i"),
        {"i": cid}).mappings().one()


def test_quem_saiu_ontem_e_desligado_e_perde_o_login(sessao_real, cena):
    """⚠️ O TESTE PRINCIPAL. É a decisão dele: corta tudo."""
    feito = svc.aplicar(sessao_real)
    sessao_real.flush()

    c = _caixinha(sessao_real, cena["saiu_ontem"]["id"])
    assert c["situacao"] == "DESLIGADO" and c["etapa"] == "DESLIGADO"
    ativo = sessao_real.execute(text("SELECT ativo FROM usuarios WHERE id = :i"),
                                {"i": cena["saiu_ontem"]["usuario_id"]}).scalar()
    assert ativo is False, "o login de quem saiu continuou valendo"
    assert feito["pessoas"] >= 1 and feito["acessos"] >= 1


def test_quem_sai_HOJE_nao_e_tocado(sessao_real, cena):
    """A pessoa trabalha o último dia: hoje ela ainda bate ponto, ainda recebe
    aviso e ainda entra no sistema."""
    svc.aplicar(sessao_real)
    sessao_real.flush()

    assert _caixinha(sessao_real, cena["sai_hoje"]["id"])["situacao"] == "ATIVO"
    ativo = sessao_real.execute(text("SELECT ativo FROM usuarios WHERE id = :i"),
                                {"i": cena["sai_hoje"]["usuario_id"]}).scalar()
    assert ativo is True


def test_quem_esta_em_aviso_previo_nao_e_tocado(sessao_real, cena):
    svc.aplicar(sessao_real)
    sessao_real.flush()
    assert _caixinha(sessao_real, cena["aviso_previo"]["id"])["situacao"] == "ATIVO"


def test_cadastro_CANCELADO_nao_e_tocado(sessao_real, cena):
    """Cancelado é cadastro que nunca virou gente na empresa — não se desliga
    quem nunca entrou."""
    svc.aplicar(sessao_real)
    sessao_real.flush()
    assert _caixinha(sessao_real, cena["cancelado"]["id"])["etapa"] == "CANCELADO"


def test_ULTIMO_DIA_sozinho_NUNCA_desliga(sessao_real, cena):
    """⚠️ Ele perguntou exatamente isto. "Último dia trabalhado" e "aviso
    prévio" são informação para a rescisão; quem decide é a DATA DE SAÍDA."""
    svc.aplicar(sessao_real)
    sessao_real.flush()
    assert _caixinha(sessao_real, cena["so_ultimo_dia"]["id"])["situacao"] == "ATIVO"


def test_a_trilha_diz_por_que_a_pessoa_foi_desligada(sessao_real, cena):
    """Desligamento automático sem rastro é indistinguível de acidente."""
    svc.aplicar(sessao_real)
    sessao_real.flush()

    linha = sessao_real.execute(text("""
        SELECT detalhe FROM eventos
         WHERE entidade_tipo = 'colaborador' AND entidade_id = :i
           AND acao = 'DESLIGADO' ORDER BY id DESC LIMIT 1"""),
        {"i": cena["saiu_ontem"]["id"]}).scalar()
    assert linha and "data de saída" in linha["por"]
    assert linha["data_saida"] == str(ONTEM)

    # e a mudança de etapa aparece no histórico da ficha
    etapas = sessao_real.execute(text("""
        SELECT para, motivo FROM colaborador_etapas
         WHERE colaborador_id = :i ORDER BY id DESC LIMIT 1"""),
        {"i": cena["saiu_ontem"]["id"]}).mappings().first()
    assert etapas and etapas["para"] == "DESLIGADO"
    assert "data de saída" in (etapas["motivo"] or "")


def test_rodar_de_novo_no_mesmo_dia_nao_mexe_em_nada(sessao_real, cena):
    """Ela é chamada em TODA requisição do ERP. Se não fosse idempotente,
    escreveria trilha repetida o dia inteiro."""
    primeira = svc.aplicar(sessao_real)
    sessao_real.flush()
    segunda = svc.aplicar(sessao_real)
    sessao_real.flush()

    assert primeira["pessoas"] >= 1
    assert segunda["pessoas"] == 0, "desligou de novo quem já estava desligado"


def test_a_PRIMEIRA_requisicao_do_dia_ja_desliga_e_ja_recusa(app_real, sessao_real, cena):
    """⚠️ A ORDEM IMPORTA, e este teste é a prova dela.

    A virada está pendurada ANTES da guarda de permissão. Então, mesmo que a
    primeira pessoa a tocar o ERP no dia seja justamente quem saiu, a própria
    requisição dela desliga o cadastro e a guarda a recusa em seguida —
    nenhuma última entrada. Pendurada depois da guarda, ela entraria uma vez.
    """
    from tests.conftest import como
    cliente = como(app_real, cena["saiu_ontem"]["usuario_id"])

    r = cliente.get("/erp/api/colaboradores")

    assert r.status_code == 401, "quem saiu entrou uma última vez"
    assert "DP" in r.get_json()["erro"]
    assert _caixinha(sessao_real, cena["saiu_ontem"]["id"])["situacao"] == "DESLIGADO"


def test_a_sessao_aberta_de_quem_perdeu_o_acesso_cai_no_clique_seguinte(
        app_real, sessao_real, cena):
    """Antes, `usuarios.ativo` era conferido SÓ no login: quem já estava com a
    tela aberta seguia trabalhando depois de o acesso ser cortado — e isso vale
    para o desligamento E para o bloqueio feito à mão pelo DP.

    Usa quem SAI HOJE (que a virada não toca) para medir só a guarda.
    """
    from tests.conftest import como
    uid = cena["sai_hoje"]["usuario_id"]
    cliente = como(app_real, uid)

    # com o acesso valendo, a tela responde
    assert cliente.get("/erp/api/colaboradores").status_code in (200, 403)

    sessao_real.execute(text("UPDATE usuarios SET ativo = FALSE WHERE id = :i"),
                        {"i": uid})
    sessao_real.flush()

    r = cliente.get("/erp/api/colaboradores")
    assert r.status_code == 401, "a sessão aberta continuou valendo"
    assert "DP" in r.get_json()["erro"]


# ---------------------------------------------------------------------------
# A JANELA DA MANHÃ: o aviso da empresa sai pelo PONTO, antes de alguém abrir
# o ERP — então a virada ainda não rodou. A mensageria tem de olhar a data.
# ---------------------------------------------------------------------------
def test_o_aviso_da_empresa_nao_vai_para_quem_saiu_mesmo_antes_da_virada(
        sessao_real, cena):
    """⚑ O resumo da manhã é disparado pela primeira batida depois das 6h —
    quase sempre ANTES de alguém do escritório abrir o ERP. Nessa janela a
    caixinha ainda diz ATIVO, e quem saiu ontem receberia o aviso."""
    from app.apps.mensageria import telegram_cadastro as tc
    conn = sessao_real.connection()

    # a caixinha AINDA diz ATIVO (a virada não rodou)
    assert _caixinha(sessao_real, cena["saiu_ontem"]["id"])["situacao"] == "ATIVO"

    quem = tc.desligado_da_empresa(conn, cpf="52998224725")
    assert quem is not None, "o aviso da empresa iria para quem saiu ontem"
    assert quem["demissao"] == str(ONTEM)


def test_o_aviso_da_empresa_CONTINUA_indo_para_quem_sai_hoje(sessao_real, cena):
    """A régua é a mesma de todo o resto: quem sai hoje ainda trabalha hoje, e
    os avisos do dia são justamente os que ela precisa receber."""
    from app.apps.mensageria import telegram_cadastro as tc
    conn = sessao_real.connection()

    assert tc.desligado_da_empresa(conn, cpf="11144477735") is None
