"""CRIAR O PRÓPRIO ACESSO PELO PORTAL — com banco de verdade.

Desenho dele em 18/11/2026, ao tentar orientar a equipe e esbarrar no absurdo:

> *"Então tá errado esse acesso ser liberado pelo aplicativo de ponto. Qual o
> sentido que tem de uma pessoa que vai lançar um diário ter que ir lá no
> aplicativo do ponto? (…) O cara tem que ir pro portal e criar o acesso dele
> por lá."*

E a trava que ele exigiu junto: *"ainda assim precisa ser validado por alguém
no sistema. (…) aí aumenta o nível de segurança."*

POR QUE `@pytest.mark.banco`: tudo aqui é busca e estado — o CPF casa com UMA
pessoa, o código é o ÚLTIMO não usado daquela pessoa, o teto conta os da
última hora. A `SessaoFalsa` ignora `WHERE` e aprovaria cada uma dessas coisas
errada, inclusive a pior: conferir o código de outra pessoa.

Cobre, nesta ordem:
  1. a resposta é SEMPRE a mesma — CPF que não existe, telefone que não bate,
     pessoa desligada, quem já tem acesso;
  2. telefone com formato diferente é a MESMA linha (é o caso real);
  3. telefone que não bate não manda código nenhum;
  4. o código confirma e o pedido entra NA FILA, marcado;
  5. ⚠️ e NÃO cria acesso — quem cria é quem aprova;
  6. código errado conta tentativa e trava em cinco;
  7. código serve UMA vez;
  8. código de uma pessoa não vale para outra;
  9. teto de três por hora.
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

from app.apps.erp.core.auth import primeiro_acesso_portal as svc
from app.apps.erp.db.models.cadastros import Colaborador, Obra

pytestmark = pytest.mark.banco

CPF = "11144477735"
OUTRO_CPF = "52998224725"


@pytest.fixture(autouse=True)
def _nao_manda_whatsapp(monkeypatch):
    """O envio é de verdade e fala com serviço de fora. Aqui ele é anotado."""
    mandados = []
    monkeypatch.setattr(svc, "_mandar",
                        lambda tel, texto: mandados.append((tel, texto)))
    monkeypatch.setattr(svc, "_registrar_nao_enviado",
                        lambda *a, **k: None)
    return mandados


@pytest.fixture
def mandados(_nao_manda_whatsapp):
    return _nao_manda_whatsapp


@pytest.fixture
def pessoa(sessao_real):
    o = Obra(codigo="OBRPA", nome="Obra do Primeiro Acesso", status="ATIVA")
    sessao_real.add(o)
    sessao_real.flush()
    c = Colaborador(nome="JOSE DA OBRA", cpf=CPF, situacao="ATIVO",
                    obra_id=o.id, telefone="(81) 98888-7777")
    sessao_real.add(c)
    sessao_real.flush()
    return c


def _codigo_de(mandados: list) -> str:
    import re
    return re.search(r"\b(\d{6})\b", mandados[-1][1]).group(1)


# ---------------------------------------------------------------------------
# 1. A resposta é sempre a mesma
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("cpf, telefone", [
    ("39053344705", "81988887777"),      # CPF que não está na base
    (CPF, "81900000000"),                # telefone que não bate
])
def test_a_resposta_e_a_mesma_de_sucesso(sessao_real, pessoa, cpf, telefone):
    """Dizer "este CPF não existe" faria a tela virar conferidor de CPF, e
    dizer "o telefone não bate" confirmaria que aquele CPF está na base."""
    r = svc.pedir_codigo(sessao_real, cpf, telefone)

    assert r["ok"] is True
    assert r["mensagem"] == svc.RESPOSTA


def test_quem_ja_tem_acesso_nao_recebe_codigo(sessao_real, pessoa, mandados):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario

    u = Usuario(nome="Jose", email="jose@teste.com", ativo=True,
                senha_hash=gerar_hash("x" * 12),
                perfil=PerfilUsuario.ADMINISTRATIVO_OBRA)
    sessao_real.add(u)
    sessao_real.flush()
    pessoa.usuario_id = u.id
    sessao_real.flush()

    r = svc.pedir_codigo(sessao_real, CPF, "81988887777")

    assert r["mensagem"] == svc.RESPOSTA
    assert mandados == [], "quem já tem acesso faz “esqueci a senha”"


def test_desligado_nao_recebe_codigo(sessao_real, pessoa, mandados):
    pessoa.situacao = "DESLIGADO"
    sessao_real.flush()

    svc.pedir_codigo(sessao_real, CPF, "81988887777")

    assert mandados == []


def test_cpf_com_tamanho_errado_e_o_unico_erro_que_a_tela_conta(sessao_real):
    """Não revela nada: um CPF de dez dígitos está errado para qualquer um."""
    r = svc.pedir_codigo(sessao_real, "123", "81988887777")
    assert r["ok"] is False and "11 números" in r["erro"]


# ---------------------------------------------------------------------------
# 2 e 3. O telefone CONFERE — e o código vai para o do cadastro
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("digitado", [
    "81988887777", "(81) 98888-7777", "81 98888 7777", "5581988887777",
    "8188887777",                      # sem o nono dígito — acontece muito
])
def test_o_mesmo_telefone_em_formatos_diferentes_bate(sessao_real, pessoa,
                                                      mandados, digitado):
    svc.pedir_codigo(sessao_real, CPF, digitado)

    assert mandados, f"“{digitado}” é a mesma linha do cadastro e tem de bater"
    assert mandados[-1][0] == "5581988887777", (
        "o código vai para o telefone DO CADASTRO, não para o digitado")


def test_telefone_de_outra_linha_nao_manda_nada(sessao_real, pessoa, mandados):
    """É a trava contra quem sabe o CPF de alguém e põe o próprio telefone."""
    svc.pedir_codigo(sessao_real, CPF, "81 97777-1234")

    assert mandados == []


# ---------------------------------------------------------------------------
# 4 e 5. Confirma, entra na fila — e NÃO cria acesso
# ---------------------------------------------------------------------------
def test_o_codigo_certo_poe_o_pedido_na_fila_marcado(sessao_real, pessoa,
                                                     mandados):
    from app.apps.erp.core.auth import pedidos_de_portal as fila

    svc.pedir_codigo(sessao_real, CPF, "81988887777")
    r = svc.concluir(sessao_real, CPF, _codigo_de(mandados), "jose@gmail.com")

    assert r["ok"] is True
    [p] = fila.listar(sessao_real.connection())
    assert p["email"] == "jose@gmail.com"
    assert p["origem"] == "PORTAL"
    assert p["identidade_confirmada"] is True, (
        "quem aprova precisa ver que a identidade foi conferida")
    assert p["obra_id"] == pessoa.obra_id, "a obra vem do cadastro"


def test_concluir_NAO_cria_acesso(sessao_real, pessoa, mandados):
    """⚠️ A trava que ele exigiu: *"ainda assim precisa ser validado por
    alguém no sistema."* Sem isto, qualquer um do cadastro se liberaria
    sozinho — e o acesso nasce podendo LANÇAR DIÁRIO."""
    from app.apps.erp.db.models.cadastros import Usuario

    svc.pedir_codigo(sessao_real, CPF, "81988887777")
    svc.concluir(sessao_real, CPF, _codigo_de(mandados), "jose@gmail.com")

    assert sessao_real.get(Colaborador, pessoa.id).usuario_id is None
    assert sessao_real.query(Usuario).filter(
        Usuario.email == "jose@gmail.com").first() is None


def test_email_torto_e_recusado(sessao_real, pessoa, mandados):
    svc.pedir_codigo(sessao_real, CPF, "81988887777")
    with pytest.raises(svc.CodigoRecusado) as e:
        svc.concluir(sessao_real, CPF, _codigo_de(mandados), "jose arroba")
    assert "@" in str(e.value)


# ---------------------------------------------------------------------------
# 6 a 9. O código se defende
# ---------------------------------------------------------------------------
def test_codigo_errado_conta_tentativa_e_trava_em_cinco(sessao_real, pessoa,
                                                        mandados):
    """Seis dígitos são um milhão de combinações — pouco para uma máquina."""
    svc.pedir_codigo(sessao_real, CPF, "81988887777")
    certo = _codigo_de(mandados)
    errado = "000000" if certo != "000000" else "111111"

    for _ in range(svc.TENTATIVAS):
        with pytest.raises(svc.CodigoRecusado):
            svc.concluir(sessao_real, CPF, errado, "jose@gmail.com")

    # … e agora nem o CERTO passa mais
    with pytest.raises(svc.CodigoRecusado) as e:
        svc.concluir(sessao_real, CPF, certo, "jose@gmail.com")
    assert "novo" in str(e.value).lower()


def test_o_codigo_serve_uma_vez(sessao_real, pessoa, mandados):
    svc.pedir_codigo(sessao_real, CPF, "81988887777")
    codigo = _codigo_de(mandados)
    svc.concluir(sessao_real, CPF, codigo, "jose@gmail.com")

    with pytest.raises(svc.CodigoRecusado):
        svc.concluir(sessao_real, CPF, codigo, "outro@gmail.com")


def test_o_codigo_de_um_nao_vale_para_outro(sessao_real, pessoa, mandados):
    """O pior erro possível aqui: conferir o código contra a pessoa errada."""
    outra = Colaborador(nome="MARIA DA OBRA", cpf=OUTRO_CPF, situacao="ATIVO",
                        telefone="81 97777-1234", obra_id=pessoa.obra_id)
    sessao_real.add(outra)
    sessao_real.flush()

    svc.pedir_codigo(sessao_real, CPF, "81988887777")
    do_jose = _codigo_de(mandados)

    with pytest.raises(svc.CodigoRecusado):
        svc.concluir(sessao_real, OUTRO_CPF, do_jose, "maria@gmail.com")


def test_teto_de_tres_codigos_por_hora(sessao_real, pessoa, mandados):
    for _ in range(svc.CODIGOS_POR_HORA):
        svc.pedir_codigo(sessao_real, CPF, "81988887777")
    quantos = len(mandados)

    svc.pedir_codigo(sessao_real, CPF, "81988887777")

    assert len(mandados) == quantos, "o quarto código da hora não sai"


def test_codigo_vencido_e_recusado(sessao_real, pessoa, mandados):
    svc.pedir_codigo(sessao_real, CPF, "81988887777")
    sessao_real.execute(text(
        "UPDATE portal_codigos_de_acesso SET expira_em = now() - interval '1 min'"))
    sessao_real.flush()

    with pytest.raises(svc.CodigoRecusado) as e:
        svc.concluir(sessao_real, CPF, _codigo_de(mandados), "jose@gmail.com")
    assert "venceu" in str(e.value)
