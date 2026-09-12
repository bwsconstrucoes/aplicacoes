"""O teto de gasto com IA de cada pessoa.

Decisão do dono em 12/09/2026: *"pra gente não ter surpresa, vamos limitar aí.
Deve ficar no cadastro da pessoa, com o valor estimado já de cinco dólares. E
se eu quiser colocar diferente pra outras pessoas (…) que seja editável. Se eu
quiser colocar alguém sem limite, eu coloco, ou botar dez dólares"*.

O raciocínio dele, que explica o valor baixo: *"isso é mais é gestão que vai
usar, pessoal de obra eu não acredito que vai usar muito"*. Cinco dólares
seguram a curiosidade de quem experimenta; quem precisa de mais recebe mais,
um a um.

DIFERENÇA PARA O TETO GLOBAL, e é o ponto: o global AVISA os administradores e
deixa passar — é termômetro. Este BARRA. Teto que só avisa vira aviso que chega
depois da fatura, e o pedido foi "não ter surpresa".

COM BANCO DE VERDADE porque o gasto é somado no `WHERE` e o teto é lido por SQL
direto (e não pelo ORM, para não derrubar o ERP na janela entre publicar e
apertar o botão do banco).
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum import ia_custo
from app.apps.erp.db.models.cadastros import PerfilUsuario as P, Usuario
from app.apps.erp.db.models.financeiro import IaUso

pytestmark = pytest.mark.banco


@pytest.fixture
def gente(sessao_real):
    s = sessao_real

    def pessoa(nome, email, teto):
        u = Usuario(nome=nome, email=email,
                    senha_hash=gerar_hash("senha-de-teste-1234"),
                    perfil=P.GESTOR_OBRA, teto_ia_usd=teto)
        s.add(u)
        s.flush()
        return u

    return {
        "s": s,
        "padrao": pessoa("Gestor comum", "comum@teste.bws.local", Decimal("5.00")),
        "folgado": pessoa("Quem usa muito", "muito@teste.bws.local", Decimal("10.00")),
        "sem_limite": pessoa("Marcelo", "chefe@teste.bws.local", None),
    }


def _gastar(s, usuario, valor):
    s.add(IaUso(modelo="gpt-4o-mini", operacao="teste",
                tokens_entrada=100, tokens_saida=50,
                custo_usd=Decimal(str(valor)), sucesso=True,
                usuario_id=usuario.id,
                criado_em=datetime.now(timezone.utc)))
    s.flush()


# ---------------------------------------------------------------------------
# O padrão, e o que ele significa
# ---------------------------------------------------------------------------
def test_o_padrao_combinado_e_cinco_dolares():
    assert ia_custo.TETO_PESSOA_PADRAO == Decimal("5.00")


def test_quem_esta_dentro_do_teto_passa(gente):
    d = gente
    _gastar(d["s"], d["padrao"], "1.50")
    ia_custo.exigir_saldo_de_ia(d["s"], d["padrao"].id)      # não levanta


def test_quem_estourou_o_teto_e_barrado(gente):
    d = gente
    _gastar(d["s"], d["padrao"], "5.00")
    with pytest.raises(ia_custo.SemSaldoDeIa):
        ia_custo.exigir_saldo_de_ia(d["s"], d["padrao"].id)


def test_o_recado_diz_o_que_continua_funcionando_e_a_quem_pedir(gente):
    """Quem lê não é programador: precisa saber que o ERP não quebrou e o que
    fazer para destravar."""
    d = gente
    _gastar(d["s"], d["padrao"], "7.00")
    with pytest.raises(ia_custo.SemSaldoDeIa) as erro:
        ia_custo.exigir_saldo_de_ia(d["s"], d["padrao"].id)
    texto = str(erro.value)
    assert "relatórios" in texto and "seguem normais" in texto
    assert "administrador" in texto
    assert "US$ 5.00" in texto


def test_o_teto_e_por_pessoa_e_nao_conta_o_gasto_dos_outros(gente):
    """Se somasse o gasto de todo mundo, o primeiro a usar travaria o resto."""
    d = gente
    _gastar(d["s"], d["folgado"], "9.00")
    ia_custo.exigir_saldo_de_ia(d["s"], d["padrao"].id)      # não levanta
    assert ia_custo.situacao_da_pessoa(d["s"], d["padrao"].id)["gasto"] == 0.0


def test_quem_tem_teto_maior_vai_mais_longe(gente):
    d = gente
    _gastar(d["s"], d["folgado"], "7.00")
    ia_custo.exigir_saldo_de_ia(d["s"], d["folgado"].id)     # 7 < 10, passa
    _gastar(d["s"], d["folgado"], "3.00")
    with pytest.raises(ia_custo.SemSaldoDeIa):
        ia_custo.exigir_saldo_de_ia(d["s"], d["folgado"].id)


def test_sem_limite_nunca_barra(gente):
    """O dono pediu poder deixar alguém sem limite. Vazio no cadastro é
    ESCOLHA, e a tela escreve isso."""
    d = gente
    _gastar(d["s"], d["sem_limite"], "500.00")
    ia_custo.exigir_saldo_de_ia(d["s"], d["sem_limite"].id)
    assert ia_custo.situacao_da_pessoa(d["s"], d["sem_limite"].id)["teto"] is None


def test_conta_do_sistema_nao_tem_teto(gente):
    """Robô, relatório agendado e agente não são curiosidade de ninguém —
    travá-los quebraria rotina sem ninguém entender por quê."""
    ia_custo.exigir_saldo_de_ia(gente["s"], None)


def test_avisa_aos_oitenta_por_cento_antes_de_barrar(gente):
    d = gente
    _gastar(d["s"], d["padrao"], "4.00")                     # 80% de 5
    sit = ia_custo.situacao_da_pessoa(d["s"], d["padrao"].id)
    assert sit["alerta"] == "AVISO"
    assert sit["restante"] == 1.0
    ia_custo.exigir_saldo_de_ia(d["s"], d["padrao"].id)      # avisa, não barra


def test_o_gasto_do_mes_passado_nao_conta(gente):
    """O teto é MENSAL: vira o mês, a pessoa volta a poder usar."""
    from datetime import timedelta

    d = gente
    antigo = IaUso(modelo="gpt-4o-mini", operacao="teste", custo_usd=Decimal("9.00"),
                   sucesso=True, usuario_id=d["padrao"].id,
                   criado_em=datetime.now(timezone.utc) - timedelta(days=45))
    d["s"].add(antigo)
    d["s"].flush()
    ia_custo.exigir_saldo_de_ia(d["s"], d["padrao"].id)


# ---------------------------------------------------------------------------
# A varredura que impede a nona rota de esquecer a trava
# ---------------------------------------------------------------------------
def test_toda_rota_que_gasta_ia_confere_o_teto():
    """São oito rotas hoje. A nona é a que alguém esquece — por isso a
    conferência é estrutural, lendo o próprio código das rotas."""
    import ast
    import pathlib

    raiz = pathlib.Path(__file__).resolve().parents[1]
    arv = ast.parse((raiz / "app" / "apps" / "erp" / "routes.py").read_text(
        encoding="utf-8"))
    # o que caracteriza "esta rota gasta IA"
    GASTA = ("ler_documento(", "transcrever(", "leitura.sugerir(",
             "ler_contrato(", "contexto(operacao=", "ia_custo.contexto(")
    sem_trava = []
    for no in arv.body:
        if not isinstance(no, ast.FunctionDef):
            continue
        decs = [ast.unparse(x) for x in no.decorator_list]
        if not any(x.startswith("bp.route") for x in decs):
            continue
        corpo = ast.unparse(no)
        if any(g in corpo for g in GASTA) and "_exigir_saldo_de_ia" not in corpo:
            sem_trava.append(no.name)
    assert not sem_trava, (
        f"estas rotas gastam IA sem conferir o teto do mês da pessoa: "
        f"{sem_trava}. Chame `_exigir_saldo_de_ia()` — FORA do `try`, senão o "
        f"`except Exception` transforma a recusa em 'falha do sistema'.")


def test_a_trava_fica_fora_do_try():
    """Dentro do `try`, um `except Exception` engoliria a recusa e ela viraria
    500 — a pessoa veria "falha do sistema" em vez de "seu limite acabou"."""
    import ast
    import pathlib

    raiz = pathlib.Path(__file__).resolve().parents[1]
    arv = ast.parse((raiz / "app" / "apps" / "erp" / "routes.py").read_text(
        encoding="utf-8"))
    dentro = []
    for no in ast.walk(arv):
        if not isinstance(no, ast.FunctionDef):
            continue
        for t in ast.walk(no):
            if isinstance(t, ast.Try) and "_exigir_saldo_de_ia()" in ast.unparse(t):
                dentro.append(no.name)
                break
    assert not dentro, f"a trava do teto está dentro de um try em: {dentro}"


# ---------------------------------------------------------------------------
# O cadastro do operador: o padrão e a escolha de "sem limite"
# ---------------------------------------------------------------------------
def test_operador_novo_nasce_com_os_cinco_dolares(sessao_real):
    from app.apps.erp.core.auth.service import criar_usuario

    s = sessao_real
    admin = Usuario(nome="Marcelo", email="admin-teto@teste.bws.local",
                    senha_hash=gerar_hash("senha-de-teste-1234"), perfil=P.ADMIN)
    s.add(admin)
    s.flush()

    novo = criar_usuario(s, nome="Gente nova", email="novo@teste.bws.local",
                         senha="senha-de-teste-1234", perfil="GESTOR_OBRA",
                         criado_por=admin)
    s.flush()
    assert novo.teto_ia_usd == Decimal("5.00")


def test_apagar_o_campo_deixa_a_pessoa_SEM_limite(sessao_real):
    """A armadilha que um teste pegou em 12/09/2026: com padrão no modelo, o
    SQLAlchemy omitia a coluna do INSERT quando nula e gravava 5,00 do mesmo
    jeito — a escolha de quem apagou o campo era desfeita em silêncio."""
    from app.apps.erp.core.auth.service import criar_usuario

    s = sessao_real
    admin = Usuario(nome="Marcelo", email="admin-teto2@teste.bws.local",
                    senha_hash=gerar_hash("senha-de-teste-1234"), perfil=P.ADMIN)
    s.add(admin)
    s.flush()
    u = criar_usuario(s, nome="Sem limite", email="livre@teste.bws.local",
                      senha="senha-de-teste-1234", perfil="ADMIN",
                      criado_por=admin)
    s.flush()

    u.teto_ia_usd = None                      # é o que a tela faz ao salvar vazio
    s.flush()
    s.expire(u)

    assert ia_custo.teto_da_pessoa(s, u.id) is None
    _gastar(s, u, "999.00")
    ia_custo.exigir_saldo_de_ia(s, u.id)      # não levanta
