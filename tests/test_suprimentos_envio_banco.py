"""Disparar a cotação, contra Postgres de verdade.

O caminho inteiro só se prova aqui: obra → empresa → cotação → e-mail por
fornecedor → registro. A sessão dublada ignora `WHERE`, e este fluxo é todo
feito de "os itens DESTA cotação", "as obras DESTES itens", "os contatos
DESTE fornecedor".

O que não pode falhar:
  - a cotação sai pela empresa da obra, não por outra;
  - obras de empresas diferentes na mesma cotação fazem o sistema PERGUNTAR,
    nunca sortear;
  - um fornecedor fora do ar não impede os outros de receberem;
  - fornecedor sem e-mail é relatado, não ignorado;
  - o corpo do e-mail traz os itens e NÃO traz preço de ninguém.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.cadastros import empresas as svc_emp
from app.apps.erp.core.comum import email as correio
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.suprimentos import cotacao as svc_cot
from app.apps.erp.core.suprimentos import envio as svc
from app.apps.erp.db.models.cadastros import (
    Fornecedor, FornecedorContato, Insumo, InsumoCategoria, Obra,
    PerfilUsuario as P, RegimeTributario, SuprimentoItem, SuprimentoSolicitacao,
    TipoPessoa, Usuario,
)

pytestmark = pytest.mark.banco

CNPJ_A = "71000001000184"
CNPJ_B = "71000002000129"


@pytest.fixture
def chave(monkeypatch):
    from cryptography.fernet import Fernet
    monkeypatch.setenv("ERP_CHAVE_SEGREDOS", Fernet.generate_key().decode())


@pytest.fixture
def correio_mudo(monkeypatch):
    """Nenhum teste manda e-mail de verdade. Guarda o que teria saído."""
    saidas = []
    monkeypatch.setattr(correio, "_entregar",
                        lambda empresa, msg, destinos: saidas.append(
                            {"de": msg["From"], "para": destinos,
                             "assunto": msg["Subject"],
                             "corpo": msg.get_content()}))
    return saidas


def _usuario(s, email="comprador@teste.bws.local"):
    u = Usuario(nome="Comprador de teste", email=email,
                senha_hash=gerar_hash("senha-de-teste-1234"), perfil=P.ADMIN)
    s.add(u)
    s.flush()
    return u


def _empresa(s, usuario, cnpj, razao, **extra):
    e = svc_emp.criar(s, {"razao_social": razao, "cnpj": cnpj,
                          "nome_fantasia": razao.split()[0]}, usuario)
    s.flush()
    svc_emp.definir_conta_de_email(s, e.id, {
        "smtp_servidor": "smtp.exemplo.com", "smtp_porta": 587,
        "smtp_usuario": f"compras@{razao.split()[0].lower()}.exemplo",
        "smtp_senha": "senha-de-aplicativo", **extra}, usuario)
    s.flush()
    return e


def _obra(s, codigo, empresa_id=None):
    o = Obra(codigo=codigo, nome=f"Obra {codigo}", status="ATIVA",
             empresa_id=empresa_id)
    s.add(o)
    s.flush()
    return o


def _insumo(s, descricao="Cimento CP-II 50kg"):
    cat = InsumoCategoria(codigo=f"C{abs(hash(descricao)) % 9999:04d}",
                          nome=f"Categoria de {descricao}")
    s.add(cat)
    s.flush()
    i = Insumo(codigo=f"INS-{abs(hash(descricao)) % 9999:04d}",
               descricao=descricao, unidade="SC", categoria_insumo_id=cat.id)
    s.add(i)
    s.flush()
    return i


def _fornecedor(s, razao, cnpj, email=None):
    f = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf=cnpj, razao_social=razao,
                   regime_tributario=RegimeTributario.NAO_INFORMADO,
                   email=email, canais_cotacao=["EMAIL"], regioes_atuacao=[])
    s.add(f)
    s.flush()
    return f


def _cotacao_com(s, usuario, obras_e_insumos, fornecedores):
    """Uma cotação com um item por (obra, insumo) e os fornecedores no mapa."""
    sol = SuprimentoSolicitacao(numero=f"SS-ENV-{usuario.id}",
                                titulo="pedido para cotar",
                                solicitante_id=usuario.id)
    s.add(sol)
    s.flush()
    itens = []
    for numero, (obra, insumo) in enumerate(obras_e_insumos, start=1):
        item = SuprimentoItem(solicitacao_id=sol.id, numero=numero,
                              insumo_id=insumo.id, quantidade=Decimal("14"),
                              unidade="SC", obra_id=obra.id,
                              especificacao="conforme projeto")
        s.add(item)
        itens.append(item)
    s.flush()

    cot = svc_cot.criar(s, {"titulo": "Cotação de teste",
                            "itens": [i.id for i in itens]}, usuario)
    s.flush()
    for f in fornecedores:
        svc_cot.adicionar_fornecedor(s, cot.id, {"fornecedor_id": f.id}, usuario)
    s.flush()
    return cot


# ---------------------------------------------------------------------------
# De qual empresa a cotação sai
# ---------------------------------------------------------------------------
def test_a_cotacao_sai_pela_empresa_da_obra(sessao_real, chave, correio_mudo):
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    _empresa(s, usuario, CNPJ_B, "CONSTRUTORA B LTDA")
    obra = _obra(s, "OBRA-A", a.id)
    forn = _fornecedor(s, "FORNECEDOR UM LTDA", "71000003000174",
                       "um@fornecedor.exemplo")
    cot = _cotacao_com(s, usuario, [(obra, _insumo(s))], [forn])

    rel = svc.disparar(s, cot.id, {}, usuario)

    assert rel["empresa"] == "CONSTRUTORA A LTDA"
    assert len(rel["enviados"]) == 1
    assert "compras@construtora" in correio_mudo[0]["de"]


def test_obras_de_empresas_diferentes_fazem_o_sistema_perguntar(
        sessao_real, chave, correio_mudo):
    """Sortear por qual CNPJ a compra corre seria pior do que recusar."""
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    b = _empresa(s, usuario, CNPJ_B, "CONSTRUTORA B LTDA")
    forn = _fornecedor(s, "FORNECEDOR UM LTDA", "71000003000174",
                       "um@fornecedor.exemplo")
    cot = _cotacao_com(s, usuario,
                       [(_obra(s, "OBRA-A", a.id), _insumo(s, "Cimento")),
                        (_obra(s, "OBRA-B", b.id), _insumo(s, "Areia"))],
                       [forn])

    with pytest.raises(ErroValidacao, match="empresas diferentes"):
        svc.disparar(s, cot.id, {}, usuario)

    # ...mas sai quando o comprador escolhe
    rel = svc.disparar(s, cot.id, {"empresa_id": b.id}, usuario)
    assert rel["empresa"] == "CONSTRUTORA B LTDA"


def test_empresa_sem_conta_de_email_recusa_com_o_motivo(sessao_real, chave):
    s = sessao_real
    usuario = _usuario(s)
    crua = svc_emp.criar(s, {"razao_social": "SEM CONTA LTDA",
                             "cnpj": CNPJ_A}, usuario)
    s.flush()
    obra = _obra(s, "OBRA-SEM", crua.id)
    forn = _fornecedor(s, "FORNECEDOR UM LTDA", "71000003000174",
                       "um@fornecedor.exemplo")
    cot = _cotacao_com(s, usuario, [(obra, _insumo(s))], [forn])

    with pytest.raises(ErroValidacao, match="está incompleta"):
        svc.disparar(s, cot.id, {}, usuario)


# ---------------------------------------------------------------------------
# Para quem vai
# ---------------------------------------------------------------------------
def test_o_contato_do_fornecedor_tem_preferencia_sobre_o_e_mail_geral(
        sessao_real, chave, correio_mudo):
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    obra = _obra(s, "OBRA-A", a.id)
    forn = _fornecedor(s, "FORNECEDOR UM LTDA", "71000003000174",
                       "geral@fornecedor.exemplo")
    s.add(FornecedorContato(fornecedor_id=forn.id, nome="Ricardo Alves",
                            email="ricardo@fornecedor.exemplo"))
    s.flush()
    cot = _cotacao_com(s, usuario, [(obra, _insumo(s))], [forn])

    svc.disparar(s, cot.id, {}, usuario)

    assert correio_mudo[0]["para"] == ["ricardo@fornecedor.exemplo"], \
        "cotação para caixa geral é cotação que ninguém responde"


def test_fornecedor_sem_e_mail_e_relatado_e_nao_some(sessao_real, chave,
                                                     correio_mudo):
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    obra = _obra(s, "OBRA-A", a.id)
    com = _fornecedor(s, "COM E-MAIL LTDA", "71000003000174", "um@f.exemplo")
    sem = _fornecedor(s, "SEM E-MAIL LTDA", "71000004000117", None)
    cot = _cotacao_com(s, usuario, [(obra, _insumo(s))], [com, sem])

    rel = svc.disparar(s, cot.id, {}, usuario)

    assert [e["fornecedor"] for e in rel["enviados"]] == ["COM E-MAIL LTDA"]
    assert rel["sem_endereco"] == ["SEM E-MAIL LTDA"]
    assert "sem e-mail" in rel["resumo"]


def test_um_fornecedor_fora_do_ar_nao_impede_os_outros(sessao_real, chave,
                                                       monkeypatch):
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    obra = _obra(s, "OBRA-A", a.id)
    bom = _fornecedor(s, "BOM LTDA", "71000003000174", "bom@f.exemplo")
    ruim = _fornecedor(s, "RUIM LTDA", "71000004000117", "ruim@f.exemplo")
    cot = _cotacao_com(s, usuario, [(obra, _insumo(s))], [bom, ruim])

    def so_o_ruim_falha(empresa, msg, destinos):
        if "ruim@f.exemplo" in destinos:
            raise correio.ErroDeEnvio("O servidor recusou o endereço.")
    monkeypatch.setattr(correio, "_entregar", so_o_ruim_falha)

    rel = svc.disparar(s, cot.id, {}, usuario)

    assert [e["fornecedor"] for e in rel["enviados"]] == ["BOM LTDA"]
    assert [f["fornecedor"] for f in rel["falhas"]] == ["RUIM LTDA"]
    assert "recusou o endereço" in rel["falhas"][0]["motivo"]


# ---------------------------------------------------------------------------
# O que o fornecedor recebe — e o que ele NÃO recebe
# ---------------------------------------------------------------------------
def test_o_corpo_traz_os_itens_e_nao_traz_preco_de_ninguem(sessao_real, chave,
                                                           correio_mudo):
    """Mandar o mapa seria entregar ao fornecedor A o preço do fornecedor B."""
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    obra = _obra(s, "OBRA-A", a.id)
    forn = _fornecedor(s, "FORNECEDOR UM LTDA", "71000003000174", "um@f.exemplo")
    cot = _cotacao_com(s, usuario, [(obra, _insumo(s, "Cimento CP-II 50kg"))],
                       [forn])
    svc.disparar(s, cot.id, {"prazo": "10/09/2026"}, usuario)

    corpo = correio_mudo[0]["corpo"]
    assert "Cimento CP-II 50kg" in corpo
    assert "conforme projeto" in corpo
    assert "14 SC" in corpo, "quantidade legível: 14, não 14.000"
    assert cot.numero in corpo, "a referência volta na resposta do fornecedor"
    assert "10/09/2026" in corpo
    assert "R$" not in corpo, "nenhum preço vai no pedido de cotação"


def test_o_registro_guarda_o_texto_exato(sessao_real, chave, correio_mudo):
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    obra = _obra(s, "OBRA-A", a.id)
    forn = _fornecedor(s, "FORNECEDOR UM LTDA", "71000003000174", "um@f.exemplo")
    cot = _cotacao_com(s, usuario, [(obra, _insumo(s))], [forn])

    svc.disparar(s, cot.id, {}, usuario)
    s.flush()

    registros = correio.historico(s, "cotacao", cot.id)
    assert len(registros) == 1
    assert registros[0]["situacao"] == "ENVIADO"
    assert registros[0]["para"] == ["um@f.exemplo"]
    # `.strip()` porque o formato de e-mail acrescenta uma quebra de linha no
    # fim da mensagem; o texto em si tem de ser o mesmo.
    assert registros[0]["corpo"].strip() == correio_mudo[0]["corpo"].strip(), \
        "seis meses depois, 'o que foi que a gente pediu' tem de ter resposta"


def test_a_tela_de_disparo_mostra_quem_recebe_antes_de_mandar(sessao_real, chave):
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    obra = _obra(s, "OBRA-A", a.id)
    com = _fornecedor(s, "COM E-MAIL LTDA", "71000003000174", "um@f.exemplo")
    sem = _fornecedor(s, "SEM E-MAIL LTDA", "71000004000117", None)
    cot = _cotacao_com(s, usuario, [(obra, _insumo(s))], [com, sem])

    p = svc.preparar(s, cot.id)

    assert p["conta_pronta"] is True
    assert p["empresa"]["razao_social"] == "CONSTRUTORA A LTDA"
    por_nome = {d["fornecedor"]: d for d in p["destinatarios"]}
    assert por_nome["COM E-MAIL LTDA"]["pode"] is True
    assert por_nome["SEM E-MAIL LTDA"]["pode"] is False
    assert "sem e-mail" in por_nome["SEM E-MAIL LTDA"]["motivo"]
    assert "não é o mesmo que entregue" in p["aviso"], \
        "a tela precisa dizer o que o registro NÃO prova"


def test_obra_sem_empresa_aparece_na_preparacao(sessao_real, chave):
    s = sessao_real
    usuario = _usuario(s)
    _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    orfa = _obra(s, "ORFA-01", None)
    forn = _fornecedor(s, "FORNECEDOR UM LTDA", "71000003000174", "um@f.exemplo")
    cot = _cotacao_com(s, usuario, [(orfa, _insumo(s))], [forn])

    p = svc.preparar(s, cot.id)

    assert p["obras_sem_empresa"] == ["ORFA-01"]
