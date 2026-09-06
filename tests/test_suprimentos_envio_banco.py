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


# ---------------------------------------------------------------------------
# O que o fornecedor lê: especificação e ONDE entregar
# ---------------------------------------------------------------------------
def _obra_com_endereco(s, codigo, empresa_id, **endereco):
    o = Obra(codigo=codigo, nome=f"Obra {codigo}", status="ATIVA",
             empresa_id=empresa_id, **endereco)
    s.add(o)
    s.flush()
    return o


def test_a_cotacao_diz_a_especificacao_de_cada_item(sessao_real, chave,
                                                    correio_mudo):
    """Sem a especificação, "cimento" cobre CP-II e CP-V, e chega o errado."""
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    obra = _obra(s, "OBRA-A", a.id)
    forn = _fornecedor(s, "FORNECEDOR UM LTDA", "71000003000174", "um@f.exemplo")
    cot = _cotacao_com(s, usuario, [(obra, _insumo(s))], [forn])

    corpo = svc.montar_mensagem(s, cot.id, a)["corpo"]

    assert "conforme projeto" in corpo, "a especificação tem de ir junto do item"


def test_a_cotacao_diz_onde_entregar(sessao_real, chave, correio_mudo):
    """O frete depende da distância: pedir preço sem dizer o endereço é
    receber um preço que muda depois."""
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    obra = _obra_com_endereco(s, "OBRA-A", a.id, endereco="Rua das Flores",
                              numero_endereco="120", bairro="Centro",
                              municipio="Barbalha", uf="CE")
    forn = _fornecedor(s, "FORNECEDOR UM LTDA", "71000003000174", "um@f.exemplo")
    cot = _cotacao_com(s, usuario, [(obra, _insumo(s))], [forn])

    corpo = svc.montar_mensagem(s, cot.id, a)["corpo"]

    assert "ENTREGAR EM: Rua das Flores, 120, Centro, Barbalha, CE" in corpo
    assert "OBRA-A" in corpo


def test_cotacao_para_duas_obras_separa_os_enderecos(sessao_real, chave):
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    uma = _obra_com_endereco(s, "OBRA-UMA", a.id, endereco="Rua A",
                             municipio="Crato", uf="CE")
    outra = _obra_com_endereco(s, "OBRA-DUAS", a.id, endereco="Rua B",
                               municipio="Juazeiro", uf="CE")
    forn = _fornecedor(s, "FORNECEDOR UM LTDA", "71000003000174", "um@f.exemplo")
    cot = _cotacao_com(s, usuario,
                       [(uma, _insumo(s, "Cimento")),
                        (outra, _insumo(s, "Areia"))], [forn])

    corpo = svc.montar_mensagem(s, cot.id, a)["corpo"]

    assert corpo.count("ENTREGAR EM:") == 2
    assert "Rua A, Crato, CE" in corpo and "Rua B, Juazeiro, CE" in corpo
    # a numeração dos itens não reinicia a cada endereço: o fornecedor cita o
    # número do item na proposta
    assert "  1." in corpo and "  2." in corpo


def test_obra_sem_endereco_diz_isso_em_vez_de_ficar_em_branco(sessao_real, chave):
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    obra = _obra(s, "SEM-END", a.id)
    forn = _fornecedor(s, "FORNECEDOR UM LTDA", "71000003000174", "um@f.exemplo")
    cot = _cotacao_com(s, usuario, [(obra, _insumo(s))], [forn])

    corpo = svc.montar_mensagem(s, cot.id, a)["corpo"]

    assert "Endereço não informado" in corpo


# ---------------------------------------------------------------------------
# O pedido de compra: o documento que FIRMA
# ---------------------------------------------------------------------------
def _pedido_pronto(s, usuario, empresa, *, autorizar=True, condicao=True):
    """Um pedido de compra fechado do mapa, opcionalmente já autorizado."""
    from app.apps.erp.core.suprimentos import pedido as svc_ped
    from app.apps.erp.db.models.cadastros import CondicaoPagamento

    obra = _obra_com_endereco(s, f"OBRA-{usuario.id}", empresa.id,
                              endereco="Av. Central", numero_endereco="900",
                              municipio="Barbalha", uf="CE")
    forn = _fornecedor(s, "FORNECEDOR DO PEDIDO LTDA", "71000005000150",
                       "pedido@fornecedor.exemplo")
    cot = _cotacao_com(s, usuario, [(obra, _insumo(s, "Cimento CP-II"))], [forn])

    from app.apps.erp.db.models.cadastros import CotacaoFornecedor, CotacaoItem
    from sqlalchemy import select as _select
    coluna = [c for c in s.scalars(_select(CotacaoFornecedor)).all()
              if c.cotacao_id == cot.id][0]
    linha = [c for c in s.scalars(_select(CotacaoItem)).all()
             if c.cotacao_id == cot.id][0]
    svc_cot.lancar_preco(s, coluna.id, linha.id, "38,50", usuario)
    s.flush()

    cond = None
    if condicao:
        cond = CondicaoPagamento(nome="28/56 dias", entrada_percentual=0,
                                 dias=[28, 56])
        s.add(cond)
        s.flush()
        coluna.condicao_pagamento_id = cond.id
        s.flush()

    # `cotacao_itens` são as LINHAS do mapa, não os itens da solicitação.
    pedido = svc_ped.fechar_do_mapa(s, cot.id, coluna.id, [linha.id], {},
                                    usuario)
    s.flush()
    if autorizar:
        svc_ped.autorizar(s, pedido.id, usuario)
        s.flush()
    return pedido


def test_o_pedido_traz_especificacao_preco_endereco_e_condicao(sessao_real, chave):
    """Os quatro pedaços que fazem o documento firmar a compra."""
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    pedido = _pedido_pronto(s, usuario, a)

    corpo = svc.montar_pedido(s, pedido.id, a)["corpo"]

    assert "conforme projeto" in corpo, "sem especificação chega o material errado"
    assert "ENTREGAR EM: Av. Central, 900, Barbalha, CE" in corpo
    assert "R$ 38,50" in corpo, "o preço unitário fecha a discussão da nota"
    assert "TOTAL DO PEDIDO" in corpo
    assert "28/56 dias" in corpo, "a condição acertada tem de estar no papel"
    assert "CNPJ" in corpo, "o fornecedor fatura contra um CNPJ"


def test_o_pedido_soma_certo(sessao_real, chave):
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    pedido = _pedido_pronto(s, usuario, a)

    corpo = svc.montar_pedido(s, pedido.id, a)["corpo"]

    # 14 unidades a 38,50 = 539,00
    assert "R$ 539,00" in corpo


def test_pedido_sem_condicao_cadastrada_diz_a_combinar(sessao_real, chave):
    """Em branco, o fornecedor inventa o prazo dele."""
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    pedido = _pedido_pronto(s, usuario, a, condicao=False)

    corpo = svc.montar_pedido(s, pedido.id, a)["corpo"]

    assert "Condição de pagamento: a combinar" in corpo


def test_pedido_nao_autorizado_nao_sai(sessao_real, chave, correio_mudo):
    """Mandar antes da autorização é comprar sem alçada: o fornecedor entrega
    e a conta chega."""
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    pedido = _pedido_pronto(s, usuario, a, autorizar=False)

    with pytest.raises(ErroValidacao, match="autorizado"):
        svc.disparar_pedido(s, pedido.id, {}, usuario)
    assert correio_mudo == [], "a recusa vem antes de qualquer envio"


def test_pedido_autorizado_sai_e_fica_registrado(sessao_real, chave, correio_mudo):
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    pedido = _pedido_pronto(s, usuario, a)

    r = svc.disparar_pedido(s, pedido.id, {}, usuario)
    s.flush()

    assert r["ok"] is True
    assert r["para"] == ["pedido@fornecedor.exemplo"]
    assert len(correio_mudo) == 1
    assert pedido.numero in correio_mudo[0]["assunto"]

    registros = correio.historico(s, "pedido_compra", pedido.id)
    assert len(registros) == 1
    assert registros[0]["situacao"] == "ENVIADO"
    assert "TOTAL DO PEDIDO" in registros[0]["corpo"], \
        "o registro guarda o texto exato que o fornecedor recebeu"


def test_fornecedor_sem_e_mail_recusa_com_o_nome_dele(sessao_real, chave,
                                                      correio_mudo):
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    pedido = _pedido_pronto(s, usuario, a)
    forn = s.get(Fornecedor, pedido.fornecedor_id)
    forn.email = None
    s.flush()

    with pytest.raises(ErroValidacao, match="sem e-mail"):
        svc.disparar_pedido(s, pedido.id, {}, usuario)


def test_a_tela_do_pedido_mostra_o_texto_antes_de_mandar(sessao_real, chave):
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    pedido = _pedido_pronto(s, usuario, a)

    p = svc.preparar_pedido(s, pedido.id)

    assert p["autorizado"] is True
    assert p["para"] == ["pedido@fornecedor.exemplo"]
    assert p["conta_pronta"] is True
    assert "PEDIDO DE COMPRA" in p["corpo"]
    assert p["envios"] == []
    assert "não é o mesmo que entregue" in p["aviso"]


def test_a_tela_avisa_quando_o_pedido_ainda_nao_foi_autorizado(sessao_real, chave):
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    pedido = _pedido_pronto(s, usuario, a, autorizar=False)

    p = svc.preparar_pedido(s, pedido.id)

    assert p["autorizado"] is False
    assert p["situacao"] == "AGUARDANDO_AUTORIZACAO"


def test_o_cnpj_sai_pontuado_no_documento(sessao_real, chave):
    """Catorze dígitos seguidos ninguém confere. O fornecedor bate este número
    contra o cadastro dele antes de faturar."""
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    pedido = _pedido_pronto(s, usuario, a)

    corpo = svc.montar_pedido(s, pedido.id, a)["corpo"]

    assert "CNPJ 71.000.001/0001-84" in corpo


def test_a_condicao_nao_aparece_repetida(sessao_real, chave):
    """"28/56 dias (28/56 dias)" é ruído: a explicação só entra quando
    acrescenta alguma coisa ao nome cadastrado."""
    s = sessao_real
    usuario = _usuario(s)
    a = _empresa(s, usuario, CNPJ_A, "CONSTRUTORA A LTDA")
    pedido = _pedido_pronto(s, usuario, a)

    corpo = svc.montar_pedido(s, pedido.id, a)["corpo"]

    assert "Condição de pagamento: 28/56 dias" in corpo
    assert "(28/56 dias)" not in corpo
