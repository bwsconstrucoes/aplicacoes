"""O contrato de empreita gerado pelo sistema e assinado à distância (27/11/2026).

O dono: *"duas pessoas assinariam. Seria o de fora e um de dentro (…) quem
assina é uma pessoa do cadastro de colaboradores (…) WhatsApp e e-mail (…) que
tenha valor jurídico."*

COM BANCO DE VERDADE: o caminho inteiro passa por tabelas novas (migração 132),
por SQL direto e pelo arquivo. O WhatsApp é dublado — a mensagem é capturada,
e é dela que o teste tira o link e o código, como a pessoa faria.
"""
from __future__ import annotations

import base64
import re
from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import text

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.titulos import contrato_assinatura as ca
from app.apps.erp.core.titulos import empreita
from app.apps.erp.db.models.cadastros import (
    Categoria, Colaborador, Empresa, Fornecedor, Obra, PerfilUsuario as P,
    RegimeTributario, TipoPessoa, Usuario,
)
from app.apps.erp.db.models.financeiro import ContratoServicoItem, EmpreitaAlcada

pytestmark = pytest.mark.banco

# Um PNG mínimo de verdade (1×1), como o quadro de assinatura mandaria.
PNG = "data:image/png;base64," + base64.b64encode(bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
    "0000000d49444154789c6360000002000154a24f5d0000000049454e44ae426082")).decode()


@pytest.fixture
def mensagens(monkeypatch):
    """O WhatsApp dublado: guarda o que sairia."""
    saiu: list[tuple[str, str]] = []

    def falso(telefone, mensagem, finalidade=None):
        saiu.append((telefone, mensagem))
        return {"ok": True, "detalhe": "dublado"}
    import app.apps.notificador as notificador
    monkeypatch.setattr(notificador, "enviar_whatsapp", falso)
    return saiu


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    if not s.query(EmpreitaAlcada).count():
        s.add(EmpreitaAlcada(valor_ate=None, perfis=["ADMIN", "DIRETOR_FINANCEIRO"],
                             descricao="Tudo"))
        s.flush()
    quem_cadastra = Usuario(nome="Adm Obra", email="ca-obra@teste.local", ativo=True,
                            senha_hash=gerar_hash("senha-de-teste-1234"), perfil=P.ADMIN)
    quem_aprova = Usuario(nome="Diretor", email="ca-dir@teste.local", ativo=True,
                          senha_hash=gerar_hash("senha-de-teste-1234"), perfil=P.ADMIN)
    emp = Empresa(razao_social="BWS Construções Ltda", cnpj="11444777000161",
                  municipio="Recife", uf="PE", logradouro="Rua A", numero="10")
    s.add_all([quem_cadastra, quem_aprova, emp])
    s.flush()
    obra = Obra(codigo="CTASS01", nome="Escola Assinatura", empresa_id=emp.id,
                endereco="Av. Central", municipio="Recife", uf="PE")
    prestador = Fornecedor(razao_social="José Pedreiro", cnpj_cpf="52998224725",
                           tipo_pessoa=TipoPessoa.PF, ativo=True,
                           regime_tributario=RegimeTributario.NAO_INFORMADO)
    socio = Colaborador(nome="Maria Sócia", cpf="11144477735", telefone="81988887777",
                        situacao="ATIVO")
    cat = Categoria(codigo="3.2.09", descricao="Empreitas teste")
    s.add_all([obra, prestador, socio, cat])
    s.flush()
    c = empreita.criar_contrato(s, {
        "obra_id": obra.id, "fornecedor_id": prestador.id, "categoria_id": cat.id,
        "objeto": "Muro de divisa do lote", "modo": "MEDICAO", "data_inicio": "2026-12-01",
        "data_fim": "2026-12-20", "retencao_garantia_pct": "5",
        "itens": [{"descricao": "Alvenaria do muro", "unidade": "m²",
                   "quantidade": "40", "preco_unitario": "55"}]}, quem_cadastra)
    s.flush()
    return {"s": s, "c": c, "obra": obra, "socio": socio, "prestador": prestador,
            "cadastra": quem_cadastra, "aprova": quem_aprova}


def _escolhas(cen, **extra):
    base = {"assinante_colaborador_id": cen["socio"].id,
            "contratado_telefone": "81999990000",
            "opcionais": {"LIMPEZA": True, "MULTA_ATRASO": True},
            "campos": {"multa_pct": "0,5"},
            "texto_adicional": "O empreiteiro guarda as ferramentas no container da obra."}
    base.update(extra)
    return base


def test_o_texto_puxa_o_cadastro_e_as_clausulas_marcadas(cenario):
    t = ca.montar(cenario["s"], cenario["c"].id, _escolhas(cenario))["texto"]
    assert "BWS Construções Ltda" in t and "11.444.777/0001-61" in t
    assert "Maria Sócia" in t, "quem assina pela empresa vem do cadastro de colaboradores"
    assert "José Pedreiro" in t and "529.982.247-25" in t
    assert "Alvenaria do muro: 40 m² × R$ 55,00 = R$ 2.200,00" in t
    assert "dois mil e duzentos reais" in t
    assert "0,5% do valor do contrato por dia" in t, "a multa marcada, com o número"
    assert "container da obra" in t, "o texto digitado entra nas condições particulares"
    assert "5% " in t, "a garantia do cadastro do contrato"


def test_clausula_marcada_sem_o_numero_e_recusada(cenario):
    with pytest.raises(ErroValidacao, match="Equipamento emprestado"):
        ca.montar(cenario["s"], cenario["c"].id, _escolhas(
            cenario, opcionais={"EQUIPAMENTO_EMPRESTADO": True}, campos={"equipamentos": ""}))


def test_assinante_sem_contato_e_recusado(cenario):
    cenario["socio"].telefone = None
    cenario["s"].flush()
    with pytest.raises(ErroValidacao, match="não tem telefone nem e-mail"):
        ca.preparar(cenario["s"], cenario["c"].id, _escolhas(cenario), cenario["cadastra"])


def test_o_caminho_inteiro_ate_o_contrato_vigente(cenario, mensagens):
    s, c = cenario["s"], cenario["c"]
    ca.preparar(s, c.id, _escolhas(cenario), cenario["cadastra"])
    assert not mensagens, "nada sai antes do aval"

    empreita.aprovar_contrato(s, c.id, cenario["aprova"])
    r = ca.ao_aprovar(s, c, "https://erp.teste/", cenario["aprova"])
    s.flush()
    assert r["assinatura"] and c.status == "AGUARDANDO_ASSINATURA", \
        "com contrato gerado, o aval não deixa vigente: espera as assinaturas"
    assert {t for t, _ in mensagens} == {"81999990000", "81988887777"}, \
        "os dois recebem o link: o empreiteiro e quem assina pela empresa"
    links = {t: re.search(r"/erp/assinar/(\S+)", m).group(1) for t, m in mensagens}

    for telefone, documento in (("81999990000", "529.982.247-25"),
                                ("81988887777", "111.444.777-35")):
        token = links[telefone]
        aberto = ca.abrir(s, token)
        assert "Muro de divisa" in aberto["texto"]
        antes = len(mensagens)
        ca.pedir_codigo(s, token, "whatsapp")
        codigo = re.search(r"\b(\d{6})\b", mensagens[antes][1]).group(1)
        with pytest.raises(ErroValidacao, match="CPF/CNPJ"):
            ca.assinar(s, token, codigo=codigo, documento="00000000000", assinatura=PNG,
                       lacre_visto=aberto["lacre"], ip="1.2.3.4", navegador="teste")
        fim = ca.assinar(s, token, codigo=codigo, documento=documento, assinatura=PNG,
                         lacre_visto=aberto["lacre"], ip="1.2.3.4", navegador="teste")
    assert fim["concluido"]
    s.expire_all()
    status = s.execute(text("SELECT status FROM contratos_servico WHERE id = :i"),
                       {"i": c.id}).scalar()
    assert status == "VIGENTE", "com as duas assinaturas, o contrato pode ser medido"
    sit = ca.situacao(s, c.id)
    assert sit["situacao"] == "ASSINADO" and sit["anexo_id"], "o PDF assinado vai para o arquivo"
    pdf, _ = ca.pdf_assinado(s, links["81999990000"])
    assert pdf.startswith(b"%PDF")


def test_codigo_errado_conta_tentativa_e_texto_mudado_e_recusado(cenario, mensagens):
    s, c = cenario["s"], cenario["c"]
    ca.preparar(s, c.id, _escolhas(cenario), cenario["cadastra"])
    empreita.aprovar_contrato(s, c.id, cenario["aprova"])
    ca.ao_aprovar(s, c, "https://erp.teste/", cenario["aprova"])
    token = re.search(r"/erp/assinar/(\S+)", mensagens[0][1]).group(1)
    aberto = ca.abrir(s, token)
    with pytest.raises(ErroValidacao, match="Peça o código"):
        ca.assinar(s, token, codigo="123456", documento="52998224725", assinatura=PNG,
                   lacre_visto=aberto["lacre"], ip="", navegador="")
    ca.pedir_codigo(s, token, "whatsapp")
    with pytest.raises(ErroValidacao, match="Código não confere"):
        ca.assinar(s, token, codigo="000000" if "000000" not in mensagens[-1][1] else "999999",
                   documento="52998224725", assinatura=PNG, lacre_visto=aberto["lacre"],
                   ip="", navegador="")
    with pytest.raises(ErroValidacao, match="texto do contrato mudou"):
        ca.assinar(s, token, codigo="123456", documento="52998224725", assinatura=PNG,
                   lacre_visto="outro-lacre", ip="", navegador="")


def test_reenviar_troca_o_link_e_o_antigo_deixa_de_valer(cenario, mensagens):
    s, c = cenario["s"], cenario["c"]
    ca.preparar(s, c.id, _escolhas(cenario), cenario["cadastra"])
    empreita.aprovar_contrato(s, c.id, cenario["aprova"])
    ca.ao_aprovar(s, c, "https://erp.teste/", cenario["aprova"])
    velho = re.search(r"/erp/assinar/(\S+)", mensagens[0][1]).group(1)
    ca.reenviar(s, c.id, "CONTRATADO", "https://erp.teste/")
    novo = re.search(r"/erp/assinar/(\S+)", mensagens[-1][1]).group(1)
    assert novo != velho
    ca.abrir(s, novo)
    from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado
    with pytest.raises(ErroNaoEncontrado):
        ca.abrir(s, velho)


def test_contrato_sem_documento_gerado_fica_vigente_no_aval_como_antes(cenario, mensagens):
    s, c = cenario["s"], cenario["c"]
    empreita.aprovar_contrato(s, c.id, cenario["aprova"])
    assert ca.ao_aprovar(s, c, "https://erp.teste/", cenario["aprova"]) == {"assinatura": False}
    assert c.status == "VIGENTE" and not mensagens


def test_valor_por_extenso():
    assert ca.por_extenso("2200") == "dois mil e duzentos reais"
    assert ca.por_extenso("1") == "um real"
    assert ca.por_extenso("1000000") == "um milhão de reais"
    assert ca.por_extenso("150.35") == "cento e cinquenta reais e trinta e cinco centavos"


# ---------------------------------------------------------------------------
# O EMPREITEIRO PELO DOCUMENTO (27/11/2026): "utilizar um cartão CNPJ, uma
# identidade, RG". A leitura (IA) é dublada; o que se testa é o que a tela
# grava depois que a pessoa CONFERIU.
# ---------------------------------------------------------------------------
def test_o_empreiteiro_novo_nasce_do_que_foi_conferido_e_guarda_o_documento(cenario):
    from app.apps.erp.core.documentos.armazenamento import listar
    from app.apps.erp.core.titulos import empreiteiro
    s = cenario["s"]
    r = empreiteiro.gravar(s, {
        "documento": "390.533.447-05", "nome": "João Azulejista", "cep": "50000-000",
        "logradouro": "Rua das Flores", "numero": "12", "bairro": "Centro",
        "municipio": "Recife", "uf": "pe", "telefone": "(81) 97777-6666"},
        [(b"%PDF-1.4 rg", "rg.pdf")], cenario["cadastra"])
    assert r["novo"]
    dados = empreiteiro.do_cadastro(s, r["fornecedor_id"])
    assert dados["tipo_pessoa"] == "PF" and dados["uf"] == "PE"
    assert dados["telefone"] == "81977776666", "o WhatsApp fica só com os dígitos"
    assert len(listar(s, "fornecedor", r["fornecedor_id"])) == 1, \
        "o documento lido fica arquivado no cadastro do empreiteiro"


def test_o_empreiteiro_que_ja_existe_e_completado_nao_duplicado(cenario):
    from app.apps.erp.core.titulos import empreiteiro
    s, f = cenario["s"], cenario["prestador"]
    r = empreiteiro.gravar(s, {"documento": f.cnpj_cpf, "nome": f.razao_social,
                               "logradouro": "Rua Nova", "telefone": "81955554444"},
                           [], cenario["cadastra"])
    assert not r["novo"] and r["fornecedor_id"] == f.id
    assert set(r["mudou"]) >= {"logradouro", "telefone"}


def test_documento_invalido_do_empreiteiro_e_recusado(cenario):
    from app.apps.erp.core.titulos import empreiteiro
    with pytest.raises(ErroValidacao, match="inválido"):
        empreiteiro.gravar(cenario["s"], {"documento": "123", "nome": "X Y Z"}, [],
                           cenario["cadastra"])


def test_o_contratado_e_identificado_pelo_cpf_ou_cnpj_sem_rg(cenario):
    """Ele: "quando eu falei RG (…) me referi a documento da pessoa de maneira
    geral (…) o documento para se fazer esse tipo de coisa é o CPF e CNPJ." """
    t = ca.montar(cenario["s"], cenario["c"].id, _escolhas(cenario))["texto"]
    linha = next(l for l in t.splitlines() if l.startswith("CONTRATADO:"))
    assert "CPF nº 529.982.247-25" in linha and "RG" not in linha


def test_a_leitura_do_documento_preenche_os_campos(cenario, monkeypatch):
    from app.apps.erp.core import documentos_de_pessoa as svc_doc
    from app.apps.erp.core.titulos import empreiteiro
    monkeypatch.setattr(svc_doc, "ler_varios", lambda arquivos: {"campos": {
        "nome": "JOSÉ PEDREIRO", "cpf": "529.982.247-25", "rg": "7654321",
        "rg_orgao": "SDS", "rg_uf": "PE", "logradouro": "Rua A", "numero_endereco": "5",
        "municipio": "Olinda", "uf": "PE"}})
    r = empreiteiro.ler(cenario["s"], [(b"x", "rg.jpg")], "PF")
    assert r["campos"]["documento"] == "52998224725" and "rg" not in r["campos"]
    assert r["campos"]["numero"] == "5"
    assert r["ja_cadastrado"]["id"] == cenario["prestador"].id, \
        "a tela avisa que esse CPF já está no cadastro"
