"""AS CORREÇÕES DA TELA DE INSUMOS E FORNECEDORES — 20/09/2026.

O dono passou meia hora usando as telas e trouxe uma lista. Estes testes
seguram o que foi consertado, e dois deles existem por causa de estragos que
aconteceram COM ELE, na tela, enquanto usava:

1. **"Eu cliquei duas vezes na conta do plano financeiro, selecionei, não
   lembro qual é o insumo, e alterei a conta (…) e eu não sei qual foi."**
   → a trilha sempre guardou o antes e o depois; faltava alguém LISTAR isso na
   tela onde o erro acontece. `alteracoes_de_insumo` é essa lista, e ela traz
   o valor anterior pronto para o Desfazer.

2. **"Eu adicionei um novo insumo e chamei ele de teste (…) não alterou a
   quantidade de insumos ativos, não está trazendo o insumo novo."**
   → o cadastro era RECUSADO por falta da conta do plano, e o recado se perdia
   atrás do último dos quatro diálogos. O teste abaixo trava a recusa; a tela
   passou a pedir tudo de uma vez, com o que falta escrito no botão.

3. **"Como é que eu excluo? Eu não estou vendo o botão para excluir."**
   → apaga quando nunca foi usado; recusa dizendo ONDE aparece quando foi.

POR QUE COM POSTGRES: quase tudo aqui é WHERE e contagem em tabela de outro
módulo (títulos, pedidos, preços). O dublê da suíte ignora WHERE — um teste de
"este fornecedor pode ser apagado?" no dublê responderia sempre a mesma coisa.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.suprimentos import cadastro as svc
from app.apps.erp.core.suprimentos import fornecedores as svc_forn
from app.apps.erp.db.models.cadastros import (
    Categoria, Fornecedor, Insumo, InsumoCategoria, PerfilUsuario as P,
    PrecoHistorico, TipoPessoa, TipoPreco, Usuario,
)

pytestmark = pytest.mark.banco


def _conta(codigo, descricao):
    """Uma conta do plano que aceita insumo: nota de material e fora do grupo
    de receita. É o que `e_conta_de_compra` exige."""
    return Categoria(codigo=codigo, descricao=descricao,
                     tipos_permitidos=["T1_MATERIAL_NFE"], natureza="RESULTADO",
                     grupo_codigo="3", grupo_nome="Custos de obra",
                     subgrupo_codigo="3.1", subgrupo_nome="Materiais aplicados",
                     ativo=True)


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    chefe = Usuario(nome="Chefe", email="chefe.ins@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN)
    cat = InsumoCategoria(codigo="TCIM", nome="Cimento (teste)", ativo=True)
    conta = _conta("9.9.01", "Cimento e concreto (teste)")
    outra = _conta("9.9.02", "Armadura (teste)")
    s.add_all([chefe, cat, conta, outra])
    s.flush()
    return {"s": s, "chefe": chefe, "cat": cat, "conta": conta, "outra": outra}


# ---------------------------------------------------------------------------
# 1. O cadastro recusa sem conta do plano — e é isso que a tela precisa dizer
# ---------------------------------------------------------------------------
def test_insumo_sem_conta_do_plano_e_recusado(cenario):
    """A recusa está certa; o que estava errado era escondê-la atrás do quarto
    diálogo, fazendo parecer que o sistema tinha engolido o cadastro."""
    s, chefe, cat = cenario["s"], cenario["chefe"], cenario["cat"]
    with pytest.raises(ErroValidacao) as e:
        svc.criar_insumo(s, {"descricao": "Teste sem conta",
                             "categoria_insumo_id": cat.id}, chefe)
    assert "conta do plano" in str(e.value).lower()


def test_insumo_completo_entra_e_conta_nos_indicadores(cenario):
    s, chefe, cat, conta = (cenario["s"], cenario["chefe"], cenario["cat"],
                            cenario["conta"])
    antes = svc.gerenciar_insumos(s)["indicadores"]["ativos"]
    svc.criar_insumo(s, {"descricao": "Cimento de teste 50kg",
                         "categoria_insumo_id": cat.id,
                         "categoria_id": conta.id}, chefe)
    s.flush()
    depois = svc.gerenciar_insumos(s)
    assert depois["indicadores"]["ativos"] == antes + 1
    assert any(i["descricao"] == "Cimento de teste 50kg" for i in depois["insumos"])


# ---------------------------------------------------------------------------
# 2. "E eu não sei qual foi" — a lista que responde isso
# ---------------------------------------------------------------------------
def test_a_alteracao_fica_listada_com_o_valor_anterior(cenario):
    s, chefe, cat, conta = (cenario["s"], cenario["chefe"], cenario["cat"],
                            cenario["conta"])
    insumo = svc.criar_insumo(s, {"descricao": "Insumo que vai mudar",
                                  "categoria_insumo_id": cat.id,
                                  "categoria_id": conta.id}, chefe)
    s.flush()
    outra = cenario["outra"]
    svc.editar_insumo(s, insumo.id, {"categoria_id": outra.id}, chefe)
    s.flush()

    lista = svc.alteracoes_de_insumo(s)
    minha = [a for a in lista if a["insumo_id"] == insumo.id]
    assert minha, "a alteração tem de aparecer na lista"
    a = minha[0]
    assert a["insumo"] == "Insumo que vai mudar"
    assert a["quem"] == "Chefe"
    mudanca = [m for m in a["mudancas"] if m["campo"] == "categoria_id"][0]
    assert mudanca["rotulo"] == "conta do plano financeiro"
    assert conta.codigo in mudanca["de"]
    assert outra.codigo in mudanca["para"]
    # E o desfazer carrega o valor ANTERIOR, cru, pronto para o PATCH.
    assert a["desfazer"] == {"categoria_id": conta.id}


def test_desfazer_devolve_o_insumo_ao_estado_anterior(cenario):
    s, chefe, cat, conta = (cenario["s"], cenario["chefe"], cenario["cat"],
                            cenario["conta"])
    insumo = svc.criar_insumo(s, {"descricao": "Vai e volta",
                                  "categoria_insumo_id": cat.id,
                                  "categoria_id": conta.id}, chefe)
    s.flush()
    outra = cenario["outra"]
    svc.editar_insumo(s, insumo.id, {"categoria_id": outra.id}, chefe)
    s.flush()

    desfazer = [a for a in svc.alteracoes_de_insumo(s)
                if a["insumo_id"] == insumo.id][0]["desfazer"]
    svc.editar_insumo(s, insumo.id, desfazer, chefe)
    s.flush()
    assert s.get(Insumo, insumo.id).categoria_id == conta.id


def test_edicao_que_nao_muda_nada_nao_polui_a_lista(cenario):
    """Reenviar o mesmo valor grava evento. Listar isso encheria a tela de
    linhas que não dizem nada — e a lista existe justamente para achar a
    alteração perdida no meio das outras."""
    s, chefe, cat, conta = (cenario["s"], cenario["chefe"], cenario["cat"],
                            cenario["conta"])
    insumo = svc.criar_insumo(s, {"descricao": "Sem mudança",
                                  "categoria_insumo_id": cat.id,
                                  "categoria_id": conta.id}, chefe)
    s.flush()
    svc.editar_insumo(s, insumo.id, {"categoria_id": conta.id}, chefe)
    s.flush()
    assert not [a for a in svc.alteracoes_de_insumo(s)
                if a["insumo_id"] == insumo.id]


# ---------------------------------------------------------------------------
# 3. Apagar fornecedor — só quem nunca foi usado
# ---------------------------------------------------------------------------
def _fornecedor(s, doc="34028316000103", nome="NUNCA USADO LTDA"):
    f = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf=doc, razao_social=nome)
    s.add(f)
    s.flush()
    return f


def test_fornecedor_nunca_usado_e_apagado(cenario):
    s, chefe = cenario["s"], cenario["chefe"]
    f = _fornecedor(s)
    assert svc_forn.uso_do_fornecedor(s, f.id) == []

    svc_forn.apagar(s, f.id, chefe)
    s.flush()
    assert s.get(Fornecedor, f.id) is None


def test_fornecedor_com_historico_nao_e_apagado_e_a_recusa_diz_onde(cenario):
    """O recado é o ponto: "não é possível excluir" deixaria a pessoa sem
    saída, que foi o que o dono descreveu ao procurar o botão."""
    s, chefe, cat, conta = (cenario["s"], cenario["chefe"], cenario["cat"],
                            cenario["conta"])
    f = _fornecedor(s, doc="11444777000161", nome="JA USADO LTDA")
    insumo = svc.criar_insumo(s, {"descricao": "Com preço",
                                  "categoria_insumo_id": cat.id,
                                  "categoria_id": conta.id}, chefe)
    s.flush()
    s.add(PrecoHistorico(insumo_id=insumo.id, preco_unitario=Decimal("10.00"),
                         fornecedor_id=f.id, tipo=TipoPreco.COTADO,
                         data=date.today()))
    s.flush()

    usos = svc_forn.uso_do_fornecedor(s, f.id)
    assert usos == [{"onde": "preço no histórico", "quantos": 1}]

    with pytest.raises(ErroValidacao) as e:
        svc_forn.apagar(s, f.id, chefe)
    recado = str(e.value)
    assert "preço no histórico" in recado
    assert "Desative" in recado, "a recusa tem de dizer o que fazer em vez disso"
    assert s.get(Fornecedor, f.id) is not None


def test_apagar_leva_junto_os_contatos_do_proprio_cadastro(cenario):
    """Contato não é histórico: é parte do cadastro que está sendo apagado."""
    from app.apps.erp.db.models.cadastros import FornecedorContato
    s, chefe = cenario["s"], cenario["chefe"]
    f = _fornecedor(s)
    s.add(FornecedorContato(fornecedor_id=f.id, nome="ELIAS",
                            email="elias@x.com.br"))
    s.flush()

    svc_forn.apagar(s, f.id, chefe)
    s.flush()
    assert not [c for c in s.scalars(select(FornecedorContato)).all()
                if c.fornecedor_id == f.id]


# ---------------------------------------------------------------------------
# O CNPJ PREENCHE O CADASTRO, e o nome oficial normaliza o que já está aqui
#
# Pedido do dono, 21/09/2026: *"preciso normalizar o nome do fornecedor através
# de consulta CNPJ, e ainda que após digitação do CNPJ sejam pesquisados os
# dados para serem pré-preenchidos (…) quando sigo após a digitação, falo no
# cadastro."*
#
# A consulta de verdade não roda nesta sessão (o serviço externo está fora do
# alcance daqui), então ela é dublada. O que estes testes seguram é o que está
# em volta dela — e é lá que mora o estrago possível: cadastrar o mesmo CNPJ
# duas vezes, sobrescrever o que a pessoa digitou, ou trocar a razão social
# sem deixar rastro de qual era.
# ---------------------------------------------------------------------------
RESPOSTA_DA_RECEITA = {
    "cnpj": "34028316000103",
    "razao_social": "EMPRESA BRASILEIRA DE CORREIOS E TELEGRAFOS",
    "nome_fantasia": "CORREIOS",
    "situacao": "ATIVA",
    "municipio": "BRASILIA", "uf": "DF", "cep": "70002900",
    "logradouro": "SBN QUADRA 1", "numero": "S/N", "bairro": "ASA NORTE",
    "cnae_principal": "5310501",
    "email": "contador@escritorio.com.br", "telefone": "6134268000",
}


@pytest.fixture
def receita_dublada(monkeypatch):
    from app.apps.erp.core.cadastros import receita

    def falsa(cnpj):
        return dict(RESPOSTA_DA_RECEITA)
    monkeypatch.setattr(receita, "consultar", falsa)
    return receita


def test_a_consulta_preenche_o_formulario_com_o_que_a_receita_tem(
        cenario, receita_dublada):
    s = cenario["s"]
    r = svc_forn.consultar_para_cadastro(s, "34.028.316/0001-03")

    assert r["ja_cadastrado"] is None
    assert r["tipo_pessoa"] == "PJ"
    assert r["campos"]["razao_social"] == "EMPRESA BRASILEIRA DE CORREIOS E TELEGRAFOS"
    assert r["campos"]["municipio"] == "BRASILIA"
    assert r["campos"]["uf"] == "DF"


def test_a_consulta_NAO_traz_email_nem_telefone(cenario, receita_dublada):
    """O que está na Receita é o do contador, quase nunca o do vendedor. Um
    e-mail errado no cadastro faz a cotação sair para o lugar errado."""
    s = cenario["s"]
    campos = svc_forn.consultar_para_cadastro(s, "34028316000103")["campos"]
    assert "email" not in campos
    assert "telefone" not in campos


def test_cnpj_ja_cadastrado_avisa_em_vez_de_deixar_digitar_tudo(
        cenario, receita_dublada):
    """Sem isto, a pessoa preenche o formulário inteiro para o banco recusar
    no fim — e foi assim que nasceu boa parte dos documentos repetidos."""
    s, chefe = cenario["s"], cenario["chefe"]
    f = _fornecedor(s, doc="34028316000103", nome="CORREIOS JA CADASTRADO")

    r = svc_forn.consultar_para_cadastro(s, "34028316000103")
    assert r["ja_cadastrado"]["id"] == f.id
    assert "já está cadastrado" in r["aviso"]
    assert r["campos"] == {}, "nem consulta a Receita: já sabe que não serve"


def test_documento_com_tamanho_errado_e_recusado(cenario, receita_dublada):
    with pytest.raises(ErroValidacao):
        svc_forn.consultar_para_cadastro(cenario["s"], "123")


def test_cpf_diz_que_nao_tem_consulta_em_vez_de_ficar_girando(
        cenario, receita_dublada):
    r = svc_forn.consultar_para_cadastro(cenario["s"], "11144477735")
    assert r["tipo_pessoa"] == "PF"
    assert r["campos"] == {}
    assert "CPF" in r["aviso"]


def test_receita_fora_do_ar_nao_quebra_o_cadastro(cenario, monkeypatch):
    """Serviço externo cai. O cadastro não pode cair junto: a pessoa preenche
    à mão e acerta depois."""
    from app.apps.erp.core.cadastros import receita

    def caiu(cnpj):
        raise receita.ReceitaIndisponivel("tempo esgotado")
    monkeypatch.setattr(receita, "consultar", caiu)

    r = svc_forn.consultar_para_cadastro(cenario["s"], "34028316000103")
    assert r["campos"] == {}
    assert "à mão" in r["aviso"]


def test_cnpj_baixado_na_receita_vem_com_aviso(cenario, monkeypatch):
    from app.apps.erp.core.cadastros import receita
    monkeypatch.setattr(receita, "consultar",
                        lambda c: {**RESPOSTA_DA_RECEITA, "situacao": "BAIXADA"})

    r = svc_forn.consultar_para_cadastro(cenario["s"], "34028316000103")
    assert "BAIXADA" in r["aviso"]
    assert r["campos"], "mesmo baixada, os campos vêm — quem decide é quem lê"


# --- normalizar o nome de quem já está cadastrado --------------------------
def test_adotar_o_nome_oficial_guarda_o_antigo_como_fantasia(cenario):
    """"MADEIREIRA SÃO JOSÉ" é como o comprador reconhece a empresa. Perder
    isso encheria a lista de cotação de razões sociais que ninguém liga a
    ninguém."""
    s, chefe = cenario["s"], cenario["chefe"]
    f = _fornecedor(s, doc="34028316000103", nome="MADEIREIRA SAO JOSE")
    f.razao_social_rfb = "J G DA SILVA COMERCIO DE MADEIRAS EIRELI"
    s.flush()

    r = svc_forn.adotar_nome_oficial(s, f.id, chefe)
    s.flush()

    assert f.razao_social == "J G DA SILVA COMERCIO DE MADEIRAS EIRELI"
    assert f.nome_fantasia == "MADEIREIRA SAO JOSE"
    assert f.razao_social_rfb is None, "some do filtro depois de resolvido"
    assert "MADEIREIRA SAO JOSE" in r["recado"]


def test_adotar_o_nome_oficial_nao_apaga_um_fantasia_que_ja_existe(cenario):
    s, chefe = cenario["s"], cenario["chefe"]
    f = _fornecedor(s, doc="34028316000103", nome="MADEIREIRA SAO JOSE")
    f.nome_fantasia = "MADEMAX"
    f.razao_social_rfb = "J G DA SILVA COMERCIO DE MADEIRAS EIRELI"
    s.flush()

    svc_forn.adotar_nome_oficial(s, f.id, chefe)
    s.flush()
    assert f.nome_fantasia == "MADEMAX"


def test_sem_divergencia_guardada_nao_da_para_adotar(cenario):
    s, chefe = cenario["s"], cenario["chefe"]
    f = _fornecedor(s, doc="34028316000103")
    with pytest.raises(ErroValidacao) as e:
        svc_forn.adotar_nome_oficial(s, f.id, chefe)
    assert "Acertar o cadastro pela Receita" in str(e.value)


def test_o_fornecedor_com_nome_divergente_aparece_no_indicador(cenario):
    s = cenario["s"]
    f = _fornecedor(s, doc="34028316000103", nome="NOME ANTIGO LTDA")
    f.razao_social_rfb = "NOME OFICIAL LTDA"
    s.flush()

    d = svc_forn.gerenciar(s)
    linha = [x for x in d["fornecedores"] if x["id"] == f.id][0]
    assert linha["razao_social_rfb"] == "NOME OFICIAL LTDA"
    assert d["indicadores"]["nome_diferente"] >= 1
