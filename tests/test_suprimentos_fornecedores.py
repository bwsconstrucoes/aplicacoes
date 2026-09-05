"""O fornecedor visto por Suprimentos: o que vende, de onde atende, por onde
recebe cotação e com quem se fala.

A tela de gestão existe por causa de três números, e é sobre eles que estes
testes insistem: fornecedor SEM CATEGORIA, SEM CONTATO e SEM E-MAIL é
fornecedor que não vai receber a próxima cotação — e ninguém percebe até a
cotação voltar com três preços em vez de seis.

O que não pode falhar:
  - marcar e desmarcar categorias substitui a lista inteira, sem sobra;
  - categoria inexistente é recusada em vez de criada;
  - canal de cotação desconhecido é recusado;
  - quem não tem categoria nenhuma NÃO entra na lista de quem cotar;
  - os indicadores contam o que está incompleto.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao
from app.apps.erp.core.suprimentos import fornecedores as svc
from app.apps.erp.db.models.cadastros import (
    Fornecedor, FornecedorCategoria, FornecedorContato, FornecedorPorte,
    InsumoCategoria, PerfilUsuario as P, RegimeTributario, TipoPessoa,
)

from conftest import SessaoFalsa, novo_usuario

ADMIN = novo_usuario(1, P.ADMIN, nome="Marcelo")


def fornecedor(id_, razao, **extra):
    padrao = dict(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf=f"{id_:014d}",
                  razao_social=razao, regime_tributario=RegimeTributario.NAO_INFORMADO,
                  ativo=True, e_fornecedor=True, e_cliente=False,
                  regioes_atuacao=[], canais_cotacao=["EMAIL"], email=None,
                  telefone=None, municipio=None, uf=None, nome_fantasia=None,
                  porte=None)
    padrao.update(extra)
    return Fornecedor(id=id_, **padrao)


CATEGORIAS = [InsumoCategoria(id=1, codigo="CAT-0001", nome="Cimento", ativo=True),
              InsumoCategoria(id=2, codigo="CAT-0002", nome="Aço", ativo=True)]


# ---------------------------------------------------------------------------
# O que o fornecedor vende
# ---------------------------------------------------------------------------
def test_marcar_categoria_nova_acrescenta():
    f = fornecedor(1, "CIMENTOS LTDA")
    s = SessaoFalsa(f, *CATEGORIAS, ADMIN)
    assert svc.definir_categorias(s, f, [1]) == [1]
    assert [v.categoria_insumo_id for v in s.adicionados] == [1]


def test_desmarcar_categoria_remove_a_ligacao():
    f = fornecedor(1, "CIMENTOS LTDA")
    ligacao = FornecedorCategoria(fornecedor_id=1, categoria_insumo_id=1)
    s = SessaoFalsa(f, ligacao, *CATEGORIAS, ADMIN)
    assert svc.definir_categorias(s, f, []) == []
    assert s.removidos == [ligacao]


def test_a_ligacao_de_outro_fornecedor_nao_e_tocada():
    f = fornecedor(1, "CIMENTOS LTDA")
    de_outro = FornecedorCategoria(fornecedor_id=2, categoria_insumo_id=1)
    s = SessaoFalsa(f, de_outro, *CATEGORIAS, ADMIN)
    svc.definir_categorias(s, f, [])
    assert s.removidos == [], "mexer no fornecedor 1 não pode alterar o 2"


def test_categoria_inexistente_e_recusada_em_vez_de_criada():
    f = fornecedor(1, "CIMENTOS LTDA")
    s = SessaoFalsa(f, *CATEGORIAS, ADMIN)
    with pytest.raises(ErroValidacao, match="Categoria de insumo inexistente"):
        svc.definir_categorias(s, f, [99])


# ---------------------------------------------------------------------------
# Porte, região e canal
# ---------------------------------------------------------------------------
def test_canal_desconhecido_e_recusado():
    f = fornecedor(1, "CIMENTOS LTDA")
    s = SessaoFalsa(f, *CATEGORIAS, ADMIN)
    with pytest.raises(ErroValidacao, match="Canal de cotação desconhecido"):
        svc.editar(s, 1, {"canais_cotacao": ["POMBO"]}, ADMIN)


def test_porte_desconhecido_e_recusado():
    f = fornecedor(1, "CIMENTOS LTDA")
    s = SessaoFalsa(f, *CATEGORIAS, ADMIN)
    with pytest.raises(ErroValidacao, match="Porte desconhecido"):
        svc.editar(s, 1, {"porte": "GIGANTE"}, ADMIN)


def test_porte_em_branco_limpa_o_campo():
    f = fornecedor(1, "CIMENTOS LTDA", porte=FornecedorPorte.FABRICA)
    s = SessaoFalsa(f, *CATEGORIAS, ADMIN)
    svc.editar(s, 1, {"porte": ""}, ADMIN)
    assert f.porte is None


def test_as_regioes_sao_normalizadas_e_sem_repetição():
    f = fornecedor(1, "CIMENTOS LTDA")
    s = SessaoFalsa(f, *CATEGORIAS, ADMIN)
    svc.editar(s, 1, {"regioes_atuacao": [" ce ", "CE", "rmf", ""]}, ADMIN)
    assert f.regioes_atuacao == ["CE", "RMF"]


def test_fornecedor_inexistente_responde_nao_encontrado():
    with pytest.raises(ErroNaoEncontrado):
        svc.editar(SessaoFalsa(ADMIN), 99, {"porte": "FABRICA"}, ADMIN)


# ---------------------------------------------------------------------------
# Contatos — o cotador
# ---------------------------------------------------------------------------
def test_contato_sem_nome_de_gente_e_recusado():
    f = fornecedor(1, "CIMENTOS LTDA")
    s = SessaoFalsa(f, *CATEGORIAS, ADMIN)
    with pytest.raises(ErroValidacao, match="nome de quem responde"):
        svc.acrescentar_contato(s, 1, {"nome": "x"}, ADMIN)


def test_contato_sem_e_mail_nem_telefone_e_recusado():
    """O banco também recusa (ck_contato_tem_canal). Recusar aqui é o que dá o
    recado em português, em vez de "violates check constraint"."""
    f = fornecedor(1, "CIMENTOS LTDA")
    s = SessaoFalsa(f, *CATEGORIAS, ADMIN)
    with pytest.raises(ErroValidacao, match="e-mail ou o telefone"):
        svc.acrescentar_contato(s, 1, {"nome": "Ricardo Alves"}, ADMIN)


def test_contato_e_acrescentado_com_funcao_e_e_mail():
    f = fornecedor(1, "CIMENTOS LTDA")
    s = SessaoFalsa(f, *CATEGORIAS, ADMIN)
    c = svc.acrescentar_contato(s, 1, {"nome": "Ricardo Alves", "funcao": "vendas",
                                       "email": "ricardo@exemplo.com"}, ADMIN)
    assert c.nome == "Ricardo Alves"
    assert c.funcao == "vendas"


def test_remover_contato_inexistente_responde_nao_encontrado():
    with pytest.raises(ErroNaoEncontrado):
        svc.remover_contato(SessaoFalsa(ADMIN), 99, ADMIN)


# ---------------------------------------------------------------------------
# A tela de gestão
# ---------------------------------------------------------------------------
def _cenario_de_gestao():
    return SessaoFalsa(
        fornecedor(1, "CIMENTOS LTDA", porte=FornecedorPorte.FABRICA,
                   regioes_atuacao=["CE"], canais_cotacao=["EMAIL"],
                   email="v@cimentos.com", municipio="Fortaleza", uf="CE"),
        fornecedor(2, "ACOS LTDA", porte=FornecedorPorte.DISTRIBUIDOR,
                   regioes_atuacao=["RMF"], canais_cotacao=["EMAIL"], email=None),
        fornecedor(3, "SEM CATEGORIA LTDA", canais_cotacao=["WHATSAPP"]),
        fornecedor(4, "APOSENTADO LTDA", ativo=False),
        FornecedorCategoria(fornecedor_id=1, categoria_insumo_id=1),
        FornecedorCategoria(fornecedor_id=2, categoria_insumo_id=2),
        FornecedorContato(id=1, fornecedor_id=1, nome="Ricardo", funcao="vendas"),
        *CATEGORIAS, ADMIN)


def test_os_indicadores_apontam_quem_nao_recebe_cotacao():
    k = svc.gerenciar(_cenario_de_gestao())["indicadores"]
    assert k["total"] == 4
    assert k["ativos"] == 3
    assert k["inativos"] == 1
    assert k["sem_categoria"] == 1, "o 'SEM CATEGORIA LTDA'"
    assert k["sem_contato"] == 2, "só o CIMENTOS tem gente"
    assert k["sem_email"] == 1, "o ACOS recebe por e-mail e não tem e-mail"


def test_a_tela_recebe_as_listas_para_os_filtros():
    d = svc.gerenciar(_cenario_de_gestao())
    assert d["regioes"] == ["CE", "RMF"]
    assert {p["chave"] for p in d["portes"]} >= {"FABRICA", "DISTRIBUIDOR"}
    assert {c["chave"] for c in d["canais"]} >= {"EMAIL", "WHATSAPP"}


def test_o_fornecedor_que_nao_e_fornecedor_fica_de_fora():
    s = SessaoFalsa(fornecedor(1, "SÓ CLIENTE LTDA", e_fornecedor=False),
                    *CATEGORIAS, ADMIN)
    assert svc.gerenciar(s)["fornecedores"] == []


# ---------------------------------------------------------------------------
# Quem entra na cotação
# ---------------------------------------------------------------------------
def test_quem_nao_tem_categoria_nao_e_sugerido_para_cotar():
    nomes = [f["razao_social"] for f in svc.para_cotar(_cenario_de_gestao(), [1, 2])]
    assert "SEM CATEGORIA LTDA" not in nomes


def test_cotar_uma_categoria_traz_so_quem_a_vende():
    nomes = [f["razao_social"] for f in svc.para_cotar(_cenario_de_gestao(), [1])]
    assert nomes == ["CIMENTOS LTDA"]


def test_o_inativo_nunca_entra():
    nomes = [f["razao_social"] for f in svc.para_cotar(_cenario_de_gestao(), [])]
    assert "APOSENTADO LTDA" not in nomes


def test_filtrar_por_regiao_respeita_quem_nao_declarou_nenhuma():
    """Fornecedor sem região declarada atende qualquer uma — declarar região é
    um recorte, não uma obrigação."""
    s = SessaoFalsa(
        fornecedor(1, "SÓ NO CEARÁ LTDA", regioes_atuacao=["CE"]),
        fornecedor(2, "SEM RECORTE LTDA", regioes_atuacao=[]),
        FornecedorCategoria(fornecedor_id=1, categoria_insumo_id=1),
        FornecedorCategoria(fornecedor_id=2, categoria_insumo_id=1),
        *CATEGORIAS, ADMIN)
    nomes = [f["razao_social"] for f in svc.para_cotar(s, [1], regiao="PE")]
    assert nomes == ["SEM RECORTE LTDA"]
