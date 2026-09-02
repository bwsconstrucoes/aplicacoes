"""Escopo por obra e por autoria — provado com banco de verdade.

O dublê de sessão ignora o WHERE, e o escopo É um WHERE. Aqui o Postgres é
real (descartável, ver conftest): obras, operadores e títulos são criados
pelo próprio teste, e cada teste roda numa transação desfeita no fim.

O que se prova, e que nenhum teste sem banco consegue provar:

  1. A listagem devolve EXATAMENTE o conjunto certo para cada perfil e cada
     configuração de alcance — nem um título a mais, nem a menos.
  2. Listagem e detalhe CONCORDAM: para todo título, ou ele aparece na lista
     e o detalhe abre, ou não aparece e o detalhe responde "não encontrado".
     É a promessa de `aplicar_escopo` único, e só o SQL executado a prova.
  3. As ROTAS HTTP reais fazem a mesma coisa — não só as funções do core.
  4. Mudar o alcance de UMA pessoa muda lista e detalhe dela juntos, e uma
     lista de obras vazia não vira "vê tudo".

Cenário (2 obras, 7 operadores, 4 títulos):

    T1  lançado pelo administrativo "só os meus"   · rateado na obra A
    T2  lançado pelo gestor                         · rateado na obra A
    T3  lançado pelo gestor                         · rateado na obra B
    T4  lançado pelo administrativo "obras designadas" (preso à A) · rateado na B
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

import pytest

from app.apps.erp.core.auth import permissoes
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado
from app.apps.erp.db.models.cadastros import (
    Categoria, EscopoVisao, Fornecedor, Obra, PerfilUsuario as P,
    RegimeTributario, TipoPessoa, Usuario, UsuarioObra,
)
from app.apps.erp.db.models.financeiro import (
    FormaPagamento, Parcela, Rateio, StatusTitulo, TipoTitulo, Titulo,
)
from sqlalchemy import select

from conftest import como

pytestmark = pytest.mark.banco

SENHA = gerar_hash("senha-de-teste-123")


@dataclass
class Cenario:
    obra_a: Obra
    obra_b: Obra
    usuarios: dict[str, Usuario] = field(default_factory=dict)
    titulos: dict[str, Titulo] = field(default_factory=dict)

    def id(self, nome: str) -> int:
        return self.titulos[nome].id

    def ids(self, *nomes: str) -> set[int]:
        return {self.titulos[n].id for n in nomes}


def _usuario(s, chave, perfil, *, escopo=None, obras=()):
    u = Usuario(nome=f"Teste {chave}", email=f"{chave}@teste.bws.local",
                senha_hash=SENHA, perfil=perfil,
                escopo_visao=escopo or EscopoVisao.PROPRIOS)
    s.add(u)
    s.flush()
    for o in obras:
        s.add(UsuarioObra(usuario_id=u.id, obra_id=o.id))
    s.flush()
    return u


def _titulo(s, chave, *, solicitante, obra, fornecedor, categoria):
    t = Titulo(numero_sp=f"SP-TESTE-{chave}", tipo=TipoTitulo.T1_MATERIAL_NFE,
               fornecedor_id=fornecedor.id, descricao=f"Título {chave}",
               valor_bruto=Decimal("100.00"), valor_liquido=Decimal("100.00"),
               competencia=date(2026, 9, 1), categoria_id=categoria.id,
               forma_pagamento=FormaPagamento.PIX, status=StatusTitulo.APROVADO,
               solicitante_id=solicitante.id)
    s.add(t)
    s.flush()
    s.add(Parcela(titulo_id=t.id, numero=1, vencimento=date(2026, 9, 30),
                  valor=Decimal("100.00")))
    s.add(Rateio(titulo_id=t.id, obra_id=obra.id, valor=Decimal("100.00")))
    s.flush()
    return t


@pytest.fixture
def cenario(sessao_real) -> Cenario:
    s = sessao_real
    a = Obra(codigo="OBRA-A", nome="Obra A (teste)", status="ATIVA")
    b = Obra(codigo="OBRA-B", nome="Obra B (teste)", status="ATIVA")
    forn = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="00000000000191",
                      razao_social="Fornecedor de teste",
                      regime_tributario=RegimeTributario.NAO_INFORMADO)
    cat = Categoria(codigo="9.9.99", descricao="Conta de teste")
    s.add_all([a, b, forn, cat])
    s.flush()

    c = Cenario(obra_a=a, obra_b=b)
    u = c.usuarios
    u["admin"] = _usuario(s, "admin", P.ADMIN)
    u["gestor"] = _usuario(s, "gestor", P.GESTOR_OBRA)
    u["supervisor"] = _usuario(s, "supervisor", P.SUPERVISOR_OBRA, obras=[a])
    u["adm_proprios"] = _usuario(s, "adm-proprios", P.ADMINISTRATIVO_OBRA)
    u["adm_obras"] = _usuario(s, "adm-obras", P.ADMINISTRATIVO_OBRA,
                              escopo=EscopoVisao.OBRAS_DESIGNADAS, obras=[a])
    u["lancador_sem_obra"] = _usuario(s, "lancador", P.LANCADOR,
                                      escopo=EscopoVisao.OBRAS_DESIGNADAS)
    u["dp"] = _usuario(s, "dp", P.DEPARTAMENTO_PESSOAL)

    t = c.titulos
    t["T1"] = _titulo(s, "T1", solicitante=u["adm_proprios"], obra=a, fornecedor=forn, categoria=cat)
    t["T2"] = _titulo(s, "T2", solicitante=u["gestor"], obra=a, fornecedor=forn, categoria=cat)
    t["T3"] = _titulo(s, "T3", solicitante=u["gestor"], obra=b, fornecedor=forn, categoria=cat)
    t["T4"] = _titulo(s, "T4", solicitante=u["adm_obras"], obra=b, fornecedor=forn, categoria=cat)
    s.commit()                      # libera o savepoint; a transação de fora segue aberta
    return c


# O que cada um TEM de enxergar. Esta tabela é a especificação do escopo.
ESPERADO = {
    "admin":             {"T1", "T2", "T3", "T4"},
    "gestor":            {"T1", "T2", "T3", "T4"},
    "supervisor":        {"T1", "T2"},              # tudo da obra A
    "adm_proprios":      {"T1"},                    # só o que lançou
    "adm_obras":         {"T1", "T2", "T4"},        # obra A + o que lançou (T4, na B)
    "lancador_sem_obra": set(),                     # ampliado sem obra = só autoria = nada
    "dp":                set(),                     # filtra por autoria; não lançou nada
}


def _visiveis_no_sql(s, usuario) -> set[int]:
    stmt = permissoes.aplicar_escopo(select(Titulo.id), s, usuario)
    return set(s.scalars(stmt).all())


# ---------------------------------------------------------------------------
# 1. A listagem devolve exatamente o conjunto certo
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("quem", sorted(ESPERADO))
def test_listagem_devolve_exatamente_o_escopo(sessao_real, cenario, quem):
    assert _visiveis_no_sql(sessao_real, cenario.usuarios[quem]) == cenario.ids(*ESPERADO[quem])


# ---------------------------------------------------------------------------
# 2. Listagem e detalhe concordam — para TODO título, para TODO perfil
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("quem", sorted(ESPERADO))
def test_detalhe_concorda_com_a_listagem(sessao_real, cenario, quem):
    usuario = cenario.usuarios[quem]
    visiveis = _visiveis_no_sql(sessao_real, usuario)
    for nome, titulo in cenario.titulos.items():
        na_lista = titulo.id in visiveis
        assert permissoes.pode_ver_titulo(sessao_real, usuario, titulo.id) is na_lista, (
            f"{quem} × {nome}: lista diz {na_lista}, detalhe diz o contrário")


def test_parcela_herda_o_escopo_do_titulo(sessao_real, cenario):
    sup = cenario.usuarios["supervisor"]
    parcela_a = sessao_real.scalars(select(Parcela).where(
        Parcela.titulo_id == cenario.id("T2"))).one()
    parcela_b = sessao_real.scalars(select(Parcela).where(
        Parcela.titulo_id == cenario.id("T3"))).one()

    permissoes.exigir_parcela_no_escopo(sessao_real, sup, parcela_a.id)   # não levanta
    with pytest.raises(ErroNaoEncontrado):
        permissoes.exigir_parcela_no_escopo(sessao_real, sup, parcela_b.id)


# ---------------------------------------------------------------------------
# 3. As rotas HTTP reais — o que o navegador recebe
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("quem", sorted(ESPERADO))
def test_rota_de_listagem_e_detalhe(app_real, cenario, quem):
    c = como(app_real, cenario.usuarios[quem].id)

    r = c.get("/erp/api/titulos")
    assert r.status_code == 200, r.get_json()
    listados = {t["id"] for t in r.get_json()["titulos"]}
    assert listados == cenario.ids(*ESPERADO[quem])

    for nome, titulo in cenario.titulos.items():
        r = c.get(f"/erp/api/titulos/{titulo.id}")
        if nome in ESPERADO[quem]:
            assert r.status_code == 200, f"{quem} devia abrir {nome}: {r.get_json()}"
        else:
            assert r.status_code == 404, f"{quem} NÃO devia abrir {nome}"
            assert "não encontrado" in (r.get_json() or {}).get("erro", "").lower()


def test_rota_da_obra_fora_do_escopo_responde_nao_encontrado(app_real, cenario):
    sup = como(app_real, cenario.usuarios["supervisor"].id)
    assert sup.get(f"/erp/api/obras/{cenario.obra_a.id}").status_code == 200
    assert sup.get(f"/erp/api/obras/{cenario.obra_b.id}").status_code == 404
    assert sup.get(f"/erp/api/obras/{cenario.obra_b.id}/titulos").status_code == 404

    # quem enxerga por autoria não fica preso a obra: abre qualquer uma
    adm = como(app_real, cenario.usuarios["adm_proprios"].id)
    assert adm.get(f"/erp/api/obras/{cenario.obra_b.id}").status_code == 200

    # quem foi AMPLIADO para obras designadas passa a ficar preso a elas
    ampliado = como(app_real, cenario.usuarios["adm_obras"].id)
    assert ampliado.get(f"/erp/api/obras/{cenario.obra_a.id}").status_code == 200
    assert ampliado.get(f"/erp/api/obras/{cenario.obra_b.id}").status_code == 404


def test_anexo_herda_o_escopo_do_titulo(app_real, sessao_real, cenario):
    from app.apps.erp.core.documentos.armazenamento import salvar
    anexo = salvar(sessao_real, b"%PDF-1.4 conteudo de teste", "nota.pdf",
                   entidade_tipo="titulo", entidade_id=cenario.id("T3"),
                   usuario=cenario.usuarios["gestor"])
    sessao_real.commit()

    assert como(app_real, cenario.usuarios["gestor"].id).get(
        f"/erp/anexo/{anexo.id}").status_code == 200
    assert como(app_real, cenario.usuarios["supervisor"].id).get(
        f"/erp/anexo/{anexo.id}").status_code == 404
    assert como(app_real, cenario.usuarios["adm_proprios"].id).get(
        f"/erp/anexo/{anexo.id}").status_code == 404


# ---------------------------------------------------------------------------
# 4. Mudar o alcance de UMA pessoa muda lista e detalhe dela — juntos
# ---------------------------------------------------------------------------
def test_ampliar_o_alcance_muda_lista_e_detalhe_juntos(app_real, sessao_real, cenario):
    adm = cenario.usuarios["adm_proprios"]
    c = como(app_real, adm.id)

    # antes: só o que lançou
    assert {t["id"] for t in c.get("/erp/api/titulos").get_json()["titulos"]} == cenario.ids("T1")
    assert c.get(f"/erp/api/titulos/{cenario.id('T2')}").status_code == 404

    # ampliado SEM obra associada: nada muda — lista vazia não vira "vê tudo"
    adm.escopo_visao = EscopoVisao.OBRAS_DESIGNADAS
    sessao_real.commit()
    assert {t["id"] for t in c.get("/erp/api/titulos").get_json()["titulos"]} == cenario.ids("T1")
    assert c.get(f"/erp/api/titulos/{cenario.id('T2')}").status_code == 404

    # ampliado COM a obra A: passa a ver o T2 (de outro, rateado na A) — e só ele
    sessao_real.add(UsuarioObra(usuario_id=adm.id, obra_id=cenario.obra_a.id))
    sessao_real.commit()
    assert {t["id"] for t in c.get("/erp/api/titulos").get_json()["titulos"]} == cenario.ids("T1", "T2")
    assert c.get(f"/erp/api/titulos/{cenario.id('T2')}").status_code == 200
    assert c.get(f"/erp/api/titulos/{cenario.id('T3')}").status_code == 404

    # e o detalhe continua concordando com a lista, título por título
    visiveis = _visiveis_no_sql(sessao_real, adm)
    for titulo in cenario.titulos.values():
        esperado = 200 if titulo.id in visiveis else 404
        assert c.get(f"/erp/api/titulos/{titulo.id}").status_code == esperado


def test_migracoes_todas_aplicadas_no_banco_de_teste(banco):
    """Se uma migração nova quebrar em banco vazio, é aqui que aparece."""
    from app.apps.erp.core.comum.migracoes import listar_estado
    estado = listar_estado()
    assert estado["pendentes"] == []
    assert len(estado["aplicadas"]) >= 30
