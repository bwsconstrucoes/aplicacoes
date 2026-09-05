"""A proposta do fornecedor vira a coluna do mapa.

As respostas chegam como PDF, foto do WhatsApp, e-mail ou texto colado, cada
fornecedor com a sua nomenclatura. A IA aqui tem um problema mais fácil do que
parece: ela não precisa adivinhar o que é o material, precisa casar o que veio
com os itens QUE JÁ ESTÃO NO MAPA.

O que não pode falhar:
  - item que não casa volta ESCRITO na resposta, e não vira preço em linha
    errada — preço na linha errada é a compra do material errado;
  - "Vergalhão CA50" e "Vergalhão CA60" não podem se confundir: a especificação
    entra na comparação;
  - preço que não dá para ler volta vazio, sem chute;
  - a leitura não grava preço nenhum — devolve sugestão;
  - o arquivo fica anexado à coluna do fornecedor, como prova para a hora de
    autorizar.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao
from app.apps.erp.core.suprimentos import proposta
from app.apps.erp.db.models.cadastros import (
    Cotacao, CotacaoFornecedor, CotacaoItem, CotacaoPreco, Insumo,
    PerfilUsuario as P, StatusCotacao, SuprimentoItem,
)

from conftest import SessaoFalsa, novo_usuario

COMPRADOR = novo_usuario(1, P.ADMIN)


def _mapa():
    """Um mapa com dois itens parecidos — CA50 e CA60 — para provar que a
    especificação separa os dois."""
    return [
        Cotacao(id=1, numero="COT-0001", titulo="armadura",
                status=StatusCotacao.ABERTA, criado_por=1),
        CotacaoFornecedor(id=21, cotacao_id=1, fornecedor_id=100),
        Insumo(id=10, codigo="INS-0010", descricao="Vergalhão CA50 12.5mm"),
        Insumo(id=11, codigo="INS-0011", descricao="Vergalhão CA60 8mm"),
        SuprimentoItem(id=7, solicitacao_id=1, numero=1, insumo_id=10,
                       especificacao="12.5mm", quantidade=Decimal("100"),
                       unidade="M", obra_id=1),
        SuprimentoItem(id=8, solicitacao_id=1, numero=2, insumo_id=11,
                       especificacao="8mm", quantidade=Decimal("50"),
                       unidade="M", obra_id=1),
        CotacaoItem(id=11, cotacao_id=1, suprimento_item_id=7, numero=1),
        CotacaoItem(id=12, cotacao_id=1, suprimento_item_id=8, numero=2),
    ]


@pytest.fixture
def ia(monkeypatch):
    def falsa(*, texto="", imagens=None, dica=""):
        return falsa.resposta
    falsa.resposta = {"itens": []}
    import app.apps.erp.core.documentos.leitor as leitor
    monkeypatch.setattr(leitor, "_chamar_ia", falsa)
    return falsa


def test_casa_cada_item_da_proposta_com_a_linha_do_mapa(ia):
    ia.resposta = {"itens": [
        {"descricao": "VERGALHAO CA50 12,5MM", "valor": "38,50"},
        {"descricao": "VERGALHAO CA60 8MM", "valor": "42,00"},
    ], "observacoes": "Pagamento 30/60 dias, frete por nossa conta."}
    s = SessaoFalsa(COMPRADOR, *_mapa())

    r = proposta.ler(s, 21, texto="proposta colada", usuario=COMPRADOR)

    por_linha = {x["cotacao_item_id"]: x for x in r["sugestoes"]}
    assert por_linha[11]["preco"] == "38.50"
    assert por_linha[12]["preco"] == "42.00", (
        "CA50 e CA60 só se distinguem pela especificação")
    assert r["nao_casados"] == []
    assert "30/60" in r["condicoes_lidas"]


def test_item_que_nao_esta_no_mapa_volta_escrito(ia):
    """Preço em linha errada é a compra do material errado — e ninguém percebe
    até chegar na obra."""
    ia.resposta = {"itens": [{"descricao": "Cimento CP-II 50kg", "valor": "39,90"}]}
    s = SessaoFalsa(COMPRADOR, *_mapa())

    r = proposta.ler(s, 21, texto="proposta colada", usuario=COMPRADOR)

    assert r["sugestoes"] == []
    assert r["nao_casados"][0]["descricao"] == "Cimento CP-II 50kg"
    assert "1 sem correspondência" in r["resumo"]


def test_preco_ilegivel_volta_vazio_e_separado(ia):
    ia.resposta = {"itens": [{"descricao": "VERGALHAO CA50 12,5MM",
                              "valor": "sob consulta"}]}
    s = SessaoFalsa(COMPRADOR, *_mapa())

    r = proposta.ler(s, 21, texto="proposta", usuario=COMPRADOR)

    assert r["sugestoes"] == []
    assert r["sem_preco"][0]["cotacao_item_id"] == 11
    assert "sem preço legível" in r["resumo"]


@pytest.mark.parametrize("bruto,esperado", [
    ({"valor": "R$ 1.234,56"}, "1234.56"),
    ({"valor_unitario": "38.5"}, "38.5"),
    ({"preco": "42,00"}, "42.00"),
    ({"valor": "0"}, ""),
    ({"valor": ""}, ""),
])
def test_o_preco_e_lido_em_qualquer_um_dos_nomes(bruto, esperado):
    assert proposta._preco_unitario(bruto) == esperado


def test_a_leitura_nao_grava_preco_nenhum(ia):
    ia.resposta = {"itens": [{"descricao": "VERGALHAO CA50 12,5MM", "valor": "38,50"}]}
    s = SessaoFalsa(COMPRADOR, *_mapa())

    proposta.ler(s, 21, texto="proposta", usuario=COMPRADOR)

    assert not any(isinstance(o, CotacaoPreco) for o in s.adicionados), (
        "a IA sugere; quem confere e manda salvar é a pessoa")


def test_diz_quais_itens_do_mapa_o_fornecedor_nao_cotou(ia):
    ia.resposta = {"itens": [{"descricao": "VERGALHAO CA50 12,5MM", "valor": "38,50"}]}
    s = SessaoFalsa(COMPRADOR, *_mapa())

    r = proposta.ler(s, 21, texto="proposta", usuario=COMPRADOR)

    assert r["itens_do_mapa_sem_proposta"] == [12]


def test_sem_arquivo_e_sem_texto_recusa(ia):
    s = SessaoFalsa(COMPRADOR, *_mapa())
    with pytest.raises(ErroValidacao, match="Anexe a proposta"):
        proposta.ler(s, 21, usuario=COMPRADOR)


def test_fornecedor_fora_do_mapa_responde_nao_encontrado(ia):
    with pytest.raises(ErroNaoEncontrado):
        proposta.ler(SessaoFalsa(COMPRADOR), 999, texto="x" * 20, usuario=COMPRADOR)


def test_falha_da_ia_vira_recado(monkeypatch):
    import app.apps.erp.core.documentos.leitor as leitor

    def explode(**kw):
        raise leitor.ErroLeitura("modelo indisponível")
    monkeypatch.setattr(leitor, "_chamar_ia", explode)
    s = SessaoFalsa(COMPRADOR, *_mapa())

    with pytest.raises(ErroValidacao, match="Não consegui ler a proposta"):
        proposta.ler(s, 21, texto="proposta", usuario=COMPRADOR)


def test_o_consumo_de_ia_e_registrado(ia, monkeypatch):
    import contextlib
    import app.apps.erp.core.comum.ia_custo as ia_custo
    vistos = []
    original = ia_custo.contexto

    @contextlib.contextmanager
    def espiao(**kw):
        vistos.append(kw)
        with original(**kw):
            yield
    monkeypatch.setattr(ia_custo, "contexto", espiao)
    ia.resposta = {"itens": []}

    proposta.ler(SessaoFalsa(COMPRADOR, *_mapa()), 21, texto="proposta",
                 usuario=COMPRADOR)

    assert vistos and vistos[0]["operacao"] == "proposta_cotacao"


def test_o_arquivo_fica_anexado_a_coluna_do_fornecedor(ia, monkeypatch):
    """Na hora de autorizar, dá para abrir a proposta original e conferir se os
    preços lançados batem com o que o fornecedor mandou."""
    import app.apps.erp.core.documentos.armazenamento as arm
    from app.apps.erp.db.models.financeiro import Anexo
    guardados = []

    def salvar_falso(s, conteudo, nome, **kw):
        guardados.append({"nome": nome, **kw})
        anexo = Anexo(id=77, entidade_tipo=kw["entidade_tipo"],
                      entidade_id=kw["entidade_id"], nome_arquivo=nome,
                      hash_sha256="x")
        return anexo
    monkeypatch.setattr(arm, "salvar", salvar_falso)

    import app.apps.erp.core.documentos.leitor as leitor
    monkeypatch.setattr(leitor, "ler_documento",
                        lambda c, n, **kw: {"itens": [], "observacoes": ""})
    s = SessaoFalsa(COMPRADOR, *_mapa())
    coluna = next(o for o in s.objetos if isinstance(o, CotacaoFornecedor))

    r = proposta.ler(s, 21, conteudo=b"%PDF-1.4 fake", nome_arquivo="proposta.pdf",
                     usuario=COMPRADOR)

    assert guardados[0]["entidade_tipo"] == "cotacao_fornecedor"
    assert guardados[0]["entidade_id"] == 21
    assert guardados[0]["categoria"] == "PROPOSTA"
    assert r["anexo_id"] == 77 and coluna.anexo_id == 77
