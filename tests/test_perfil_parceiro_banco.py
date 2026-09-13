"""O PARCEIRO: enxerga tudo da obra dele, e nada do resto da empresa.

Pedido do dono em 12/09/2026: *"tem um perfil que eu acho que vai precisar ser
criado, que é onde parceiro, e esse parceiro vai estar associado a alguma obra,
e o correto é que ele possa visualizar todas as informações referente à obra —
de financeiro, de DP, de contratos e etcétera — mas não visualizar o restante
da empresa"*.

Três decisões tomadas com ele, e que estes testes seguram:

  1. **Todo o custo da obra.** Ele escolheu a leitura larga sabendo do preço:
     o parceiro passa a enxergar por quanto a BWS compra naquela obra.
  2. **SÓ OLHA.** Nenhuma ação que grave. Há teste percorrendo a tabela de
     permissões inteira.
  3. **Sem obra designada, não vê nada.** O padrão NEGAR do ERP, dito em voz
     alta — e não o efeito colateral de uma lista vazia.

COM BANCO DE VERDADE porque o recorte por obra vive no `WHERE`, e o dublê da
suíte ignora `WHERE` — foi esse buraco que escondeu a brecha das Locações.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.apps.erp.core.arquivo import service as svc_arq
from app.apps.erp.core.auth.permissoes import (
    PERMISSOES, aplicar_escopo, condicao_escopo_sql, pode,
)
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.pessoal import listar_colaboradores
from app.apps.erp.core.relatorios import resumo
from app.apps.erp.db.models.cadastros import (
    Categoria, Colaborador, Empresa, EscopoVisao, Fornecedor, Obra,
    PerfilUsuario as P, RegimeTributario, TipoPessoa, Usuario, UsuarioObra,
)
from app.apps.erp.db.models.financeiro import (
    Anexo, Documento, DocumentoTipo, FormaPagamento, Parcela, Rateio,
    StatusTitulo, Titulo, TipoTitulo,
)

pytestmark = pytest.mark.banco


# ---------------------------------------------------------------------------
# O que NÃO precisa de banco: a alçada do perfil
# ---------------------------------------------------------------------------
def test_o_parceiro_so_olha():
    """Nenhuma ação que grave. Se alguém acrescentar uma, este teste cai."""
    de_escrita = {a for a in PERMISSOES if not a.startswith("ver_")}
    dadas = {a for a, perfis in PERMISSOES.items() if P.PARCEIRO in perfis}
    assert not (dadas & de_escrita), (
        f"o parceiro é de FORA da empresa e só olha; recebeu ação de escrita: "
        f"{sorted(dadas & de_escrita)}")


def test_o_parceiro_nao_ve_dado_bancario_nem_o_resto_da_empresa():
    fechadas = ("ver_dados_pagamento", "ver_contratos", "ver_notas",
                "ver_notas_emitidas", "ver_pedidos_compra", "ver_uso_da_equipe",
                "configurar", "gerir_usuarios")
    abertas = [a for a in fechadas if P.PARCEIRO in PERMISSOES.get(a, set())]
    assert not abertas, f"o parceiro alcançou o que não é da obra dele: {abertas}"


# ---------------------------------------------------------------------------
# O cenário: duas obras, um parceiro na primeira
# ---------------------------------------------------------------------------
@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    creche = Obra(codigo="CRECHE", nome="Creche do Eusébio")
    escola = Obra(codigo="ESCOLA", nome="Escola do Planalto")
    forn = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="11444777000161",
                      razao_social="CONSTRUTORA ALFA LTDA",
                      regime_tributario=RegimeTributario.NAO_INFORMADO)
    cat = Categoria(codigo="2.1.01", descricao="Material de construção",
                    grupo_codigo="2", grupo_nome="Custo de obra",
                    natureza="RESULTADO")
    aberto = DocumentoTipo(codigo="CONTRATO-OBRA", nome="Contrato da obra",
                           grupo="OBRA", dono="OBRA", sigilo="ABERTO")
    da_empresa = DocumentoTipo(codigo="CERTIDAO", nome="Certidão da empresa",
                               grupo="EMPRESA", dono="EMPRESA", sigilo="ABERTO")
    pessoal = DocumentoTipo(codigo="FOLHA", nome="Documento de pessoal",
                            grupo="PESSOAL", dono="PESSOA", sigilo="PESSOAL")
    bws = Empresa(razao_social="BWS Construções LTDA", cnpj="12345678000199")
    s.add_all([creche, escola, forn, cat, aberto, da_empresa, pessoal, bws])
    s.flush()

    def pessoa(nome, email, perfil, obra=None):
        u = Usuario(nome=nome, email=email,
                    senha_hash=gerar_hash("senha-de-teste-1234"), perfil=perfil,
                    escopo_visao=EscopoVisao.OBRAS_DESIGNADAS)
        s.add(u)
        s.flush()
        if obra is not None:
            s.add(UsuarioObra(usuario_id=u.id, obra_id=obra.id))
            s.flush()
        return u

    dono = pessoa("Marcelo", "chefe@teste.bws.local", P.ADMIN)
    parceiro = pessoa("Parceiro da creche", "parceiro@teste.bws.local",
                      P.PARCEIRO, creche)
    sem_obra = pessoa("Parceiro sem obra", "avulso@teste.bws.local", P.PARCEIRO)

    def titulo(sp, obra, valor):
        v = Decimal(valor)
        t = Titulo(numero_sp=sp, tipo=TipoTitulo.T1_MATERIAL_NFE,
                   fornecedor_id=forn.id, descricao="compra de material",
                   valor_bruto=v, valor_liquido=v, competencia=date(2026, 9, 1),
                   categoria_id=cat.id, forma_pagamento=FormaPagamento.PIX,
                   status=StatusTitulo.APROVADO, solicitante_id=dono.id)
        s.add(t)
        s.flush()
        s.add(Parcela(titulo_id=t.id, numero=1, vencimento=date(2026, 10, 1),
                      valor=v))
        s.add(Rateio(titulo_id=t.id, obra_id=obra.id, valor=v,
                     percentual=Decimal("100.0000")))
        s.flush()
        return t

    def arquivar(tipo, nome, obra=None, empresa=None):
        anexo = Anexo(entidade_tipo="documento", entidade_id=0,
                      nome_arquivo=nome, mime_type="application/pdf",
                      conteudo=b"x", hash_sha256=f"hash-{nome}",
                      guardado_em="BANCO")
        s.add(anexo)
        s.flush()
        d = Documento(tipo_codigo=tipo.codigo, anexo_id=anexo.id,
                      nome_padronizado=nome,
                      obra_id=obra.id if obra else None,
                      empresa_id=empresa.id if empresa else None)
        s.add(d)
        s.flush()
        return d

    def gente(nome, cpf, obra):
        c = Colaborador(nome=nome, cpf=cpf, regime="CLT", situacao="ATIVO",
                        obra_id=obra.id)
        s.add(c)
        s.flush()
        return c

    return {
        "s": s, "dono": dono, "parceiro": parceiro, "sem_obra": sem_obra,
        "creche": creche, "escola": escola,
        "t_creche": titulo("SP-CRE-1", creche, "1000.00"),
        "t_escola": titulo("SP-ESC-1", escola, "7000.00"),
        "doc_creche": arquivar(aberto, "Contrato Creche", creche),
        "doc_escola": arquivar(aberto, "Contrato Escola", escola),
        "doc_empresa": arquivar(da_empresa, "Certidão FGTS", empresa=bws),
        "doc_pessoal": arquivar(pessoal, "Acordo de jornada", creche),
        "pedreiro_creche": gente("José da Creche", "11111111111", creche),
        "pedreiro_escola": gente("Maria da Escola", "22222222222", escola),
    }


# ---------------------------------------------------------------------------
# Financeiro: vê o custo da obra dele, e só dele
# ---------------------------------------------------------------------------
def test_o_parceiro_ve_os_lancamentos_da_obra_dele(cenario):
    d = cenario
    ids = set(d["s"].scalars(
        aplicar_escopo(select(Titulo.id), d["s"], d["parceiro"])).all())
    assert d["t_creche"].id in ids
    assert d["t_escola"].id not in ids


def test_o_parceiro_ve_o_custo_da_obra_dele_no_relatorio(cenario):
    """Decisão do dono: ele vê TODO o custo da obra — inclusive por quanto a
    BWS compra. Escolha consciente, com o preço dito na hora."""
    d = cenario
    r = resumo(d["s"], "obra", {}, d["parceiro"])
    assert round(r["total"], 2) == 1000.00
    assert all("ESCOLA" not in l["chave"] for l in r["linhas"])


def test_parceiro_sem_obra_nao_ve_lancamento_nenhum(cenario):
    """O padrão NEGAR, dito em voz alta: lista vazia de obras não pode virar
    'vê tudo', e nem 'vê o que ele mesmo lançou' — ele não lança nada."""
    d = cenario
    ids = set(d["s"].scalars(
        aplicar_escopo(select(Titulo.id), d["s"], d["sem_obra"])).all())
    assert ids == set()


def test_as_duas_escritas_do_recorte_concordam_para_o_parceiro(cenario):
    """O recorte tem duas formas (consulta e pedaço de WHERE). Perfil novo é
    justamente quando elas divergem sem ninguém notar."""
    from sqlalchemy import text

    d = cenario
    for quem in (d["parceiro"], d["sem_obra"]):
        pelo_orm = set(d["s"].scalars(
            aplicar_escopo(select(Titulo.id), d["s"], quem)).all())
        onde, params = condicao_escopo_sql(d["s"], quem)
        pelo_sql = {r[0] for r in d["s"].execute(
            text(f"SELECT t.id FROM titulos t WHERE {onde}"), params)}
        assert pelo_orm == pelo_sql


# ---------------------------------------------------------------------------
# Arquivo: só a papelada da obra dele
# ---------------------------------------------------------------------------
def _documentos(s, usuario):
    return {l["nome"] for l in
            svc_arq.listar(s, usuario=usuario)["documentos"]}


def test_o_parceiro_ve_o_documento_da_obra_dele(cenario):
    d = cenario
    nomes = _documentos(d["s"], d["parceiro"])
    assert "Contrato Creche" in nomes
    assert "Contrato Escola" not in nomes


def test_o_parceiro_nao_ve_a_papelada_da_empresa(cenario):
    """Documento que não é de obra nenhuma é da BWS — certidão, contrato
    social, seguro. Não diz respeito a quem é de fora."""
    d = cenario
    assert "Certidão FGTS" not in _documentos(d["s"], d["parceiro"])


def test_o_parceiro_nao_ve_documento_de_faixa_pessoal(cenario):
    """Mesmo sendo da obra dele: folha e acordo de jornada são da BWS com o
    empregado dela."""
    d = cenario
    assert "Acordo de jornada" not in _documentos(d["s"], d["parceiro"])


def test_o_parceiro_nao_apaga_documento(cenario):
    """Só olha — e a trava não é só a alçada: é o escopo do objeto também."""
    assert not pode(cenario["parceiro"], "arquivar")


# ---------------------------------------------------------------------------
# Pessoal: a equipe da obra dele
# ---------------------------------------------------------------------------
def test_o_parceiro_ve_a_equipe_da_obra_dele_e_so_ela(cenario):
    """Decisão do dono: aqui não muda nada — o parceiro segue a mesma regra
    dos outros perfis presos a obra."""
    d = cenario
    nomes = {c["nome"] for c in listar_colaboradores(d["s"], usuario=d["parceiro"])}
    assert nomes == {"José da Creche"}
