"""Perguntar o que está ESCRITO nos documentos da empresa.

Pedido do dono em 11/09/2026: *"atrelar depois documentação da empresa para
orientar o assistente/agente… temos que pensar grande"*.

É a única família de respostas do assistente que não faz conta. Ela devolve
pedaços de texto que já estavam num contrato ou numa norma — e o que o sistema
garante não é o número, é a PROCEDÊNCIA: de qual documento saiu, e em que
trecho.

AS DUAS COISAS QUE ESTE ARQUIVO GUARDA, e as duas são decisão do dono:

1. **"Quem vê o quê tem que estar associado às suas permissões."** A busca
   passa pelo MESMO recorte da tela do Arquivo — faixa de sigilo e obra
   designada. Sem isso o assistente vira a porta dos fundos do controle de
   acesso que já existe.
2. **A resposta nunca vem sem o trecho.** Sem a citação é a IA falando, e só
   se pode confiar no que dá para conferir na fonte.

COM BANCO DE VERDADE porque a busca inteira vive no `WHERE` — e o dublê da
suíte ignora `WHERE`. Aqui isso não é detalhe: o que o dublê esconderia é
justamente o vazamento.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.perguntas import catalogo, documentos as svc_doc
from app.apps.erp.db.models.cadastros import (
    Colaborador, EscopoVisao, Obra, PerfilUsuario as P, Usuario, UsuarioObra,
)
from app.apps.erp.db.models.financeiro import Anexo, Documento, DocumentoTipo

pytestmark = pytest.mark.banco

CONTRATO_CRECHE = """CLAUSULA DECIMA - DO REAJUSTE
Os precos serao reajustados anualmente pelo INCC, contado da data-base do
orcamento, mediante requerimento da contratada.
CLAUSULA DECIMA PRIMEIRA - DA GARANTIA
O prazo de garantia da obra e de 5 (cinco) anos, contado do recebimento
definitivo."""

CONTRATO_ESCOLA = """CLAUSULA OITAVA - DA MULTA
A multa por atraso injustificado e de 0,5% por dia sobre o valor da parcela.
CLAUSULA NONA - DO REAJUSTE
Nao havera reajuste neste contrato, cujo prazo e inferior a doze meses."""

FOLHA_PESSOAL = """Acordo de compensacao de jornada. O reajuste salarial da
categoria foi de 4,5% conforme convencao coletiva."""


@pytest.fixture
def acervo(sessao_real):
    s = sessao_real
    creche = Obra(codigo="CRECHE", nome="Creche do Eusébio")
    escola = Obra(codigo="ESCOLA", nome="Escola do Planalto")
    s.add_all([creche, escola])
    s.flush()

    aberto = DocumentoTipo(codigo="CONTRATO-OBRA", nome="Contrato da obra",
                           grupo="OBRA", dono="OBRA", sigilo="ABERTO")
    pessoal = DocumentoTipo(codigo="FOLHA", nome="Documento de pessoal",
                            grupo="PESSOAL", dono="PESSOA",
                            sigilo="PESSOAL")
    s.add_all([aberto, pessoal])
    s.flush()

    pedreiro = Colaborador(nome="José da Silva", cpf="12345678901")
    s.add(pedreiro)
    s.flush()

    def arquivar(tipo, nome, texto, obra=None, colaborador=None):
        anexo = Anexo(entidade_tipo="documento", entidade_id=0,
                      nome_arquivo=nome, mime_type="application/pdf",
                      conteudo=b"x", hash_sha256=f"hash-{nome}",
                      guardado_em="BANCO")
        s.add(anexo)
        s.flush()
        d = Documento(tipo_codigo=tipo.codigo, anexo_id=anexo.id,
                      nome_padronizado=nome, texto=texto,
                      obra_id=obra.id if obra else None,
                      colaborador_id=colaborador.id if colaborador else None)
        s.add(d)
        s.flush()
        return d

    arquivar(aberto, "Contrato Creche", CONTRATO_CRECHE, creche)
    arquivar(aberto, "Contrato Escola", CONTRATO_ESCOLA, escola)
    arquivar(pessoal, "Acordo de jornada", FOLHA_PESSOAL,
             colaborador=pedreiro)

    def pessoa(nome, email, perfil, escopo=None):
        u = Usuario(nome=nome, email=email,
                    senha_hash=gerar_hash("senha-de-teste"), perfil=perfil,
                    escopo_visao=escopo or EscopoVisao.PROPRIOS)
        s.add(u)
        s.flush()
        return u

    chefe = pessoa("Marcelo", "chefe@bws.test", P.ADMIN)
    da_creche = pessoa("Adm da Creche", "adm@bws.test", P.ADMINISTRATIVO_OBRA,
                       EscopoVisao.OBRAS_DESIGNADAS)
    s.add(UsuarioObra(usuario_id=da_creche.id, obra_id=creche.id))
    comprador = pessoa("Comprador", "compras@bws.test", P.LANCADOR)
    s.flush()
    return {"sessao": s, "chefe": chefe, "da_creche": da_creche,
            "comprador": comprador, "creche": creche}


def _procurar(acervo, quem, assunto, **extra):
    return svc_doc.procurar(acervo["sessao"], acervo[quem],
                            pergunta=assunto, **extra)


def _nomes(achados):
    return {a["nome"] for a in achados}


# ---------------------------------------------------------------------------
# Acha o que está escrito
# ---------------------------------------------------------------------------
def test_acha_a_clausula_pela_palavra(acervo):
    assert _nomes(_procurar(acervo, "chefe", "reajuste")) >= {
        "Contrato Creche", "Contrato Escola"}


def test_o_dicionario_de_portugues_entende_a_familia_da_palavra(acervo):
    """"reajustados" no contrato tem de ser achado por "reajuste". É o que o
    dicionário de português do Postgres faz, e o motivo de usá-lo em vez de
    procurar o pedaço de texto cru."""
    assert "Contrato Creche" in _nomes(_procurar(acervo, "chefe", "reajustar"))


def test_o_trecho_vem_junto_e_marca_a_palavra(acervo):
    achado = [a for a in _procurar(acervo, "chefe", "garantia")
              if a["nome"] == "Contrato Creche"][0]
    assert "«" in achado["trecho"] and "»" in achado["trecho"]
    assert "cinco" in achado["trecho"].lower()


def test_a_citacao_diz_de_quem_e_o_documento(acervo):
    """"o contrato" não quer dizer nada: quem lê precisa saber de QUAL obra."""
    achado = [a for a in _procurar(acervo, "chefe", "garantia")
              if a["nome"] == "Contrato Creche"][0]
    assert achado["de_quem"] == "obra CRECHE"


def test_pergunta_escrita_como_gente_nao_derruba_a_busca(acervo):
    """Pontuação solta quebra as formas menos tolerantes de montar a busca."""
    for frase in ["o que diz sobre reajuste?", "multa por atraso!",
                  "garantia -- prazo", "reajuste e/ou correção"]:
        svc_doc.procurar(acervo["sessao"], acervo["chefe"], pergunta=frase)


def test_assunto_vazio_avisa_em_vez_de_varrer_tudo(acervo):
    with pytest.raises(svc_doc.SemBusca):
        _procurar(acervo, "chefe", "   ")


def test_da_para_recortar_por_obra(acervo):
    assert _nomes(_procurar(acervo, "chefe", "reajuste", obra="creche")) == {
        "Contrato Creche"}


# ---------------------------------------------------------------------------
# ESCOPO — a decisão do dono: "quem vê o quê tem que estar associado às
# suas permissões"
# ---------------------------------------------------------------------------
def test_quem_e_preso_a_uma_obra_nao_le_o_contrato_da_outra(acervo):
    achados = _nomes(_procurar(acervo, "da_creche", "reajuste"))
    assert "Contrato Creche" in achados
    assert "Contrato Escola" not in achados, (
        "o assistente virou a porta dos fundos do escopo por obra")


def test_documento_de_pessoal_nao_aparece_para_quem_nao_alcanca(acervo):
    """O acordo de jornada fala de "reajuste" salarial e é de faixa PESSOAL.
    Quem não vê essa faixa no Arquivo também não pode vê-la por aqui."""
    assert "Acordo de jornada" not in _nomes(_procurar(acervo, "comprador",
                                                       "reajuste"))
    assert "Acordo de jornada" not in _nomes(_procurar(acervo, "da_creche",
                                                       "reajuste"))


def test_o_chefe_alcanca_a_faixa_pessoal(acervo):
    assert "Acordo de jornada" in _nomes(_procurar(acervo, "chefe", "reajuste"))


def test_o_recorte_e_o_MESMO_da_tela_do_arquivo():
    """Duas regras iguais escritas em dois lugares divergem — e aqui divergir
    quer dizer alguém ler documento que não devia."""
    import inspect
    fonte = inspect.getsource(svc_doc.procurar)
    assert "aplicar_escopo" in fonte
    from app.apps.erp.core.arquivo import service
    assert "aplicar_escopo(stmt, s, usuario)" in inspect.getsource(service.listar)


# ---------------------------------------------------------------------------
# A resposta, do jeito que chega na tela
# ---------------------------------------------------------------------------
def _responder(acervo, quem, **parametros):
    return catalogo.responder("o_que_os_documentos_dizem", acervo["sessao"],
                              acervo[quem], parametros)


def test_a_resposta_traz_o_trecho_como_coluna(acervo):
    r = _responder(acervo, "chefe", assunto="garantia")
    assert [c["chave"] for c in r["colunas"]] == [
        "documento", "tipo", "de_quem", "trecho"]
    assert any("cinco" in l["trecho"].lower() for l in r["linhas"])


def test_sem_assunto_ele_pede_o_assunto_em_vez_de_despejar_o_acervo(acervo):
    r = _responder(acervo, "chefe")
    assert "Diga o que você quer procurar" in r["frase"]
    assert r["linhas"] == []


def test_nao_achar_explica_as_duas_causas_possiveis(acervo):
    """"Não achei" sem explicação faz a pessoa achar que o sistema é ruim. As
    causas pedem coisas diferentes: arquivar o documento, ou procurar com
    outra palavra."""
    r = _responder(acervo, "chefe", assunto="helicoptero")
    assert "Não achei" in r["frase"]
    assert "não está no Arquivo" in r["observacao"]
    assert "outras palavras" in r["observacao"]


def test_a_resposta_diz_que_a_busca_e_por_palavra_e_nao_por_sentido(acervo):
    """O limite tem de estar escrito onde a pessoa lê, senão ela conclui que
    o documento não existe quando ele só usa outro termo."""
    r = _responder(acervo, "chefe", assunto="helicoptero")
    assert "correção monetária" in r["observacao"]


def test_sem_chave_de_ia_a_resposta_continua_valendo(acervo, monkeypatch):
    """Os trechos são a resposta; a frase da IA é acréscimo. Sem chave, a
    pergunta continua respondida."""
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    r = _responder(acervo, "chefe", assunto="garantia")
    assert r["linhas"]
    assert "Achei em" in r["frase"]


def test_a_ia_so_recebe_os_trechos_achados(monkeypatch):
    """Ela não consulta o banco e não lembra de nada por fora: o material dela
    é o que a busca devolveu, e só."""
    import inspect
    fonte = inspect.getsource(svc_doc.resumir)
    assert "t['trecho']" in fonte
    assert "SOMENTE com o que estiver nos trechos" in svc_doc._INSTRUCAO
    assert "não respondem isso" in svc_doc._INSTRUCAO


def test_a_falha_da_ia_nao_derruba_a_pergunta(acervo, monkeypatch):
    def explode(*a, **k):
        raise RuntimeError("serviço fora do ar")
    monkeypatch.setattr(svc_doc, "resumir", explode)
    with pytest.raises(RuntimeError):
        svc_doc.resumir([], "x")          # confirma que o dublê está de pé
    monkeypatch.undo()
    monkeypatch.setenv("OPENAI_API_KEY", "chave-que-nao-funciona")
    r = _responder(acervo, "chefe", assunto="garantia")
    assert r["linhas"], "os trechos somem quando a IA falha"


# ---------------------------------------------------------------------------
# A FRASE VIRA O ASSUNTO — e a moldura da pergunta fica de fora
#
# Ninguém escreve "procure o ASSUNTO reajuste". Escreve "o que o contrato diz
# sobre reajuste", ou só "prazo de garantia". Então este parâmetro declara que
# pode valer a frase inteira — e "contrato", "documento", "diz" ficam de fora,
# porque procurar por "contrato" faria todos os contratos empatarem e o
# "reajuste", que é o que importa, se perderia no meio.
# ---------------------------------------------------------------------------
def _assunto(frase):
    from app.apps.erp.core.perguntas.entender import entender
    return entender(frase, catalogo.para_a_tela()).get("parametros", {}).get(
        "assunto")


@pytest.mark.parametrize("frase, esperado", [
    ("o que o contrato diz sobre reajuste", "reajuste"),
    ("qual o prazo de garantia", "prazo garantia"),
    ("procure multa por atraso nos documentos", "multa atraso"),
    ("procure helicoptero nos documentos", "helicoptero"),
])
def test_a_frase_vira_o_assunto_sem_a_moldura(frase, esperado):
    assert _assunto(frase) == esperado


def test_so_este_parametro_pode_valer_a_frase_toda():
    """Aplicar isso a qualquer parâmetro faria TODA pergunta virar busca por
    si mesma — "o que tem a pagar hoje" viraria obra = "pagar hoje"."""
    from app.apps.erp.core.perguntas.entender import entender
    r = entender("o que tem a pagar hoje", catalogo.para_a_tela())
    assert r["chave"] == "a_pagar_no_periodo"
    assert r["parametros"] == {}
    marcados = [p["nome"] for q in catalogo.CATALOGO
                for p in q["parametros"] if p.get("a_frase_toda")]
    assert marcados == ["assunto"], (
        f"outro parâmetro passou a valer a frase toda: {marcados}")


def test_a_pergunta_dos_documentos_e_alcancada_escrevendo():
    from app.apps.erp.core.perguntas.entender import entender
    for frase in ("o que o contrato diz sobre reajuste",
                  "qual o prazo de garantia",
                  "procure multa por atraso nos documentos"):
        assert entender(frase, catalogo.para_a_tela())["chave"] == \
            "o_que_os_documentos_dizem", frase
