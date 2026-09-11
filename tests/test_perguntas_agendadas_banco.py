"""A pergunta que chega sozinha.

Pedido do dono, com as palavras dele: *"toda segunda-feira me manda
determinado tipo de informação. Aí a própria [IA] agendar essa necessidade
minha e fazer aquela ação executar e me mandar. Isso é muito poderoso."*

AS SEIS REGRAS QUE ESTE ARQUIVO GUARDA — cada uma existe porque o contrário
estraga o relatório:

1. Guarda a CONSULTA, não a frase.
2. Roda com a permissão de QUEM RECEBE.
3. Compara com a rodada anterior.
4. Só manda quando há o que mandar, se assim for pedido.
5. Relatório quebrado RECLAMA — zero calado parece resposta.
6. Só lê. Não lança, não aprova, não paga.

COM BANCO DE VERDADE porque a regra 2 é a mais importante e vive inteira no
`WHERE` — o dublê da suíte ignora `WHERE`, e é justamente o vazamento que ele
esconderia.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao
from app.apps.erp.core.perguntas import agendadas as svc
from app.apps.erp.db.models.cadastros import (
    EscopoVisao, Obra, PerfilUsuario as P, Usuario, UsuarioObra,
)
from app.apps.erp.db.models.financeiro import PerguntaAgendada

pytestmark = pytest.mark.banco

SEGUNDA = date(2026, 9, 14)          # uma segunda-feira de verdade
TERCA = date(2026, 9, 15)


@pytest.fixture
def base(sessao_real):
    s = sessao_real
    a = Obra(codigo="OBRA-A", nome="Creche")
    b = Obra(codigo="OBRA-B", nome="Escola")
    s.add_all([a, b])
    s.flush()

    def pessoa(nome, email, perfil, escopo=None, telefone="85999990000"):
        u = Usuario(nome=nome, email=email,
                    senha_hash=gerar_hash("senha-de-teste"), perfil=perfil,
                    escopo_visao=escopo or EscopoVisao.PROPRIOS,
                    telefone=telefone)
        s.add(u)
        s.flush()
        return u

    chefe = pessoa("Marcelo", "chefe@bws.test", P.ADMIN)
    da_obra = pessoa("Gestor da A", "gestor@bws.test", P.ADMINISTRATIVO_OBRA,
                     EscopoVisao.OBRAS_DESIGNADAS)
    s.add(UsuarioObra(usuario_id=da_obra.id, obra_id=a.id))
    sem_telefone = pessoa("Sem contato", "mudo@bws.test", P.LANCADOR,
                          telefone=None)
    s.flush()
    return {"sessao": s, "chefe": chefe, "da_obra": da_obra,
            "sem_telefone": sem_telefone, "obra_a": a, "obra_b": b}


def _agendar(base, quem="chefe", **extra):
    parametros = extra.pop("parametros", {})
    return svc.agendar(base["sessao"], base[quem],
                       chave=extra.pop("chave", "panorama_de_vencimentos"),
                       titulo=extra.pop("titulo", "Caixa da semana"),
                       parametros=parametros, **extra)


# ---------------------------------------------------------------------------
# 1. Guarda a CONSULTA, não a frase
# ---------------------------------------------------------------------------
def test_guarda_a_chave_e_os_filtros(base):
    a = _agendar(base, chave="a_pagar_no_periodo",
                 parametros={"obra": "OBRA-A"})
    assert a.chave == "a_pagar_no_periodo"
    assert a.parametros == {"obra": "OBRA-A"}


def test_pergunta_que_nao_existe_e_recusada(base):
    """Chave inventada viraria um relatório que quebra toda segunda."""
    with pytest.raises(ErroValidacao):
        _agendar(base, chave="pergunta_inventada")


def test_o_texto_da_mensagem_nao_e_reinterpretado(base):
    """O que roda é a consulta guardada. Se o sistema relesse uma frase toda
    semana, o critério mudaria sozinho e comparar deixaria de fazer sentido."""
    import inspect
    fonte = inspect.getsource(svc.rodar_do_dia)
    assert "catalogo.responder(a.chave" in fonte
    assert "entender" not in fonte


# ---------------------------------------------------------------------------
# 2. Roda com a permissão de QUEM RECEBE — a regra mais importante
# ---------------------------------------------------------------------------
def test_agendar_para_outra_pessoa_exige_cuidar_de_acessos(base):
    """Sem isso, qualquer um agendaria um relatório no nome do diretor — que
    rodaria com a permissão DELE, e chegaria a quem agendou."""
    with pytest.raises(ErroValidacao) as e:
        _agendar(base, quem="da_obra",
                 para_usuario_id=base["chefe"].id)
    assert "quem cuida dos acessos" in str(e.value)


def test_quem_cuida_de_acessos_pode_agendar_para_outro(base):
    a = _agendar(base, quem="chefe", para_usuario_id=base["da_obra"].id)
    assert a.usuario_id == base["da_obra"].id
    assert a.criado_por == base["chefe"].id


def test_a_resposta_e_calculada_com_a_permissao_do_destinatario(base, monkeypatch):
    """O teste que importa: o relatório do gestor da obra A tem de ser
    calculado COMO ELE, não como quem criou."""
    vistos = []
    from app.apps.erp.core.perguntas import catalogo

    def espiar(chave, s, usuario, parametros):
        vistos.append(usuario.id)
        return {"frase": "ok", "linhas": [{"x": 1}], "quantas": 1,
                "titulo": "t", "colunas": [], "de_onde_veio": {}}

    monkeypatch.setattr(catalogo, "responder", espiar)
    monkeypatch.setattr(svc, "_mandar", lambda *a, **k: True)
    _agendar(base, quem="chefe", para_usuario_id=base["da_obra"].id,
             frequencia="DIARIA")
    svc.rodar_do_dia(base["sessao"], hoje=SEGUNDA)
    assert vistos == [base["da_obra"].id], (
        "o relatório rodou com a permissão de quem CRIOU, não de quem recebe")


def test_desligar_relatorio_de_outra_pessoa_responde_nao_encontrado(base):
    """Dizer "sem permissão" para um número que existe confirma que ele
    existe — e varrer os números mapearia o sistema."""
    a = _agendar(base, quem="chefe")
    with pytest.raises(ErroNaoEncontrado):
        svc.desligar(base["sessao"], base["da_obra"], a.id)


# ---------------------------------------------------------------------------
# 3. Compara com a rodada anterior
# ---------------------------------------------------------------------------
def test_a_mensagem_diz_quanto_era_na_vez_passada(base):
    a = _agendar(base)
    a.ultimo_total = Decimal("280000")
    texto = svc._texto(a, {"frase": "R$ 340.000,00 a pagar.",
                           "total": 340000.0, "titulo": "t"})
    assert "era R$ 280.000,00" in texto
    assert "↑" in texto


def test_quando_nao_mudou_ele_diz_que_nao_mudou(base):
    a = _agendar(base)
    a.ultimo_total = Decimal("100")
    assert "igual à última vez" in svc._texto(
        a, {"frase": "x", "total": 100.0, "titulo": "t"})


def test_na_primeira_vez_nao_inventa_comparacao(base):
    a = _agendar(base)
    texto = svc._texto(a, {"frase": "x", "total": 100.0, "titulo": "t"})
    assert "era" not in texto


def test_o_total_da_rodada_fica_guardado_para_a_proxima(base, monkeypatch):
    from app.apps.erp.core.perguntas import catalogo
    monkeypatch.setattr(catalogo, "responder", lambda *a, **k: {
        "frase": "x", "linhas": [{"a": 1}], "quantas": 1, "total": 4242.0,
        "titulo": "t", "colunas": [], "de_onde_veio": {}})
    monkeypatch.setattr(svc, "_mandar", lambda *a, **k: True)
    a = _agendar(base, frequencia="DIARIA")
    svc.rodar_do_dia(base["sessao"], hoje=SEGUNDA)
    assert a.ultimo_total == Decimal("4242.00")


# ---------------------------------------------------------------------------
# 4. Só manda quando há o que mandar
# ---------------------------------------------------------------------------
def test_marcado_so_se_houver_nao_manda_vazio(base, monkeypatch):
    """Relatório que chega igual todo mês vira spam e para de ser lido — e aí
    o dia em que ele traz algo importante também não é lido."""
    from app.apps.erp.core.perguntas import catalogo
    monkeypatch.setattr(catalogo, "responder", lambda *a, **k: {
        "frase": "nada", "linhas": [], "quantas": 0, "titulo": "t",
        "colunas": [], "de_onde_veio": {}})
    enviados = []
    monkeypatch.setattr(svc, "_mandar", lambda *a, **k: enviados.append(1) or True)
    _agendar(base, frequencia="DIARIA", so_se_houver=True)
    r = svc.rodar_do_dia(base["sessao"], hoje=SEGUNDA)
    assert enviados == []
    assert r["pulados"][0]["motivo"] == "nada a relatar hoje"


def test_sem_a_marca_ele_manda_mesmo_vazio(base, monkeypatch):
    from app.apps.erp.core.perguntas import catalogo
    monkeypatch.setattr(catalogo, "responder", lambda *a, **k: {
        "frase": "nada hoje", "linhas": [], "quantas": 0, "titulo": "t",
        "colunas": [], "de_onde_veio": {}})
    monkeypatch.setattr(svc, "_mandar", lambda *a, **k: True)
    _agendar(base, frequencia="DIARIA", so_se_houver=False)
    assert svc.rodar_do_dia(base["sessao"], hoje=SEGUNDA)["enviados"]


# ---------------------------------------------------------------------------
# 5. Relatório quebrado RECLAMA
# ---------------------------------------------------------------------------
def test_quando_quebra_ele_avisa_em_vez_de_mandar_zero(base, monkeypatch):
    """Zero calado é pior que erro: parece resposta."""
    from app.apps.erp.core.perguntas import catalogo

    def explode(*a, **k):
        raise RuntimeError("a obra foi encerrada")
    monkeypatch.setattr(catalogo, "responder", explode)
    recados = []
    monkeypatch.setattr(svc, "_mandar",
                        lambda s, a, dono, texto: recados.append(texto) or True)
    a = _agendar(base, frequencia="DIARIA")
    r = svc.rodar_do_dia(base["sessao"], hoje=SEGUNDA)
    assert r["falhas"]
    assert recados and "Não consegui montar" in recados[0]
    assert "encerrada" in recados[0]
    assert a.ultimo_erro


def test_quem_nao_tem_telefone_nem_email_fica_registrado(base, monkeypatch):
    """Não dá para mandar — mas isso tem de aparecer, não sumir."""
    from app.apps.erp.core.perguntas import catalogo
    monkeypatch.setattr(catalogo, "responder", lambda *a, **k: {
        "frase": "x", "linhas": [{"a": 1}], "quantas": 1, "titulo": "t",
        "colunas": [], "de_onde_veio": {}})
    a = svc.agendar(base["sessao"], base["sem_telefone"],
                    chave="panorama_de_vencimentos", titulo="t",
                    frequencia="DIARIA")
    r = svc.rodar_do_dia(base["sessao"], hoje=SEGUNDA)
    assert any("sem telefone" in (p.get("motivo") or "") for p in r["pulados"])
    assert "não há por onde mandar" in (a.ultimo_erro or "")


# ---------------------------------------------------------------------------
# 6. Só lê
# ---------------------------------------------------------------------------
def test_o_agendado_nao_escreve_nada_de_negocio():
    """Agendado que AGE sem ninguém olhando é o que faz empresa desligar
    assistente. Para agir, o caminho continua sendo preparar e esperar."""
    import inspect
    fonte = inspect.getsource(svc)
    for proibido in ("lancar(", "aprovar(", "pagar(", "baixar(", "emitir("):
        assert proibido not in fonte, f"o agendado chama {proibido}"


# ---------------------------------------------------------------------------
# O relógio
# ---------------------------------------------------------------------------
def test_semanal_so_no_dia_combinado(base):
    a = _agendar(base, frequencia="SEMANAL", dia=0)
    assert svc.vence_hoje(a, SEGUNDA) is True
    assert svc.vence_hoje(a, TERCA) is False


def test_mensal_no_dia_do_mes(base):
    a = _agendar(base, frequencia="MENSAL", dia=5)
    assert svc.vence_hoje(a, date(2026, 9, 5)) is True
    assert svc.vence_hoje(a, date(2026, 9, 6)) is False


def test_dia_29_vira_28_para_caber_em_fevereiro(base):
    """Combinar "todo dia 31" faria o relatório pular quatro meses por ano."""
    assert _agendar(base, frequencia="MENSAL", dia=31).dia == 28


def test_rodar_duas_vezes_no_mesmo_dia_nao_manda_duas_vezes(base, monkeypatch):
    """A rotina diária pode ser chamada de novo — e às vezes é."""
    from app.apps.erp.core.perguntas import catalogo
    monkeypatch.setattr(catalogo, "responder", lambda *a, **k: {
        "frase": "x", "linhas": [{"a": 1}], "quantas": 1, "titulo": "t",
        "colunas": [], "de_onde_veio": {}})
    idas = []
    monkeypatch.setattr(svc, "_mandar", lambda *a, **k: idas.append(1) or True)
    _agendar(base, frequencia="DIARIA")
    svc.rodar_do_dia(base["sessao"], hoje=SEGUNDA)
    svc.rodar_do_dia(base["sessao"], hoje=SEGUNDA)
    assert len(idas) == 1


def test_desligado_nao_roda(base, monkeypatch):
    from app.apps.erp.core.perguntas import catalogo
    monkeypatch.setattr(catalogo, "responder", lambda *a, **k: {
        "frase": "x", "linhas": [], "quantas": 0, "titulo": "t",
        "colunas": [], "de_onde_veio": {}})
    idas = []
    monkeypatch.setattr(svc, "_mandar", lambda *a, **k: idas.append(1) or True)
    a = _agendar(base, frequencia="DIARIA")
    svc.desligar(base["sessao"], base["chefe"], a.id)
    svc.rodar_do_dia(base["sessao"], hoje=SEGUNDA)
    assert idas == []


def test_simular_nao_manda_nem_grava(base, monkeypatch):
    """É como se confere o que vai sair antes de deixar solto."""
    from app.apps.erp.core.perguntas import catalogo
    monkeypatch.setattr(catalogo, "responder", lambda *a, **k: {
        "frase": "x", "linhas": [{"a": 1}], "quantas": 1, "titulo": "t",
        "colunas": [], "de_onde_veio": {}})
    idas = []
    monkeypatch.setattr(svc, "_mandar", lambda *a, **k: idas.append(1) or True)
    a = _agendar(base, frequencia="DIARIA")
    r = svc.rodar_do_dia(base["sessao"], hoje=SEGUNDA, simular=True)
    assert idas == []
    assert a.ultima_rodada is None
    assert r["enviados"][0]["texto"]


def test_quem_saiu_do_erp_nao_recebe_mais(base, monkeypatch):
    from app.apps.erp.core.perguntas import catalogo
    monkeypatch.setattr(catalogo, "responder", lambda *a, **k: {
        "frase": "x", "linhas": [{"a": 1}], "quantas": 1, "titulo": "t",
        "colunas": [], "de_onde_veio": {}})
    idas = []
    monkeypatch.setattr(svc, "_mandar", lambda *a, **k: idas.append(1) or True)
    _agendar(base, frequencia="DIARIA")
    base["chefe"].ativo = False
    base["sessao"].flush()
    r = svc.rodar_do_dia(base["sessao"], hoje=SEGUNDA)
    assert idas == []
    assert "saiu do ERP" in r["pulados"][0]["motivo"]


# ---------------------------------------------------------------------------
# O teto, e o que a pessoa lê
# ---------------------------------------------------------------------------
def test_tem_teto_por_pessoa(base):
    """Quinze relatórios chegando toda segunda não são lidos, e aí nenhum é."""
    for i in range(svc.TETO_POR_PESSOA):
        _agendar(base, parametros={"obra": f"O{i}"})
    with pytest.raises(ErroValidacao) as e:
        _agendar(base, parametros={"obra": "ultima"})
    assert "ninguém lê" in str(e.value)


def test_o_quando_e_escrito_como_gente_fala():
    assert svc.quando_por_extenso("SEMANAL", 0) == "toda segunda-feira"
    assert svc.quando_por_extenso("MENSAL", 5) == "todo dia 5"
    assert svc.quando_por_extenso("DIARIA", None) == "todo dia"


def test_email_ainda_nao_esta_ligado_e_diz_por_que(base):
    """O e-mail do ERP sai pela conta de uma empresa, e a BWS tem mais de uma.
    Escolher uma por conta própria mandaria pelo remetente errado."""
    with pytest.raises(ErroValidacao) as e:
        _agendar(base, canal="EMAIL")
    assert "conta de uma empresa" in str(e.value)


def test_a_listagem_mostra_so_os_da_pessoa(base):
    _agendar(base, quem="chefe")
    _agendar(base, quem="chefe", para_usuario_id=base["da_obra"].id)
    assert len(svc.listar(base["sessao"], base["da_obra"])) == 1
    assert len(svc.listar(base["sessao"], base["chefe"])) == 1


def test_combinar_o_mesmo_duas_vezes_responde_como_gente(base):
    """Dois cliques seguidos no botão chegam juntos, e quem clica de novo quer
    saber que já está combinado — não ler "duplicate key value violates unique
    constraint", que foi o que apareceu na tela na primeira versão."""
    _agendar(base, parametros={"obra": "OBRA-A"})
    with pytest.raises(ErroValidacao) as e:
        _agendar(base, parametros={"obra": "OBRA-A"})
    frase = str(e.value)
    assert "já está combinado" in frase
    assert "toda segunda-feira" in frase
    assert "duplicate" not in frase.lower()


def test_o_mesmo_relatorio_com_filtro_diferente_pode(base):
    """"a pagar da obra A" e "a pagar da obra B" são dois relatórios."""
    _agendar(base, parametros={"obra": "OBRA-A"})
    _agendar(base, parametros={"obra": "OBRA-B"})
    assert len(svc.listar(base["sessao"], base["chefe"])) == 2


def test_desligado_libera_combinar_de_novo(base):
    a = _agendar(base, parametros={"obra": "OBRA-A"})
    svc.desligar(base["sessao"], base["chefe"], a.id)
    _agendar(base, parametros={"obra": "OBRA-A"})     # não levanta
