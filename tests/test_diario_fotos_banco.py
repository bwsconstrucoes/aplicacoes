# -*- coding: utf-8 -*-
"""
AS FOTOS DO DIÁRIO DE OBRA — contra um Postgres de verdade.

Ele, em 14/11/2026: *"era muito interessante a gente poder colocar serviços
executados e fotos, e integrar isso no diário impresso também (…) é como se
fosse um anexo ao diário, essa página das fotos, para não interferir no layout
que a gente já trabalhou. (…) Lembrando que fotos sempre no Google Drive."* E,
perguntado sobre o limite: *"sim, podemos limitar a qtd a 6."*

⚑ POR QUE COM BANCO: a foto é um ANEXO (`entidade_tipo = "diario_foto"`), e
tudo que importa aqui — o teto por dia, o escopo por obra, a trava do diário
fechado — mora em consulta. O dublê de sessão não alcança nada disso.
"""
from __future__ import annotations

import io as _io
from datetime import date, timedelta

import pytest
from sqlalchemy import text

from app.apps.erp.core import diario as svc
from app.apps.erp.core import diario_fotos as fotos
from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao

pytestmark = pytest.mark.banco


# ⚑ A DATA É LIDA NO MOMENTO DO USO, nunca no topo do arquivo: calculada na
# importação, ela discorda do ERP se a rodada virar a meia-noite. Há um teste
# da suíte vigiando isso (`test_suite_nao_depende_do_relogio.py`), e foi ele
# que me pegou aqui.
def _ontem():
    return date.today() - timedelta(days=1)


def _jpeg(cor=(120, 140, 160), tamanho=(60, 45)) -> bytes:
    from PIL import Image
    buf = _io.BytesIO()
    Image.new("RGB", tamanho, cor).save(buf, format="JPEG")
    return buf.getvalue()


@pytest.fixture
def dono(sessao_real, obra):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    u = Usuario(nome="Administrativo da obra", email="adm@fotos.teste",
                ativo=True, senha_hash=gerar_hash("senha-de-teste-123"),
                perfil=PerfilUsuario.ADMINISTRATIVO_OBRA,
                telefone="5585900000021")
    sessao_real.add(u)
    sessao_real.flush()
    # ⚑ 24/11/2026: quem é da obra abre só as obras dele (`PERFIS_SO_PORTAL`).
    # Antes, sem obra nenhuma, o administrativo abria o diário de qualquer
    # obra — o vazamento corrigido. O dono do diário é o da obra do teste.
    from app.apps.erp.db.models.cadastros import UsuarioObra
    sessao_real.add(UsuarioObra(usuario_id=u.id, obra_id=obra))
    sessao_real.flush()
    return u


@pytest.fixture
def obra(sessao_real):
    oid = sessao_real.execute(text("""
        INSERT INTO obras (codigo, nome, status, data_inicio, data_termino)
        VALUES ('FOTO-01', 'Obra das fotos', 'ATIVA', :ini, :fim)
        RETURNING id"""),
        {"ini": date.today() - timedelta(days=200),
         "fim": date.today() + timedelta(days=200)}).scalar()
    sessao_real.flush()
    return oid


@pytest.fixture
def dia(sessao_real, obra, dono):
    quando = _ontem()
    svc.salvar(sessao_real, obra, quando,
               {"relato": "Concretagem da laje do bloco B."}, dono)
    sessao_real.flush()
    return quando


# ---------------------------------------------------------------------------
def test_a_foto_entra_no_dia_com_legenda(sessao_real, obra, dia, dono):
    r = fotos.anexar(sessao_real, obra, dia, _jpeg(), "laje.jpg",
                     legenda="Laje do bloco B concluída", usuario=dono)
    sessao_real.flush()
    assert r["quantas"] == 1 and r["teto"] == 6
    lista = fotos.listar(sessao_real, fotos._diario(sessao_real, obra, dia)["id"])
    assert len(lista) == 1
    assert lista[0]["legenda"] == "Laje do bloco B concluída"


def test_o_teto_e_seis_por_dia(sessao_real, obra, dia, dono):
    """⚑ Decidido por ele. Não é capricho de tela: o relatório de três meses
    com seis fotos por dia já passa de 500 imagens num arquivo que alguém vai
    mandar por e-mail."""
    for n in range(fotos.TETO_POR_DIA):
        fotos.anexar(sessao_real, obra, dia, _jpeg(cor=(n * 20, 100, 100)),
                     f"foto{n}.jpg", usuario=dono)
    sessao_real.flush()
    with pytest.raises(ErroValidacao) as e:
        fotos.anexar(sessao_real, obra, dia, _jpeg(cor=(9, 9, 9)),
                     "setima.jpg", usuario=dono)
    assert "limite" in str(e.value)


def test_so_entra_imagem(sessao_real, obra, dia, dono):
    """Documento da obra tem lugar próprio, no Arquivo. Aqui é registro
    visual do serviço executado."""
    with pytest.raises(ErroValidacao) as e:
        fotos.anexar(sessao_real, obra, dia, b"%PDF-1.4 nao e foto",
                     "contrato.pdf", usuario=dono)
    assert "só foto" in str(e.value)


# ---------------------------------------------------------------------------
# ⚑ A REGRA VIROU AO CONTRÁRIO EM 26/11/2026, e quem a virou foi o dono.
#
# Antes, anexar foto num dia sem diário era RECUSADO ("Grave o diário deste dia
# antes de anexar foto"). Havia um teste aqui garantindo a recusa. Ele olhou e
# desmontou:
#
# > *"Eu não consigo adicionar uma foto (…) por que eu não adiciono a foto e
# > automaticamente aquela foto já grava? (…) Se a pessoa quiser colocar uma
# > foto na hora, no dia, e alimentar imediatamente, ela vai abrir o dia,
# > adicionar a foto. E depois vai fazer o relato, concluir o diário."*
#
# Quem está na obra fotografa primeiro e escreve depois. Então a foto passou a
# CRIAR o dia — e o teste antigo virou este par, que guarda as duas metades da
# nova regra: a foto abre o dia, E o dia aberto assim continua sendo cobrado.
# ---------------------------------------------------------------------------
def test_a_foto_abre_o_dia_que_ainda_nao_existe(sessao_real, obra, dono):
    quando = date.today() - timedelta(days=5)
    assert svc.abrir_dia(sessao_real, obra, quando)["existe"] is False

    r = fotos.anexar(sessao_real, obra, quando, _jpeg(), "primeira.jpg",
                     legenda="Chegada do material", usuario=dono)
    sessao_real.flush()
    assert r["quantas"] == 1
    visao = svc.abrir_dia(sessao_real, obra, quando)
    assert visao["existe"] is True
    assert len(visao["fotos"]) == 1
    # E o dia nasce SEM relato — ninguém escreveu nada ainda.
    assert not (visao["diario"].get("relato") or "").strip()


def test_o_dia_aberto_por_uma_foto_continua_sendo_cobrado(sessao_real, obra, dono):
    """A metade que protege o diário: foto adianta o trabalho, não o conclui.

    Sem isto, mandar uma foto faria o dia sumir da cobrança — e a obra
    apareceria em dia sem ninguém ter escrito o relato. É exatamente o buraco
    que a regra "dia com expediente precisa de relato" existe para tapar.
    """
    quando = date.today() - timedelta(days=5)
    fotos.anexar(sessao_real, obra, quando, _jpeg(), "primeira.jpg", usuario=dono)
    sessao_real.flush()

    assert svc.estado_do_dia(
        {"sem_expediente": False, "relato": None, "situacao": "ABERTO"}) == "comecado"

    quadro = svc.quadro_do_mes(sessao_real, quando.year, quando.month,
                               obra_ids=[obra])
    linha = [o for o in quadro["obras"] if o["obra_id"] == obra][0]
    estado = linha["celulas"][quando.day - 1]
    assert estado == "comecado", f"o dia saiu como {estado}"
    # O dia de hoje pode ou não contar (fim de semana, fora do prazo); o que
    # importa é que ESTE dia continua na conta de quem falta.
    assert linha["faltam"] >= 1


def test_escrever_o_relato_tira_o_dia_da_cobranca(sessao_real, obra, dono):
    quando = date.today() - timedelta(days=5)
    fotos.anexar(sessao_real, obra, quando, _jpeg(), "primeira.jpg", usuario=dono)
    svc.salvar(sessao_real, obra, quando, {"relato": "Fôrma da viga V3."}, dono)
    sessao_real.flush()

    quadro = svc.quadro_do_mes(sessao_real, quando.year, quando.month,
                               obra_ids=[obra])
    linha = [o for o in quadro["obras"] if o["obra_id"] == obra][0]
    assert linha["celulas"][quando.day - 1] == "aberto"


def test_legendar_e_tirar_foto_continuam_exigindo_o_dia(sessao_real, obra, dono):
    """A criação do dia é SÓ do anexar. Legendar ou tirar foto de um dia que
    não existe é pedido sem sentido — e criar o dia para atendê-lo deixaria
    diários vazios por aí."""
    quando = date.today() - timedelta(days=6)
    with pytest.raises(ErroNaoEncontrado):
        fotos.legendar(sessao_real, obra, quando, 1, "nova legenda", usuario=dono)
    with pytest.raises(ErroNaoEncontrado):
        fotos.remover(sessao_real, obra, quando, 1, usuario=dono)
    assert svc.abrir_dia(sessao_real, obra, quando)["existe"] is False


def test_diario_fechado_aceita_foto_nova_mas_nao_deixa_tirar(
        sessao_real, obra, dia, dono):
    """⚑ Acrescentar só soma prova; tirar, não: o papel já saiu com ela, e
    sumir com evidência de documento fechado é outra conversa."""
    r = fotos.anexar(sessao_real, obra, dia, _jpeg(), "antes.jpg", usuario=dono)
    sessao_real.flush()
    svc.fechar(sessao_real, obra, dia, dono)
    sessao_real.flush()

    # acrescentar: pode
    fotos.anexar(sessao_real, obra, dia, _jpeg(cor=(10, 200, 10)),
                 "depois.jpg", usuario=dono)
    sessao_real.flush()
    # tirar: não
    with pytest.raises(ErroValidacao) as e:
        fotos.remover(sessao_real, obra, dia, r["id"], dono)
    assert "fechado" in str(e.value)


def test_tirar_foto_de_outro_dia_nao_funciona(sessao_real, obra, dia, dono):
    """Os números dos anexos são sequenciais. Sem esta conferência, mandar um
    número qualquer apagaria a foto de outro dia — ou de outra obra."""
    outro = dia - timedelta(days=1)
    svc.salvar(sessao_real, obra, outro, {"relato": "Outro dia."}, dono)
    sessao_real.flush()
    alheia = fotos.anexar(sessao_real, obra, outro, _jpeg(), "alheia.jpg",
                          usuario=dono)
    sessao_real.flush()
    with pytest.raises(ErroNaoEncontrado):
        fotos.remover(sessao_real, obra, dia, alheia["id"], dono)


def test_a_tela_do_dia_traz_as_fotos_e_o_teto(sessao_real, obra, dia, dono):
    fotos.anexar(sessao_real, obra, dia, _jpeg(), "uma.jpg",
                 legenda="Pilar P3", usuario=dono)
    sessao_real.flush()
    d = svc.abrir_dia(sessao_real, obra, dia)
    assert d["fotos_teto"] == 6
    assert [f["legenda"] for f in d["fotos"]] == ["Pilar P3"]


def test_o_papel_leva_a_foto_embutida_e_encolhida(sessao_real, obra, dia, dono):
    """⚑ EMBUTIDA, e não por link: o papel é gerado pelo navegador de quem
    imprime, e um `<img src="/erp/anexo/123">` dependeria de a sessão valer na
    hora. ENCOLHIDA porque 90 dias × 6 fotos é um PDF que não passa em e-mail."""
    grande = _jpeg(tamanho=(2400, 1800))
    fotos.anexar(sessao_real, obra, dia, grande, "grande.jpg",
                 legenda="Vista geral", usuario=dono)
    sessao_real.flush()
    r = svc.relatorio(sessao_real, obra, dia, dia)
    pagina = r["paginas"][0]
    assert len(pagina["fotos"]) == 1
    assert pagina["fotos"][0]["legenda"] == "Vista geral"
    assert pagina["fotos"][0]["dados"].startswith("data:image/")
    # a imagem do papel é menor que a guardada
    import base64
    bruto = base64.b64decode(pagina["fotos"][0]["dados"].split(",", 1)[1])
    from PIL import Image
    assert max(Image.open(_io.BytesIO(bruto)).size) <= fotos.LADO_NO_PAPEL


def test_dia_sem_foto_nao_ganha_folha_de_anexo(sessao_real, obra, dia, dono):
    """A folha de anexo só existe quando há foto — senão o relatório de um mês
    sairia com 30 folhas em branco atrás."""
    r = svc.relatorio(sessao_real, obra, dia, dia)
    assert r["paginas"][0]["fotos"] == []


def test_a_foto_do_diario_herda_o_escopo_da_obra(sessao_real, obra, dia, dono):
    """⚑ Sem isto, quem é preso a uma obra baixaria a foto de qualquer outra:
    os números dos anexos são sequenciais, e um laço varreria o acervo."""
    from app.apps.erp.core.auth.permissoes import exigir_entidade_no_escopo
    r = fotos.anexar(sessao_real, obra, dia, _jpeg(), "x.jpg", usuario=dono)
    sessao_real.flush()
    diario_id = fotos._diario(sessao_real, obra, dia)["id"]
    # o administrativo desta obra alcança
    exigir_entidade_no_escopo(sessao_real, dono, "diario_foto", diario_id)
    # e um diário que não existe não vira 500 nem vaza nada
    with pytest.raises(ErroNaoEncontrado):
        exigir_entidade_no_escopo(sessao_real, dono, "diario_foto", 99999999)
    assert r["id"]
