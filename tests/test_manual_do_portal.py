# -*- coding: utf-8 -*-
"""O MANUAL DO PORTAL NÃO PODE ENVELHECER EM SILÊNCIO (25/11/2026).

O manual do ponto ensinou a lição, e ela está escrita no histórico dele:
*"ao mudar uma tela ou uma regra que o colaborador vê, o manual fica velho"*.
Manual velho é pior que manual nenhum — a pessoa confia, faz o que está
escrito, e descobre no meio do caminho que a tela mudou.

Aqui o envelhecimento tem causa conhecida e barata de pegar: a obra ganha um
TIPO de pedido novo, ou uma ÁREA fechada abre, e ninguém lembra do manual.
Estes testes quebram quando isso acontece, dizendo exatamente o que falta.

Eles não conferem redação — conferem que o nome que a pessoa VÊ NA TELA
aparece em algum lugar do manual, e que nenhuma imagem está quebrada.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from app.apps.erp.core import pessoal_solicitacoes as svc_sp
from app.apps.erp.core import portal

MANUAL = (Path(__file__).resolve().parents[1]
          / "app" / "apps" / "erp" / "manual_portal")


@pytest.fixture(scope="module")
def texto() -> str:
    return (MANUAL / "index.html").read_text(encoding="utf-8")


def test_o_manual_existe_com_a_pagina_as_telas_e_o_pdf():
    assert (MANUAL / "index.html").exists(), "o manual do portal sumiu"
    assert (MANUAL / "telas").is_dir(), "a pasta das telas sumiu"
    pdf = MANUAL / "manual-portal-da-obra.pdf"
    assert pdf.exists(), (
        "o PDF do manual sumiu. Ele é impresso da própria página pelo "
        "Chromium — ver o histórico de 25/11/2026.")
    assert pdf.stat().st_size > 50_000, "o PDF ficou pequeno demais para ser o manual"


def test_toda_imagem_citada_no_manual_existe(texto: str):
    """Imagem quebrada só aparece para quem abre o manual — e aí já é tarde."""
    citadas = set(re.findall(r'src="telas/([^"]+)"', texto))
    assert citadas, "o manual ficou sem nenhuma tela"
    faltando = [n for n in sorted(citadas) if not (MANUAL / "telas" / n).exists()]
    assert not faltando, f"telas citadas no manual que não existem: {faltando}"


def test_nenhuma_tela_fica_sobrando_na_pasta(texto: str):
    """Tela que ninguém usa é peso no repositório e dúvida na próxima sessão."""
    citadas = set(re.findall(r'src="telas/([^"]+)"', texto))
    no_disco = {p.name for p in (MANUAL / "telas").iterdir() if p.is_file()}
    assert not (no_disco - citadas), (
        f"telas na pasta que o manual não usa: {sorted(no_disco - citadas)}")


def test_toda_area_do_portal_tem_uma_parte_no_manual(texto: str):
    """Área que abre sem entrar no manual deixa a obra sem saber que pode usar."""
    faltando = [a["nome"] for a in portal.AREAS if a["nome"] not in texto]
    assert not faltando, (
        f"áreas do portal que o manual não cita: {faltando}. "
        "Acrescente a parte dela — inclusive se ainda estiver fechada.")


def test_todo_tipo_que_a_obra_pode_pedir_aparece_no_manual(texto: str):
    """O nome é o que a pessoa lê na caixinha: é por ele que ela procura."""
    faltando = []
    for chave in portal.TIPOS_POR_AREA.get("pessoal", ()):
        nome = svc_sp.TIPOS.get(chave, {}).get("nome")
        if nome and nome not in texto:
            faltando.append(f"{chave} ({nome})")
    assert not faltando, (
        f"tipos de pedido que o manual não explica: {faltando}")


def test_a_conta_de_pedidos_escrita_no_manual_bate_com_a_tela(texto: str):
    """O manual diz "14 pedidos" em dois lugares. Tipo novo muda esse número —
    e número errado no manual é a parte que a pessoa decora."""
    quantos = len([k for k in portal.TIPOS_POR_AREA.get("pessoal", ())
                   if k in svc_sp.TIPOS])
    escritos = {int(n) for n in re.findall(r"(\d+)\s+pedidos", texto)}
    assert escritos, "o manual deixou de dizer quantos pedidos a área tem"
    assert escritos == {quantos}, (
        f"o manual diz {sorted(escritos)} pedido(s) e a área oferece {quantos}")


def test_o_manual_nao_promete_area_fechada_como_aberta(texto: str):
    """Porta que anuncia o que não tem é o defeito que já aconteceu uma vez,
    com 'férias' na explicação da área (ver `core/portal.py`)."""
    for a in portal.AREAS:
        if a.get("pronta"):
            continue
        parte = texto.split(a["nome"], 1)[1][:1200]
        assert "ainda não" in parte.lower() or "enquanto" in parte.lower(), (
            f"a área {a['nome']} está fechada no sistema e o manual não diz isso")

# ---------------------------------------------------------------------------
# ONDE O LINK MORA — decisão dele em 25/11/2026, depois de procurar e não achar
# ---------------------------------------------------------------------------
TEMPLATES = (Path(__file__).resolve().parents[1]
             / "app" / "apps" / "erp" / "templates")


def test_o_portal_leva_ao_manual_no_alto_e_no_pe():
    """Link que a pessoa precisa procurar é link que não existe.

    Ele: *"eu não entendi bem exatamente onde é que está o link para acesso ao
    manual. Mas eu acho que o ideal é na tela depois do login."* O link estava
    só no pé da página. Agora está também na barra azul, visível sem rolar —
    e este teste existe para ele não sumir de lá numa mexida futura.
    """
    tela = (TEMPLATES / "erp_portal.html").read_text(encoding="utf-8")
    topo = tela.split('<div class="topo-portal">', 1)[1].split("</div>\n<div class=\"caixa\">", 1)[0]
    assert "/erp/portal/manual/" in topo, (
        "o link do manual saiu da barra do portal — ele tem de aparecer sem rolar")
    assert tela.count("/erp/portal/manual/") >= 2, (
        "o manual deixou de ter o segundo caminho, no pé da página")
    assert "/erp/portal/manual.pdf" in tela, "sumiu o 'baixar em PDF' do portal"


def test_a_tela_publica_de_criar_acesso_nao_leva_ao_manual():
    """O manual exige login desde 25/11/2026. Um link para ele numa tela de
    quem AINDA NÃO ENTROU é link morto: leva à tela de entrar, que é de onde a
    pessoa veio. Pior que não ter link é ter um que não vai a lugar nenhum."""
    tela = (TEMPLATES / "erp_primeiro_acesso_portal.html").read_text(encoding="utf-8")
    assert "/erp/portal/manual" not in tela, (
        "a tela pública de criar acesso voltou a apontar para o manual, que "
        "agora exige login — o link levaria de volta para a tela de entrar.")
