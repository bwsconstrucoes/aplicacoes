# -*- coding: utf-8 -*-
"""A FILA DE PEDIDOS DE ACESSO PRECISA NASCER COM A TELA (26/11/2026).

O defeito que este arquivo existe para impedir, nas palavras dele:

> *"Aparece a pendência em Pessoal, clico, sou remetido pra Operadores, mas
> não abre a tela pra liberação e confirmação do perfil. Tampouco se eu
> clicar em adm e operadores, não vejo quem tá pendente."*

A causa era de uma linha: `carregarFilaDoPortal()` tinha ido parar **dentro do
"Salvar" do formulário de PERFIS**. O cartão só nascia se alguém abrisse um
perfil e salvasse — coisa que ninguém faz para liberar acesso. Em toda abertura
normal da tela a fila não era buscada, e o cartão ficava invisível **sem erro
nenhum aparecer**: por isso passou despercebido até o dono tropeçar.

⚠️ POR QUE A CONFERÊNCIA É PELA VIZINHANÇA, E NÃO PELA EXISTÊNCIA DA LINHA:
a chamada errada também começava na coluna zero, então um `in texto` teria
passado. O que distingue as duas é **onde** ela está — e o lugar certo é a
sequência de abertura da tela, entre `carregarOperadores()` e
`carregarPerfis()`.

⚠️ O QUE ESTE TESTE NÃO FAZ, dito para ninguém confiar demais: ele não entende
JavaScript. Cheguei a escrever um contador de profundidade de chaves e ele
**não funcionou** neste arquivo — o template mistura Jinja (`{{ }}`, `{% %}`),
crases com HTML dentro e expressões regulares, e a conta sai errada. Testado:
com o defeito de volta, o contador dizia que estava tudo bem. Uma conferência
que mente é pior que nenhuma, então ficou a simples, que eu provei que pega.
"""
from __future__ import annotations

import re
from pathlib import Path

TELAS = (Path(__file__).resolve().parents[1]
         / "app" / "apps" / "erp" / "templates")


def _tela(nome: str) -> str:
    return (TELAS / nome).read_text(encoding="utf-8")


def test_a_fila_do_portal_e_carregada_na_abertura_da_tela():
    config = _tela("erp_config.html")
    assert "carregarFilaDoPortal()" in config, (
        "Configurações nunca busca a fila de pedidos de acesso: "
        "`carregarFilaDoPortal()` sumiu da tela.")
    assert re.search(r"^carregarFilaDoPortal\(\);\s*\ncarregarPerfis\(\);",
                     config, re.M), (
        "`carregarFilaDoPortal()` saiu da sequência de abertura da tela (ela "
        "fica entre `carregarOperadores()` e `carregarPerfis()`). Fora dali, "
        "quem abre Configurações › Operadores não vê quem está esperando "
        "liberação — e nenhum erro aparece para denunciar.")


def test_a_fila_nao_depende_de_salvar_um_perfil():
    """Era exatamente onde a chamada estava: no `catch`/`then` do Salvar do
    formulário de perfis. Liberar acesso não tem nada a ver com editar perfil
    de acesso, e ninguém salvaria um perfil para ver a fila aparecer."""
    config = _tela("erp_config.html")
    salvar_perfil = config.split('els("pf-salvar").addEventListener', 1)
    assert len(salvar_perfil) == 2, "o Salvar do formulário de perfis sumiu"
    corpo = salvar_perfil[1].split("els(\"dlg\").showModal();", 1)[0]
    assert "carregarFilaDoPortal" not in corpo, (
        "a carga da fila voltou para dentro do Salvar do formulário de "
        "perfis — foi assim que ela deixou de rodar na abertura da tela")


def test_o_link_de_pessoal_leva_o_pedido_junto():
    """Ele clicava na pendência de UMA pessoa e chegava numa lista sem saber
    qual era. Com o número do pedido no endereço, a confirmação abre sozinha
    do outro lado."""
    pessoal = _tela("erp_colaboradores.html")
    assert "/erp/configuracoes?pedido=${p.id}#usuarios" in pessoal, (
        "o link 'Liberar em Operadores' voltou a ser igual para todo mundo — "
        "quem clica chega em Operadores sem saber qual pedido era o dele")

    config = _tela("erp_config.html")
    assert 'busca.get("pedido")' in config, (
        "Configurações deixou de atender `?pedido=`: o link de Pessoal passa "
        "a cair numa tela que não faz nada")
    assert 'decidirPedidoDeAcesso(+pedido, "LIBERAR")' in config, (
        "Configurações recebe `?pedido=` e não abre mais a confirmação")
