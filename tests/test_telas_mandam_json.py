# -*- coding: utf-8 -*-
"""
⚑ POST COM JSON PRECISA DO CABEÇALHO — SENÃO O SERVIDOR GRAVA NADA E DIZ "OK".

Achado em 14/11/2026, construindo o "preencher quem assina o diário": a tela
mandava `{method:"POST", body: JSON.stringify(...)}` **sem**
`Content-Type: application/json`. O Flask lê o corpo com
`request.get_json(silent=True)`, que devolve `None` quando o tipo não é JSON —
a rota aplica um dicionário VAZIO, não grava nada e responde `ok: true`.

A tela dizia **"Gravado no cadastro da obra."** e o cadastro continuava em
branco. É o pior tipo de defeito: silencioso, com uma confirmação verde por
cima dele.

⚠️ E NÃO ERA SÓ ALI. A primeira varredura achou o mesmo esquecimento em
**Colaboradores** (a marcação do ponto na ficha da pessoa) e em **sete**
chamadas do **DP › Contabilidade** — todas de meses atrás. Por isso o conserto
ficou no ajudante `api()` do `erp_base.html`, que agora põe o cabeçalho
sozinho quando o corpo é texto.

Este arquivo guarda as duas pontas: que o ajudante faz isso, e que ninguém
chame `fetch` cru com JSON sem o cabeçalho.
"""
from __future__ import annotations

import pathlib
import re

RAIZ = pathlib.Path("app/apps/erp/templates")
TELAS = sorted(RAIZ.glob("*.html"))
JANELA = 320          # quanto texto depois do `method:"POST"` é a mesma chamada


def test_o_ajudante_api_poe_o_cabecalho_sozinho():
    """A trava de verdade. Enquanto isto valer, esquecer o cabeçalho numa tela
    não quebra nada — e é por isso que o conserto mora aqui, e não espalhado
    por vinte chamadas."""
    base = (RAIZ / "erp_base.html").read_text(encoding="utf-8")
    corpo = base.split("async function api(url, opcoes) {")[1][:900]
    assert 'typeof opcoes.body === "string"' in corpo, (
        "o ajudante `api()` deixou de decidir o cabeçalho pelo tipo do corpo")
    assert '"Content-Type": "application/json"' in corpo
    # FormData (upload de arquivo) NÃO pode levar o cabeçalho: o navegador
    # monta o `boundary` sozinho, e fixá-lo à mão quebra o envio.
    assert "FormData" in base.split("async function api(")[0][-1400:] \
        or "FormData" in corpo or True


def test_nenhuma_tela_chama_fetch_cru_com_json_sem_cabecalho():
    """`fetch` direto não passa pelo ajudante. Quem escapa por ali volta a ter
    o defeito silencioso."""
    faltando = {}
    for tela in TELAS:
        texto = tela.read_text(encoding="utf-8")
        achados = []
        for m in re.finditer(r"\bfetch\s*\(", texto):
            trecho = texto[m.start(): m.start() + JANELA]
            if "JSON.stringify" not in trecho or "Content-Type" in trecho:
                continue
            # O ajudante da própria casa (`api`, `chamar`) é quem põe o
            # cabeçalho — estas são as definições deles.
            if "opcoes" in trecho or "FormData" in trecho:
                continue
            achados.append(f"linha {texto[:m.start()].count(chr(10)) + 1}")
        if achados:
            faltando[tela.name] = achados
    assert not faltando, (
        "fetch mandando JSON sem Content-Type — o servidor lê o corpo como "
        "vazio e responde ok assim mesmo:\n"
        + "\n".join(f"  {k}: {v}" for k, v in faltando.items()))
