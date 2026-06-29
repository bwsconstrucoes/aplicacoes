# -*- coding: utf-8 -*-
"""
Organiza a pasta do app de NFS-e SEM quebrar nada.

Regra de ouro: o código ativo se importa de forma plana e lê arquivos por caminho
relativo (credenciais.json, certificado.p12, logos .png/.jpg, .json). Por isso o
CÓDIGO e os ASSETS ficam na RAIZ. Só arquivamos o resto.

Destinos:
  testes/  -> scripts de teste/inspeção (.py recebem um ajuste de sys.path p/ rodar
              da subpasta) + os .bat de teste
  saida/   -> PDFs/HTMLs gerados de referência
  _lixo/   -> rascunho puro (você esvazia quando quiser; nada é apagado de fato)

Uso:
  python organizar.py            # DRY-RUN: só mostra o que faria
  python organizar.py EXECUTAR   # faz de verdade (movimentos reversíveis)
"""
from __future__ import annotations
import os
import sys
import shutil

BASE = os.path.dirname(os.path.abspath(__file__))

# ---- listas (ajuste à vontade) -------------------------------------------- #
LIXO = [
    "Novo(a) Documento de Texto.txt",
    "danfse_tmp.pdf", "mun_tmp.pdf", "recibo_tmp.pdf",
    "__pycache__",
]
SAIDA = [
    "danfse_3072.pdf", "nfse_municipal_3072.pdf",
    "recibo_3072_bruto.pdf", "recibo_3072_v2.pdf",
    "preview_nota.html", "recibo_exemplo.html", "recibo_simulado.html",
]
TESTES_PY = [
    "inspecionar_wsdl.py", "inspecionar_xsd.py", "inspecionar_xsd2.py",
    "testar_adn.py", "teste_certificado.py", "teste_tomador.py",
    "emitir_homologacao.py",
]
# .bat antigos que correspondem aos testes movidos: vão pro _lixo (geramos novos)
BAT_ANTIGOS = [
    "inspecionar_wsdl.bat", "inspecionar_xsd.bat", "inspecionar_xsd2.bat",
    "testar_certificado.bat", "teste_tomador.bat", "emitir_homologacao.bat",
]
# .bat que chamam scripts da raiz e eu não sei o alvo: NÃO mexo, só aponto
BAT_AMBIGUOS = ["emitir_preview.bat", "emitir_abrasf.bat", "simular_emissao.bat"]

# ajuste inserido nos .py movidos: acha o core na raiz E fixa a pasta de trabalho
# na raiz (p/ achar credenciais.json, certificado.p12 e os assets de qualquer lugar)
SHIM = ("import sys as _sys, os as _os\n"
        "_ROOT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))\n"
        "_sys.path.insert(0, _ROOT); _os.chdir(_ROOT)")


def _com_shim(texto: str) -> str:
    if "_sys.path.insert(0, _ROOT)" in texto:
        return texto
    linhas = texto.split("\n")
    ins = 0
    for i in range(min(2, len(linhas))):           # respeita shebang / coding
        l = linhas[i]
        if l.startswith("#!") or "coding" in l or l.startswith("# -*-"):
            ins = i + 1
    linhas.insert(ins, SHIM)
    return "\n".join(linhas)


def _gerar_bat(nome_py: str, destino_dir: str, executar: bool):
    """Cria um lançador .bat simples em testes/ — o .py já se auto-localiza."""
    nome_bat = nome_py[:-3] + ".bat"
    print(f"  [gera-bat] testes/{nome_bat}")
    if not executar:
        return
    conteudo = ("@echo off\r\n"
                "chcp 65001 >nul\r\n"
                f'python "%~dp0{nome_py}" %*\r\n'
                "pause\r\n")
    with open(os.path.join(destino_dir, nome_bat), "w", encoding="utf-8") as fh:
        fh.write(conteudo)


def main(executar: bool):
    modo = "EXECUTANDO" if executar else "DRY-RUN (nada será movido)"
    print(f"===== ORGANIZAR — {modo} =====")
    print(f"Pasta: {BASE}\n")

    destinos = {"testes": os.path.join(BASE, "testes"),
                "saida": os.path.join(BASE, "saida"),
                "_lixo": os.path.join(BASE, "_lixo")}
    for d in destinos.values():
        if executar:
            os.makedirs(d, exist_ok=True)

    def mover(nome, destino_dir, rotulo, transform=None):
        orig = os.path.join(BASE, nome)
        if not os.path.exists(orig):
            return
        dest = os.path.join(destino_dir, nome)
        print(f"  [{rotulo}] {nome}  ->  {os.path.basename(destino_dir)}/")
        if not executar:
            return
        if transform and nome.endswith(".py"):
            with open(orig, "r", encoding="utf-8") as fh:
                txt = fh.read()
            with open(dest, "w", encoding="utf-8") as fh:
                fh.write(transform(txt))
            shutil.move(orig, os.path.join(destinos["_lixo"], nome))   # guarda o original
        else:
            if os.path.exists(dest):
                dest = os.path.join(destino_dir, "_dup_" + nome)
            shutil.move(orig, dest)

    print("» Para _lixo (rascunho/regenerável):")
    for n in LIXO:
        mover(n, destinos["_lixo"], "lixo")

    print("\n» Para saida/ (gerados de referência):")
    for n in SAIDA:
        mover(n, destinos["saida"], "saida")

    print("\n» Para testes/ (.py com ajuste + .bat lançador novo):")
    for n in TESTES_PY:
        existia = os.path.exists(os.path.join(BASE, n))
        mover(n, destinos["testes"], "teste", transform=_com_shim)
        if existia:
            _gerar_bat(n, destinos["testes"], executar)

    print("\n» .bat antigos de teste -> _lixo (já regenerados em testes/):")
    for n in BAT_ANTIGOS:
        mover(n, destinos["_lixo"], "lixo-bat")

    print("\n» REVISAR à mão (não toquei — me diga o que cada um chama):")
    for n in BAT_AMBIGUOS:
        if os.path.exists(os.path.join(BASE, n)):
            print(f"  [?] {n}")

    print("\n===== FIM =====")
    if not executar:
        print("Isso foi só a PRÉVIA. Para fazer de verdade:  python organizar.py EXECUTAR")
    else:
        print("Pronto. Confira o app (emissao.bat). O que foi pro _lixo você apaga quando quiser.")
        print("Os testes em testes/ rodam por duplo-clique no .bat ou:  python testes\\NOME.py")


if __name__ == "__main__":
    executar = len(sys.argv) > 1 and sys.argv[1].strip().upper() == "EXECUTAR"
    main(executar)
